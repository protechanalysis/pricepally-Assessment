import logging
import io
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from util.config import postgres_conn_id, indicators_column_names, database_table_name
from includes.extraction import transform_to_dataframe

# Configure logging for consistent and timestamped messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def create_metric_table():
    # Define fixed base columns for the table
    column_definitions = [
        "country_name TEXT",
        "country_iso3 VARCHAR(3)",  # ISO3 country code (3 letters)
        "year INTEGER"              # Year of observation
    ]
    
    # Add all indicator columns dynamically from config
    for col_name in indicators_column_names.values():
        column_definitions.append(f"{col_name} FLOAT")

    # Build the CREATE TABLE SQL with dynamic schema
    # Primary key ensures unique rows per country & year
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_table_name} (
        {', '.join(column_definitions)},
        PRIMARY KEY (country_iso3, year)
    );
    """

    logging.info(f"Preparing to create table '{database_table_name}' if it does not exist.")

    try:
        # Initialize Postgres connection
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)  # Execute CREATE TABLE statement
                conn.commit()  # Commit changes
                logging.info("Metric table and indexes created successfully!")
    except Exception as e:
        # Log and re-raise if any issue occurs
        logging.error(f"Error creating metric table: {e}", exc_info=True)
        raise


def load_dataframe_to_postgres() -> None:
    # Transform raw data into a DataFrame (source logic in includes/extraction.py)
    df = transform_to_dataframe()

    # Open Postgres connection
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # --- 1. Create staging table ---
            # Drop if exists to avoid duplicates during reruns
            staging_table = f"{database_table_name}_staging"
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
            # Create a temporary staging table with same structure as final table
            cursor.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {database_table_name} INCLUDING ALL)")

            # --- 2. Copy DataFrame into staging table ---
            buffer = io.StringIO()
            # Export DataFrame to CSV format without headers (COPY expects raw rows)
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)  # Reset buffer cursor
            # Use Postgres COPY for bulk insert (very efficient)
            cursor.copy_expert(f"COPY {staging_table} FROM STDIN WITH CSV", buffer)

            # --- 3. Merge staging → final table ---
            # INSERT all rows from staging into final table
            # Use ON CONFLICT to upsert (insert new rows, update existing ones)
            merge_sql = f"""
            INSERT INTO {database_table_name}
            SELECT * FROM {staging_table}
            ON CONFLICT (country_iso3, year) DO UPDATE
            SET
                country_name = EXCLUDED.country_name,
                {', '.join([
                    f"{col} = EXCLUDED.{col}"
                    for col in df.columns
                    if col not in ['country_iso3','year','country_name']
                ])};
            """
            cursor.execute(merge_sql)

        # Commit the transaction after cursor is closed
        conn.commit()
