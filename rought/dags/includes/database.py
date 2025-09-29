import logging
import io
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from util.config import postgres_conn_id, indicators_column_names, database_table_name
from includes.extraction import transform_to_dataframe

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def create_metric_table():
      # 1. Dynamically build the column definitions for the SQL statement
    column_definitions = [
        "country_name TEXT",
        "country_iso3 VARCHAR(3)",
        "year INTEGER"
    ]
    
    for col_name in indicators_column_names.values():
        column_definitions.append(f"{col_name} FLOAT")

    # 2. Construct the full CREATE TABLE statement
    # Using 'CREATE TABLE IF NOT EXISTS' makes the script safe to run multiple times.
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_table_name} (
        {', '.join(column_definitions)},
        PRIMARY KEY (country_iso3, year)
    );
    """

    logging.info(f"Preparing to create table '{database_table_name}' if it does not exist.")

    try:
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                conn.commit()
                logging.info("metric table and indexes created successfully!")
    except Exception as e:
        logging.error(f"Error creating metric table: {e}", exc_info=True)
        raise


def load_dataframe_to_postgres() -> None:
    """
    Best-practice insert into Postgres using COPY + upsert pattern.
    """
    df =  transform_to_dataframe()
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # 1. Create staging table
            staging_table = f"{database_table_name}_staging"
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
            cursor.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {database_table_name} INCLUDING ALL)")

            # 2. Copy dataframe into staging
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cursor.copy_expert(f"COPY {staging_table} FROM STDIN WITH CSV", buffer)

            # 3. Merge from staging → final table
            # Requires Postgres 15+ (MERGE). For older versions, use INSERT ... ON CONFLICT.
            merge_sql = f"""
            INSERT INTO {database_table_name}
            SELECT * FROM {staging_table}
            ON CONFLICT (country_iso3, year) DO UPDATE
            SET
                country_name = EXCLUDED.country_name,
                {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in ['country_iso3','year','country_name']])};
            """
            cursor.execute(merge_sql)

        conn.commit()
