# load_dataframe_dag.py

import pandas as pd
import logging
import io
from datetime import datetime

# Import Airflow classes
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id='postgres_dataframe_loader',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # This DAG runs manually
    catchup=False,
    tags=['postgres', 'data-eng-case-study'],
    doc_md="A DAG to demonstrate loading a pandas DataFrame into PostgreSQL using PostgresHook."
)
def load_dataframe_to_postgres_dag():
    """
    ### DataFrame Loading DAG
    This DAG simulates an ETL process where a DataFrame is generated
    and then loaded into a PostgreSQL table using the PostgresHook.
    """

    @task
    def create_sample_dataframe() -> pd.DataFrame:
        """
        This task simulates the 'Transform' step of an ETL process.
        It creates a sample pandas DataFrame to be loaded.
        """
        logging.info("Creating a sample DataFrame.")
        columns = [
            'country_name', 'country_iso3', 'year', 'food_production_idx', 
            'cereal_yield_kg_per_hectare', 'crop_production_idx', 'agricultural_land_pct', 
            'gdp_per_capita_usd', 'total_cpi_2010_base', 'food_imports_pct_merch', 
            'food_exports_pct_merch', 'population_total', 'population_urban_pct', 
            'population_growth_annual_pct'
        ]
        
        sample_data = {
            'country_name': ['Nigeria', 'Ghana'],
            'country_iso3': ['NGA', 'GHA'],
            'year': [2025, 2025],
            'food_production_idx': [110.5, 112.3],
            'cereal_yield_kg_per_hectare': [1500.0, 1800.0]
        }
        
        sample_df = pd.DataFrame(sample_data, columns=columns)
        logging.info(f"DataFrame created with shape: {sample_df.shape}")
        return sample_df

    @task
    def load_df_to_postgres(df: pd.DataFrame):
        """
        This task uses the PostgresHook to load the DataFrame into the database.
        It uses the high-performance COPY method.
        """
        if df.empty:
            logging.warning("Received an empty DataFrame. Skipping load.")
            return

        table_name = "west_african_agri_metrics_wide"
        # The postgres_conn_id must match the Connection Id you set in the Airflow UI.
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        
        logging.info(f"Preparing to load {len(df)} rows into table '{table_name}'.")

        # The PostgresHook's `get_conn()` method returns a standard psycopg2 connection object
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Prepare DataFrame for COPY by writing it to an in-memory buffer
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0) # Rewind buffer to the beginning

            # Truncate the table before loading new data
            logging.info(f"Truncating table '{table_name}'...")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Use the COPY command
            logging.info("Loading data with COPY command...")
            cursor.copy_from(buffer, table_name, sep=',')
            
            # Commit the transaction
            conn.commit()
            logging.info(f"Successfully loaded {cursor.rowcount} rows.")

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            # The hook manages closing the connection, so conn.close() is not needed here.

    # Define the task dependencies
    dataframe = create_sample_dataframe()
    load_df_to_postgres(dataframe)

# Instantiate the DAG
load_dataframe_to_postgres_dag()