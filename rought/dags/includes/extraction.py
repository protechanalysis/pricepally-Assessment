import requests
import pandas as pd
import pandera as pa
import logging
import json
import os
from time import sleep
from datetime import datetime
from pathlib import Path
from util.config import ecowas_country, indicators, start_year, end_year, indicators_column_names, json_folder
from includes.validation import get_wide_schema


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_world_bank_data() -> json:
    """
    Extracts data from the World Bank API by batching countries for efficiency.
    It saves the raw JSON response to a file in the specified landing zone.

    Args:
        countries (list): A list of 3-letter country ISO codes.
        indicators (dict): A dictionary of indicator codes and names.
        start_year (int): The starting year for data extraction.
        end_year (int): The ending year for data extraction.
        output_folder (Path): The directory to save the raw JSON file.

    Returns:
        Path: The file path to the saved raw data, or None if extraction fails.
    """
    all_data = []
    country_str = ";".join(ecowas_country)
    logging.info(f"Beginning data extraction for {len(ecowas_country)} countries.")

    for indicator_code, indicator_name in indicators.items():
        url = (
            f"http://api.worldbank.org/v2/country/{country_str}/indicator/{indicator_code}"
            f"?format=json&date={start_year}:{end_year}&per_page=1000"
        )
        logging.info(f"Fetching data for indicator: {indicator_name}...")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise an error for bad status codes (4xx or 5xx)
            
            data = response.json()
            if len(data) > 1 and data[1]:
                all_data.extend(data[1])
            else:
                logging.warning(f"No data returned for indicator: {indicator_code}")

        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred for {indicator_code}: {http_err}")
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request error for {indicator_code}: {req_err}")
        except ValueError as json_err:
            logging.error(f"JSON decode error for {indicator_code}: {json_err}")
        
        sleep(0.5)  # A small delay to be respectful to the API server

    if not all_data:
        logging.error("Extraction failed. No data was retrieved from the API.")
        return None

    
    logging.info(f"Saving raw extracted data to {json_folder}")
    os.makedirs("/opt/airflow/tmp", exist_ok=True)  # create if missing
    with open(json_folder, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=4)



def transform_to_dataframe() -> pd.DataFrame:
    """
    Transforms the raw JSON data into a clean, long-format pandas DataFrame.
    It handles data type conversion, nulls, and structuring.

    Args:
        raw_data_path (Path): The file path to the raw JSON data.
        indicator_mapping (dict): A dictionary mapping indicator codes to names.

    Returns:
        pd.DataFrame: A cleaned and structured DataFrame.
    """
    logging.info(f"Reading raw data from {json_folder}")

    with open(json_folder, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    if not raw_data:
        logging.warning("Raw data file is empty. Returning an empty DataFrame.")
        return pd.DataFrame()

    records = []
    for entry in raw_data:
        records.append({
            "country_name": entry.get("country", {}).get("value"),
            "country_iso3": entry.get("countryiso3code"),
            "year": entry.get("date"),
            "indicator_code": entry.get("indicator", {}).get("id"),
            "indicator_name": indicators.get(entry.get("indicator", {}).get("id")),
            "value": entry.get("value")
        })
    
    df = pd.DataFrame(records)
    
    logging.info("Cleaning and transforming data...")
    df.dropna(subset=["country_name", "year", "indicator_code", "value"], inplace=True) # Drop rows with essential info missing
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df.dropna(subset=['year', 'value'], inplace=True) # Drop rows where conversion failed
    df['year'] = df['year'].astype(int)

    logging.info("Pivoting data from long to wide format...")
    df_wide = df.pivot_table(
        index=["country_name", "country_iso3", "year"],
        columns="indicator_code",
        values="value"
    ).reset_index()
    df_wide.columns.name = None # Clean up the column index name
    
    logging.info("Renaming columns for clarity...")
    df_wide.rename(columns=indicators_column_names, inplace=True)
    logging.info(f"Successfully transformed data. Final shape: {df.shape}")
    return df_wide


def validate_data() -> pd.DataFrame:
    """
    Validates the transformed DataFrame against the Pandera schema.

    Args:
        df (pd.DataFrame): The DataFrame to validate.

    Returns:
        pd.DataFrame: The validated DataFrame, if successful.
    
    Raises:
        pa.errors.SchemaErrors: If the DataFrame fails validation.
    """
    logging.info("Validating data against the WIDE-FORMAT quality schema...")
    try:
        # 1. Get the schema object from our function
        schema = get_wide_schema()
        df = transform_to_dataframe()
        # 2. Validate the DataFrame against the schema
        validated_df = schema.validate(df, lazy=True)
        logging.info("Data validation successful.")
        return validated_df
    except pa.errors.SchemaErrors as err:
        logging.error("Data validation failed. See failure cases below.")
        logging.error(err.failure_cases)
        raise err
