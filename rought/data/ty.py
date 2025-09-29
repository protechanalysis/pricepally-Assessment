import requests
import pandas as pd
import pandera as pa
import logging
import json
from time import sleep
from datetime import datetime
from pathlib import Path
from config import WEST_AFRICAN_COUNTRIES, INDICATORS, START_YEAR, END_YEAR, INDICATOR_COLUMN_NAMES
from dags.includes.validation import get_wide_schema

# Setup professional logging to monitor the pipeline's execution
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_log.log"), # Log to a file
        logging.StreamHandler() # Also log to console
    ]
)

def extract_world_bank_data(countries: list, indicators: dict, start_year: int, end_year: int, output_folder: Path) -> Path:
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
    country_str = ";".join(countries)
    logging.info(f"Beginning data extraction for {len(countries)} countries.")

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

    output_folder.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = output_folder / f"raw_data_{timestamp}.json"
    
    logging.info(f"Saving raw extracted data to {file_path}")
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=4)
        
    return file_path


def transform_to_dataframe(raw_data_path: Path, indicator_mapping: dict, column_rename_map: dict) -> pd.DataFrame:
    """
    Transforms the raw JSON data into a clean, long-format pandas DataFrame.
    It handles data type conversion, nulls, and structuring.

    Args:
        raw_data_path (Path): The file path to the raw JSON data.
        indicator_mapping (dict): A dictionary mapping indicator codes to names.

    Returns:
        pd.DataFrame: A cleaned and structured DataFrame.
    """
    logging.info(f"Reading raw data from {raw_data_path}")
    with open(raw_data_path, 'r', encoding='utf-8') as f:
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
            "indicator_name": indicator_mapping.get(entry.get("indicator", {}).get("id")),
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
    df_wide.rename(columns=column_rename_map, inplace=True)
    logging.info(f"Successfully transformed data. Final shape: {df.shape}")
    return df_wide


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
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
        # 2. Validate the DataFrame against the schema
        validated_df = schema.validate(df, lazy=True)
        logging.info("Data validation successful.")
        return validated_df
    except pa.errors.SchemaErrors as err:
        logging.error("Data validation failed. See failure cases below.")
        logging.error(err.failure_cases)
        raise err


def main():
    """Main function to orchestrate the ETL process."""
    logging.info("="*50)
    logging.info("Starting World Bank ETL process")
    logging.info("="*50)
    
    country_codes = list(WEST_AFRICAN_COUNTRIES.keys())
    raw_data_landing_zone = Path("data/raw")
    clean_data_zone = Path("data/clean")
    
    # Step 1: EXTRACT
    raw_file_path = extract_world_bank_data(
        country_codes, INDICATORS, START_YEAR, END_YEAR, raw_data_landing_zone
    )
    
    # Step 2: TRANSFORM
    if raw_file_path:
        transformed_df = transform_to_dataframe(raw_file_path, INDICATORS, INDICATOR_COLUMN_NAMES)

        if transformed_df.empty:
            logging.warning("Transformation resulted in an empty DataFrame. Halting process.")
            raise

        try:
            # Step 3: VALIDATE
            validated_df = validate_data(transformed_df)
            
            # --- NEW STEP: Save validated data to CSV ---
            logging.info("Saving validated data to CSV file...")
            clean_data_zone.mkdir(parents=True, exist_ok=True)
            # Create a filename based on the raw file's timestamp for consistency
            output_path = clean_data_zone / f"clean_west_africa_data_{raw_file_path.stem.replace('raw_data_', '')}.csv"
            validated_df.to_csv(output_path, index=False, encoding='utf-8')
            logging.info(f"Validated data successfully saved to {output_path}")
        except pa.errors.SchemaErrors:
            logging.error("ETL process halted due to data validation failure. No data was saved or loaded.")
            raise
        logging.info("="*50)
        logging.info("ETVL process finished")
        logging.info("="*50)

if __name__ == "__main__":
    main()