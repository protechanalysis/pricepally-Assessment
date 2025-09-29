import requests              # For making HTTP requests to the World Bank API
import pandas as pd          # For handling tabular data and transformations
import pandera as pa         # For DataFrame schema validation (data quality checks)
import logging               # For structured logging
import json                  # For reading/writing JSON data
import os                    # For filesystem operations (creating directories, etc.)
from time import sleep       # For introducing delays between API calls
from datetime import datetime
from pathlib import Path     # For working with file system paths in an object-oriented way

# Import project-specific configurations and helper functions
from util.config import (
    ecowas_country,          # List of ISO3 codes for ECOWAS countries
    indicators,              # Dictionary mapping indicator codes -> descriptive names
    start_year, end_year,    # Range of years for pulling World Bank data
    indicators_column_names, # Mapping from World Bank codes -> human-readable column names
    json_folder              # Path where raw JSON API responses should be stored
)
from includes.validation import get_wide_schema  # Function returning Pandera schema for validation


# Configure logging format and level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def extract_world_bank_data() -> json:
    all_data = []  # Will accumulate all data entries across indicators

    # Concatenate all ECOWAS ISO3 country codes into the semicolon-separated format required by World Bank API
    country_str = ";".join(ecowas_country)
    logging.info(f"Beginning data extraction for {len(ecowas_country)} countries.")

    # Loop through each indicator code configured in util.config
    for indicator_code, indicator_name in indicators.items():

        # Construct the World Bank API URL
        url = (
            f"http://api.worldbank.org/v2/country/{country_str}/indicator/{indicator_code}"
            f"?format=json&date={start_year}:{end_year}&per_page=1000"
        )
        logging.info(f"Fetching data for indicator: {indicator_name}...")

        try:
            # Make GET request to API with a 30s timeout safeguard
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise exception if HTTP status code indicates error (4xx, 5xx)

            # Parse JSON response into Python list/dict
            data = response.json()

            # World Bank API responses are a list: [metadata, data]. We want index 1.
            if len(data) > 1 and data[1]:
                all_data.extend(data[1])  # Append records to our cumulative list
            else:
                logging.warning(f"No data returned for indicator: {indicator_code}")

        # Handle various error types separately for clearer debugging
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred for {indicator_code}: {http_err}")
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request error for {indicator_code}: {req_err}")
        except ValueError as json_err:
            logging.error(f"JSON decode error for {indicator_code}: {json_err}")

        # Add short sleep to avoid overwhelming the API (rate-limiting best practice)
        sleep(0.5)

    # If no data was retrieved at all, log error and abort
    if not all_data:
        logging.error("Extraction failed. No data was retrieved from the API.")
        return None

    # Save raw data to disk for traceability and reproducibility
    logging.info(f"Saving raw extracted data to {json_folder}")
    os.makedirs("/opt/airflow/tmp", exist_ok=True)  # Ensure base folder exists (important for Airflow workers)

    # Dump JSON into configured landing file
    with open(json_folder, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=4)


def transform_to_dataframe() -> pd.DataFrame:
    logging.info(f"Reading raw data from {json_folder}")

    # Load previously saved JSON file
    with open(json_folder, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    # Handle case where file exists but is empty
    if not raw_data:
        logging.warning("Raw data file is empty. Returning an empty DataFrame.")
        return pd.DataFrame()

    # Flatten JSON entries into row dictionaries
    records = []
    for entry in raw_data:
        records.append({
            "country_name": entry.get("country", {}).get("value"),          # Human-readable country name
            "country_iso3": entry.get("countryiso3code"),                   # ISO3 country code
            "year": entry.get("date"),                                      # Year as string
            "indicator_code": entry.get("indicator", {}).get("id"),         # WB indicator code
            "indicator_name": indicators.get(entry.get("indicator", {}).get("id")),  # Map to descriptive name
            "value": entry.get("value")                                     # Actual numeric value
        })

    # Convert list of dicts into DataFrame
    df = pd.DataFrame(records)

    logging.info("Cleaning and transforming data...")

    # # Drop rows missing critical fields (country/year/indicator/value)
    # df.dropna(subset=["country_name", "year", "indicator_code", "value"], inplace=True)

    # Convert year and value fields into numeric, coercing invalid entries to NaN
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    # # Drop rows where numeric conversion failed
    # df.dropna(subset=['year', 'value'], inplace=True)

    # Cast year into integer (safe after coercion + dropna)
    # df['year'] = df['year'].astype(int)

    logging.info("Pivoting data from long to wide format...")

    # Pivot: each row is (country, iso, year), each column is an indicator
    df_wide = df.pivot_table(
        index=["country_name", "country_iso3", "year"],
        columns="indicator_code",
        values="value"
    ).reset_index()

    # Drop Pandas’ extra column index name (cosmetic cleanup)
    df_wide.columns.name = None

    logging.info("Renaming columns for clarity...")

    # Rename indicator codes to more descriptive names (from config mapping)
    df_wide.rename(columns=indicators_column_names, inplace=True)

    logging.info(f"Successfully transformed data. Final shape: {df.shape}")
    return df_wide


def validate_data() -> pd.DataFrame:
    logging.info("Validating data against the WIDE-FORMAT quality schema...")

    try:
        # Fetch schema definition object (defined in includes.validation)
        schema = get_wide_schema()

        # Re-run transformation pipeline to get fresh DataFrame
        df = transform_to_dataframe()

        # Validate DataFrame against schema.
        # lazy=True → collect ALL validation errors before raising, instead of failing fast.
        validated_df = schema.validate(df, lazy=True)

        logging.info("Data validation successful.")
        return validated_df

    except pa.errors.SchemaErrors as err:
        # If validation fails, log all errors for debugging
        logging.error("Data validation failed. See failure cases below.")
        logging.error(err.failure_cases)
        raise err  # Re-raise so pipeline halts instead of silently continuing
