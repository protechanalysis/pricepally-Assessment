# etl.py

# --- (Imports and logging setup remain the same) ---
import pandas as pd
import logging
from sqlalchemy import create_engine, text, types
import pandera as pa

# --- (Import updated config and validation schema) ---
from config import WEST_AFRICAN_COUNTRIES, INDICATORS, START_YEAR, END_YEAR, DB_CONFIG, TABLE_NAME, INDICATOR_COLUMN_NAMES
from validation import AgriDataWideSchema # <-- Import the new wide schema

# --- (extract_world_bank_data function remains the same) ---

# --- REVISED transform_to_dataframe function ---
def transform_to_dataframe(raw_data_path: Path, indicator_mapping: dict, column_rename_map: dict) -> pd.DataFrame:
    """
    Transforms raw JSON data into a clean, WIDE-FORMAT pandas DataFrame.
    This involves cleaning, pivoting, renaming columns, and ensuring data types.
    """
    logging.info(f"Reading raw data from {raw_data_path}")
    with open(raw_data_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    if not raw_data:
        logging.warning("Raw data file is empty. Returning an empty DataFrame.")
        return pd.DataFrame()

    # 1. Initial cleaning into a LONG format DataFrame
    # ... (This part of the logic is the same as before, creating the 'records' list) ...
    df_long = pd.DataFrame(records)
    df_long['year'] = pd.to_numeric(df_long['year'], errors='coerce')
    df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
    df_long.dropna(subset=["country_iso3", "year", "indicator_code", "value"], inplace=True)
    df_long['year'] = df_long['year'].astype(int)

    # 2. Pivot the table from long to wide format
    logging.info("Pivoting data from long to wide format...")
    df_wide = df_long.pivot_table(
        index=["country_name", "country_iso3", "year"],
        columns="indicator_code",
        values="value"
    ).reset_index()
    df_wide.columns.name = None # Clean up the column index name

    # 3. Rename columns for better context using the map from config
    logging.info("Renaming columns for clarity...")
    df_wide.rename(columns=column_rename_map, inplace=True)

    # 4. Ensure final data types for all indicator columns
    logging.info("Ensuring final data types...")
    for col in column_rename_map.values():
        if col in df_wide.columns:
            # Convert to numeric, coercing errors will turn non-numeric values into NaN
            df_wide[col] = pd.to_numeric(df_wide[col], errors='coerce')
    
    logging.info(f"Successfully transformed data to wide format. Final shape: {df_wide.shape}")
    return df_wide

# # --- (validate_data function is updated to use the new schema) ---
# def validate_data(df: pd.DataFrame) -> pd.DataFrame:
#     logging.info("Validating data against the WIDE-FORMAT quality schema...")
#     try:
#         validated_df = AgriDataWideSchema.validate(df, lazy=True)
#         logging.info("Data validation successful.")
#         return validated_df
#     except pa.errors.SchemaErrors as err:
#         # ... (error handling remains the same) ...
#         raise err

# # --- (load_to_postgres function is updated to handle wide format efficiently) ---
# def load_to_postgres(df: pd.DataFrame, db_config: dict, table_name: str, column_map: dict):
#     # ... (connection logic is the same) ...
#     try:
#         # Dynamically create a dtype mapping for SQLAlchemy for efficiency
#         sql_dtypes = {
#             'country_name': types.String,
#             'country_iso3': types.String(3),
#             'year': types.Integer,
#         }
#         for col_name in column_map.values():
#             sql_dtypes[col_name] = types.Float
        
#         df.to_sql(
#             table_name,
#             engine,
#             if_exists='replace',
#             index=False,
#             dtype=sql_dtypes, # Use the explicit dtype map
#             chunksize=1000
#         )
#         logging.info(f"Successfully loaded {len(df)} rows into '{table_name}'.")
#     except Exception as e:
#         # ... (error handling is the same) ...
#         raise

# # --- UPDATED main() FUNCTION ---
# def main():
#     # ... (logging and setup remain the same) ...
    
#     # Step 1: EXTRACT
#     raw_file_path = extract_world_bank_data(...)

#     if not raw_file_path:
#         # ... (error handling) ...
#         return

#     # Step 2: TRANSFORM (Pass the new column rename map)
#     transformed_df = transform_to_dataframe(raw_file_path, INDICATORS, INDICATOR_COLUMN_NAMES)

#     if transformed_df.empty:
#         # ... (error handling) ...
#         return

#     try:
#         # Step 3: VALIDATE (Uses the new wide schema implicitly via the updated function)
#         validated_df = validate_data(transformed_df)
        
#         # Step 4: LOAD (Pass the column map for dtype mapping)
#         load_to_postgres(validated_df, DB_CONFIG, TABLE_NAME, INDICATOR_COLUMN_NAMES)

#     except pa.errors.SchemaErrors:
#         # ... (error handling) ...
#     except Exception as e:
#         # ... (error handling) ...

#     # ... (logging finish remains the same) ...

# if __name__ == "__main__":
#     main()