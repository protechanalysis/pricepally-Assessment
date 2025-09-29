# etl.py

# --- (Other imports remain the same) ---
import pandera as pa
from validation import get_wide_schema # <-- Import the new function

# --- (extract_world_bank_data, transform_to_dataframe, and load_to_postgres functions remain the same) ---


# --- REVISED validate_data function ---
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

# --- (The main() function remains exactly the same) ---

def main():
    # ... No changes needed here ...
    pass

if __name__ == "__main__":
    main()