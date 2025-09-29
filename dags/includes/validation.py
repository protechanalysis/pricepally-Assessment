import pandera as pa
from datetime import datetime
from util.config import start_year, end_year

# Dynamically capture the current year → ensures schema always validates "year" correctly
# current_year = datetime.now().year


def get_wide_schema() -> pa.DataFrameSchema:
    # Build and return the Pandera validation schema for the wide-format dataset.
    # This schema enforces column types, value ranges, uniqueness, and ensures no unexpected columns appear.

    schema = pa.DataFrameSchema(
        columns={
            # Basic identifiers
            "country_name": pa.Column(pa.String, nullable=False),  # Must be a string, cannot be null

            "country_iso3": pa.Column(
                pa.String,
                checks=[pa.Check.str_matches(r'^[A-Z]{3}$')],  # Must match 3 uppercase letters
                nullable=False
            ),

            "year": pa.Column(
                pa.Int,
                checks=[
                    pa.Check.ge(start_year),        # Year must be >= 2000
                    pa.Check.le(end_year) # Year must not exceed current system year
                ],
                nullable=False
            ),

            # Indicator columns
            # Nullable=True → missing values allowed (World Bank often has gaps)
            "food_production_idx": pa.Column(pa.Float, nullable=True),

            "cereal_yield_kg_per_hectare": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      # Yield cannot be negative
                nullable=True
            ),

            "crop_production_idx": pa.Column(pa.Float, nullable=True),

            "agricultural_land_pct": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      # Percent cannot be negative
                nullable=True
            ),

            "gdp_per_capita_usd": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      # GDP per capita must be >= 0
                nullable=True
            ),

            "food_cpi_2010_base_100": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      # CPI index must be >= 0
                nullable=True
            ),

            "food_imports_pct_merch": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      # Percent cannot be negative
                nullable=True
            ),

            "food_exports_pct_merch": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      # Percent cannot be negative
                nullable=True
            ),

            "population_total": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      # Population must be >= 0
                nullable=True
            ),

            "population_urban_pct": pa.Column(
                pa.Float,
                checks=pa.Check.ge(0),      # Percent cannot be negative
                nullable=True
            ),

            "population_growth_annual_pct": pa.Column(
                pa.Float,
                nullable=True               # Can be negative (population decline), so no >= 0 check
            ),
        },

        # DataFrame-level checks
        strict=True,                     # Enforce that no unexpected columns are present
        unique=["country_iso3", "year"], # Ensure each country-year pair appears only once
    )

    return schema
