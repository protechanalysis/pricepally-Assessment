# validation.py
# This file defines the Pandera schema for data quality checks using a functional approach.

import pandera as pa
from datetime import datetime

# Get the current year for dynamic date range checks
current_year = datetime.now().year

def get_wide_schema() -> pa.DataFrameSchema:
    """
    Builds and returns the validation schema for the wide-format data.
    This is a functional alternative to the class-based DataFrameModel.
    """
    schema = pa.DataFrameSchema(
        columns={
            "country_name": pa.Column(pa.String, nullable=False),
            "country_iso3": pa.Column(
                pa.String,
                checks=[pa.Check.str_matches(r'^[A-Z]{3}$')],
                nullable=False
            ),
            "year": pa.Column(
                pa.Int,
                checks=[
                    pa.Check.ge(2000), # Greater than or equal to 2000
                    pa.Check.le(current_year) # Less than or equal to the current year
                ],
                nullable=False
            ),
            # Define each expected indicator column
            # They are nullable because data may be missing from the source.
            "food_production_idx": pa.Column(pa.Float, nullable=True),
            "cereal_yield_kg_per_hectare": pa.Column(pa.Float, checks=pa.Check.ge(0), nullable=True),
            "crop_production_idx": pa.Column(pa.Float, nullable=True),
            "agricultural_land_pct": pa.Column(pa.Float, checks=pa.Check.ge(0), nullable=True),
            "gdp_per_capita_usd": pa.Column(pa.Float, checks=pa.Check.ge(0), nullable=True),
            "food_cpi_2010_base_100": pa.Column(pa.Float, checks=pa.Check.ge(0), nullable=True),
            "food_imports_pct_merch": pa.Column(pa.Float, checks=pa.Check.ge(0), nullable=True),
            "food_exports_pct_merch": pa.Column(pa.Float, checks=pa.Check.ge(0), nullable=True),
            "population_total": pa.Column(pa.Float, checks=pa.Check.ge(0), nullable=True),
            "population_urban_pct": pa.Column(pa.Float, checks=pa.Check.ge(0), nullable=True),
            "population_growth_annual_pct": pa.Column(pa.Float, nullable=True),
        },
        # DataFrame-level checks
        strict=True,  # Ensure no extra columns are present
        unique=["country_iso3", "year"], # Enforce unique records per country and year
    )
    return schema