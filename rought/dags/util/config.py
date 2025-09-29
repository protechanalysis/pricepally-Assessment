# config.py
# This file contains the configuration for the World Bank ETL pipeline.

# 1. Define the scope for countries (ECOWAS region)
# A dictionary stores ISO codes and full names for clarity.
ecowas_country = {
    "BEN": "Benin", "BFA": "Burkina Faso", "CPV": "Cabo Verde",
    "CIV": "Côte d'Ivoire", "GMB": "The Gambia", "GHA": "Ghana",
    "GIN": "Guinea", "GNB": "Guinea-Bissau", "LBR": "Liberia",
    "MLI": "Mali", "NER": "Niger", "NGA": "Nigeria",
    "SEN": "Senegal", "SLE": "Sierra Leone", "TGO": "Togo"
}

# 2. Define the indicators of interest
# A dictionary maps technical codes to human-readable names.
indicators = {
    # Agriculture & Food Supply
    "AG.PRD.FOOD.XD": "Food production index",
    "AG.YLD.CREL.KG": "Cereal yield (kg per hectare)",
    "AG.PRD.CROP.XD": "Crop production index",
    "AG.LND.AGRI.ZS": "Agricultural land (% of land area)",

    # Economic Access & Trade
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "FP.CPI.TOTL": "Consumer Price Index, Food (2010 = 100)",
    "TM.VAL.FOOD.ZS.UN": "Food imports (% of merchandise imports)",
    "TX.VAL.FOOD.ZS.UN": "Food exports (% of merchandise exports)",

    # Population & Consumption Demand
    "SP.POP.TOTL": "Total population",
    "SP.URB.TOTL.IN.ZS": "Urban population (% of total)",
    "SP.POP.GROW": "Population growth (annual %)"
}

# 4. New mapping for final column names after pivoting
indicators_column_names = {
    "AG.PRD.FOOD.XD": "food_production_idx",
    "AG.YLD.CREL.KG": "cereal_yield_kg_per_hectare",
    "AG.PRD.CROP.XD": "crop_production_idx",
    "AG.LND.AGRI.ZS": "agricultural_land_pct",
    "NY.GDP.PCAP.CD": "gdp_per_capita_usd",
    "FP.CPI.TOTL": "food_cpi_2010_base_100",
    "TM.VAL.FOOD.ZS.UN": "food_imports_pct_merch",
    "TX.VAL.FOOD.ZS.UN": "food_exports_pct_merch",
    "SP.POP.TOTL": "population_total",
    "SP.URB.TOTL.IN.ZS": "population_urban_pct",
    "SP.POP.GROW": "population_growth_annual_pct"
}

database_table_name = "west_african_agri_metrics_wide"

start_year = 2001
end_year = 2021

json_folder = "/opt/airflow/tmp/raw_data.json"

postgres_conn_id = "postgres_default"