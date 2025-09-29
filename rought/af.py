import pandas as pd
import requests

# World Bank API base URL
BASE_URL = "http://api.worldbank.org/v2"

# Africa region code
REGION = "AFR"

# Indicators (selected for Food & Agriculture context)
INDICATORS = {
    "AG.PRD.FOOD.XD": "Food production index (2004-2006=100)",
    "AG.YLD.CREL.KG": "Cereal yield (kg per hectare)",
    "AG.CON.FERT.ZS": "Fertilizer consumption (% of fertilizer production)",
    "AG.LND.AGRI.ZS": "Agricultural land (% of land area)",
    "TM.VAL.FOOD.ZS.UN": "Food imports (% of merchandise imports)",
    "NE.EXP.GNFS.ZS": "Exports of goods and services (% of GDP)",
    "NE.IMP.GNFS.ZS": "Imports of goods and services (% of GDP)",
    "SP.POP.TOTL": "Population, total",
    "SP.URB.TOTL.IN.ZS": "Urban population (% of total population)",
    "SP.DYN.LE00.IN": "Life expectancy at birth, total (years)",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)"
}

def fetch_world_bank_data(region, indicators, start_year=2000, end_year=2023):
    """Fetch multiple indicators for a region from World Bank API."""
    all_data = []

    for code, name in indicators.items():
        url = f"{BASE_URL}/region/{region}/indicator/{code}?date={start_year}:{end_year}&format=json&per_page=2000"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Failed to fetch {code}")
            continue

        data = response.json()
        if not data or len(data) < 2:
            print(f"No data found for {code}")
            continue

        records = data[1]
        for record in records:
            all_data.append({
                "indicator_code": code,
                "indicator_name": name,
                "country": record.get("country", {}).get("value"),
                "date": record.get("date"),
                "value": record.get("value")
            })

    return pd.DataFrame(all_data)

# Fetch data
df = fetch_world_bank_data(REGION, INDICATORS)

# Save to CSV
df.to_csv("africa_food_agriculture_indicators.csv", index=False)

print("✅ Data fetched and saved as africa_food_agriculture_indicators.csv")
print(df.head())
