import requests
import pandas as pd

# Step 1: Define indicators of interest
indicators = [
    # Agriculture & Food Supply
    "AG.PRD.FOOD.XD", "AG.YLD.CREL.KG", "AG.PRD.CROP.XD", "AG.PRD.LVSK.XD",
    "AG.LND.AGRI.ZS", "AG.LND.CROP.ZS",
    # Economic Access & Trade
    "TM.VAL.FOOD.ZS.UN", "TX.VAL.FOOD.ZS.UN", "FP.CPI.FOOD", "PA.NUS.FCRF", "NY.GDP.PCAP.CD",
    # Population & Consumption Demand
    "SP.POP.TOTL", "SP.URB.TOTL.IN.ZS", "SP.RUR.TOTL.ZS", "SP.DYN.LE00.IN", "SP.POP.GROW"
]

# Step 2: Define African countries (ISO3 codes)
africa_countries = [
    "NGA","GHA","KEN","UGA","TZA","ZAF","EGY","ETH","CMR","CIV","SEN","MLI","NER","BFA",
    "ZMB","MWI","MOZ","AGO","RWA","BDI","COD","COG","GAB","TGO","BEN","GNB","GNQ","SLE",
    "LBR","NAM","BWA","ZWE","LES","SWZ","CPV","STP","DJI","SOM","SDN","SSD","TCD","CAF",
    "LBY","MAR","DZA","TUN","MRT","ESH","MDG","COM","SYC","ERI","GMB"
]

# Step 3: Helper function to fetch data from World Bank API
def fetch_indicator(indicator, countries, start=2010, end=2023):
    """
    Fetch indicator data for given countries from World Bank API.
    Returns DataFrame with columns: country, countryiso3, date, indicator, value
    """
    all_data = []
    for country in countries:
        url = f"http://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&date={start}:{end}&per_page=200"
        response = requests.get(url)
        if response.status_code != 200:
            continue
        try:
            data = response.json()[1]  # second item is the data
        except:
            continue
        if data:
            for entry in data:
                all_data.append({
                    "country": entry["country"]["value"],
                    "countryiso3": entry["countryiso3code"],
                    "date": entry["date"],
                    "indicator": entry["indicator"]["id"],
                    "value": entry["value"]
                })
    return pd.DataFrame(all_data)

# Step 4: Fetch all indicators
frames = []
for ind in indicators:
    df = fetch_indicator(ind, africa_countries)
    frames.append(df)

# Step 5: Combine into single DataFrame
final_df = pd.concat(frames, ignore_index=True)

# Step 6: Pivot to wide format (one row per country-year, columns = indicators)
pivot_df = final_df.pivot_table(
    index=["country", "countryiso3", "date"],
    columns="indicator",
    values="value"
).reset_index()
