import pandas as pd

# OSM
osm = pd.read_csv("output/riepilogo_osm_brand.csv")

# Excel (tuo conteggio)
excel = pd.read_excel("MyTraffic_MASTER.xlsx", sheet_name="24_Copertura_Competitor")

excel = excel[["Brand","PV_nel_file"]]

# merge
df = osm.merge(excel, on="Brand", how="left")

df["Differenza"] = df["PV_trovati_OSM"] - df["PV_nel_file"]

df.to_csv("output/confronto_osm_vs_excel.csv", index=False)

print(df)
