import pandas as pd
import os

folder = "output/osm_brand_search_sicilia"

summary = []


for file in os.listdir(folder):
    if file.endswith(".csv"):
        brand = file.replace("_sicilia.csv", "").upper()
        path = os.path.join(folder, file)
        # Salta file vuoti o troppo piccoli
        if os.path.getsize(path) < 10:
            print(f"Salto {file}: file vuoto o non valido")
            continue
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"Errore lettura {file}: {e}")
            continue
        summary.append({
            "Brand": brand,
            "PV_trovati_OSM": len(df)
        })

df_summary = pd.DataFrame(summary).sort_values("PV_trovati_OSM", ascending=False)

df_summary.to_csv("output/riepilogo_osm_brand.csv", index=False)

print(df_summary)
