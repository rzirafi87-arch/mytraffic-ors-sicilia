import pandas as pd
import os
import re
import unicodedata

def norm(x):
    if pd.isna(x):
        return ""
    x = str(x).upper().strip()
    x = unicodedata.normalize("NFKD", x).encode("ascii","ignore").decode("ascii")
    x = re.sub(r"[^A-Z0-9 ]"," ",x)
    x = re.sub(r"\s+"," ",x)
    return x

folder = "output/osm_brand_search_sicilia"

# carica tuo file
comp = pd.read_excel("MyTraffic_MASTER.xlsx", sheet_name="03_Competitor")


# Usa colonne corrette per nome/insegna e comune
col_nome = "Brand"
col_comune = "Comune"
comp["key"] = comp[col_nome].apply(norm) + "|" + comp[col_comune].apply(norm)

output_folder = "output/negozi_mancanti_per_brand"
os.makedirs(output_folder, exist_ok=True)

for file in os.listdir(folder):
    if file.endswith(".csv"):
        brand = file.replace("_sicilia.csv","").upper()
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
        if "name" not in df.columns:
            continue
        df["key"] = df["name"].apply(norm) + "|" + df.get("city","").apply(norm)
        missing = df[~df["key"].isin(comp["key"])]
        missing.to_csv(f"{output_folder}/{brand}_missing.csv", index=False)
        print(f"{brand}  mancanti: {len(missing)}")
