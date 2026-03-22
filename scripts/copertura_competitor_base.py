import pandas as pd

brand_target = [
    "DECO",
    "LIDL",
    "CONAD",
    "SUPERCONVENIENTE",
    "PAGHI POCO",
    "ARD",
    "MD",
    "EUROSPIN",
    "SISA",
    "CRAI",
    "COOP",
    "IL CENTESIMO",
    "ALTRO"
]

df = pd.read_csv("output/pv_per_brand.csv")

# Trova la colonna brand in modo robusto
brand_col = None
for col in df.columns:
    if "brand" in str(col).strip().lower().replace('_',''):
        brand_col = col
        break
if not brand_col:
    raise Exception("Colonna brand non trovata nel CSV")
# normalizza brand
df[brand_col] = df[brand_col].astype(str).str.upper().str.strip()

mappa = dict(zip(df[brand_col], df["PV_nel_file"]))

out = pd.DataFrame({
    "Brand": brand_target,
    "PV_ufficiali_Sicilia": [None]*len(brand_target),
    "PV_nel_file": [mappa.get(b, 0) for b in brand_target],
    "Gap": [None]*len(brand_target),
    "Copertura": ["N.D."]*len(brand_target),
    "Stato": ["N.D."]*len(brand_target),
    "Note": [""]*len(brand_target)
})

out.to_csv("output/copertura_competitor_base.csv", index=False, encoding="utf-8-sig")
print(out.to_string(index=False))
print("\nCreato: output/copertura_competitor_base.csv")
