import pandas as pd

# carica file competitor (modifica nome se diverso)
df = pd.read_excel("MyTraffic_MASTER.xlsx", sheet_name="03_Competitor")


# Trova la colonna brand in modo robusto
brand_col = None
for col in df.columns:
	if "brand" in str(col).strip().lower().replace('_',''):
		brand_col = col
		break
if not brand_col:
	raise Exception("Colonna brand non trovata nel foglio 03_Competitor")
# normalizza brand (importantissimo)
df[brand_col] = df[brand_col].astype(str).str.upper().str.strip()


# conta punti vendita per brand
conteggio = df.groupby(brand_col).size().reset_index(name="PV_nel_file")

# ordina
df_out = conteggio.sort_values(by="PV_nel_file", ascending=False)

print(df_out)

# salva output
df_out.to_csv("output/pv_per_brand.csv", index=False)

print("\nFile salvato in: output/pv_per_brand.csv")
