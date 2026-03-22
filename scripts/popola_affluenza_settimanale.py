import pandas as pd
import numpy as np

file = "MyTraffic_MASTER.xlsx"

# carica foglio
df = pd.read_excel(file, sheet_name="25_Affluenza_Settimanale_Store")

# genera pattern affluenza realistico (retail GDO)
pattern = {
    "Lun": 0.75,
    "Mar": 0.80,
    "Mer": 0.85,
    "Gio": 0.90,
    "Ven": 1.10,
    "Sab": 1.30,
    "Dom": 1.05
}

# base traffico (puoi cambiare scala dopo)
base = 100

for giorno, coeff in pattern.items():
    df[giorno] = (base * coeff * np.random.uniform(0.9, 1.1, len(df))).round(0)

# media settimanale
df["Media_settimanale"] = df[["Lun","Mar","Mer","Gio","Ven","Sab","Dom"]].mean(axis=1)

# indice affluenza (normalizzato 0-100)
max_val = df["Media_settimanale"].max()
df["Indice_affluenza"] = (df["Media_settimanale"] / max_val * 100).round(0)

# salva
with pd.ExcelWriter(file, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df.to_excel(writer, sheet_name="25_Affluenza_Settimanale_Store", index=False)

print("✅ Foglio affluenza popolato correttamente")
