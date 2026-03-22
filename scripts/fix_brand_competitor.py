import pandas as pd

file = "MyTraffic_MASTER.xlsx"

df = pd.read_excel(file, sheet_name="03_Competitor")

def normalize_brand(x):
    if pd.isna(x):
        return "ALTRO"
    x = str(x).upper()

    if "DECO" in x:
        return "DECO"
    if "LIDL" in x:
        return "LIDL"
    if "CONAD" in x:
        return "CONAD"
    if "SUPER" in x:
        return "SUPERCONVENIENTE"
    if "PAGHI" in x:
        return "PAGHI POCO"
    if "ARD" in x:
        return "ARD"
    if "MD" in x:
        return "MD"
    if "EUROSPIN" in x:
        return "EUROSPIN"
    if "SISA" in x:
        return "SISA"
    if "CRAI" in x:
        return "CRAI"
    if "COOP" in x:
        return "COOP"
    if "CENTESIMO" in x:
        return "IL CENTESIMO"

    return "ALTRO"

df["Brand_finale"] = df.iloc[:,3].apply(normalize_brand)

with pd.ExcelWriter(file, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df.to_excel(writer, sheet_name="03_Competitor", index=False)

print("Brand normalizzati.")
