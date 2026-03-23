import os
import re
import time
import unicodedata
from pathlib import Path

import pandas as pd
import requests

# =========================
# CONFIG
# =========================
ORS_API_KEY = os.getenv("ORS_API_KEY", "INSERISCI_LA_TUA_API_KEY")
OUT_DIR = Path("output/ors_brand_search_sicilia")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BRANDS = [
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
]

# Varianti utili per migliorare il recall
QUERY_VARIANTS = {
    "DECO": ["Decò Sicilia", "Supermercati Decò Sicilia", "Deco Sicilia Italia"],
    "LIDL": ["Lidl Sicilia", "Lidl Italia Sicilia"],
    "CONAD": ["Conad Sicilia", "Conad Sicilia Italia"],
    "SUPERCONVENIENTE": ["SuperConveniente Sicilia", "Super Conveniente Sicilia"],
    "PAGHI POCO": ["Paghi Poco Sicilia", "PaghiPoco Sicilia", "Paghi Poco Fratello Sicilia"],
    "ARD": ["ARD Discount Sicilia", "ARD Sicilia"],
    "MD": ["MD Sicilia", "MD Discount Sicilia"],
    "EUROSPIN": ["Eurospin Sicilia", "Eurospin Italia Sicilia"],
    "SISA": ["Sisa Sicilia", "SISA Sicilia Italia"],
    "CRAI": ["Crai Sicilia", "CRAI Sicilia Italia"],
    "COOP": ["Coop Sicilia", "COOP Sicilia Italia"],
    "IL CENTESIMO": ["Il Centesimo Sicilia", "Centesimo Sicilia"],
}

GEOCODE_URL = "https://api.openrouteservice.org/geocode/search"

# =========================
# HELPERS
# =========================
def norm_text(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def slugify(x: str) -> str:
    s = norm_text(x).lower().replace(" ", "_")
    return s

def looks_like_sicily(props: dict) -> bool:
    """
    Filtro euristico: tiene risultati che sembrano in Sicilia.
    """
    fields = [
        props.get("label", ""),
        props.get("region", ""),
        props.get("county", ""),
        props.get("locality", ""),
        props.get("localadmin", ""),
        props.get("macroregion", ""),
    ]
    txt = norm_text(" | ".join(str(f) for f in fields))

    sicily_markers = [
        "SICILIA",
        "SICILY",
        "PALERMO",
        "CATANIA",
        "MESSINA",
        "SIRACUSA",
        "RAGUSA",
        "TRAPANI",
        "AGRIGENTO",
        "CALTANISSETTA",
        "ENNA",
    ]
    return any(marker in txt for marker in sicily_markers)

def extract_row(brand: str, feature: dict) -> dict:
    props = feature.get("properties", {}) or {}
    geom = feature.get("geometry", {}) or {}
    coords = geom.get("coordinates", [None, None])

    lon = coords[0] if len(coords) > 0 else None
    lat = coords[1] if len(coords) > 1 else None

    name = props.get("name") or props.get("label") or ""
    city = props.get("locality") or props.get("localadmin") or props.get("county") or ""
    region = props.get("region") or ""
    label = props.get("label") or ""
    source = props.get("source") or ""
    layer = props.get("layer") or ""
    confidence = props.get("confidence")

    return {
        "brand_query": brand,
        "name": name,
        "city": city,
        "region": region,
        "label": label,
        "layer": layer,
        "source": source,
        "confidence": confidence,
        "lat": lat,
        "lon": lon,
    }

def dedupe_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    tmp = df.copy()
    tmp["name_key"] = tmp["name"].fillna("").map(norm_text)
    tmp["city_key"] = tmp["city"].fillna("").map(norm_text)
    tmp["lat_key"] = tmp["lat"].fillna("").astype(str)
    tmp["lon_key"] = tmp["lon"].fillna("").astype(str)

    # prima deduplica forte su coordinate
    tmp = tmp.drop_duplicates(subset=["lat_key", "lon_key", "name_key"])

    # poi deduplica più morbida
    tmp = tmp.drop_duplicates(subset=["name_key", "city_key"])

    return tmp.drop(columns=["name_key", "city_key", "lat_key", "lon_key"], errors="ignore")

def query_ors(text: str, size: int = 40) -> list:
    params = {
        "api_key": ORS_API_KEY,
        "text": text,
        "size": size,
        "boundary.country": "IT",
    }
    r = requests.get(GEOCODE_URL, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data.get("features", [])

# =========================
# MAIN
# =========================
def main():
    if not ORS_API_KEY or ORS_API_KEY == "INSERISCI_LA_TUA_API_KEY":
        raise ValueError("Imposta ORS_API_KEY come variabile ambiente o nello script.")

    summary_rows = []

    for brand in BRANDS:
        print(f"\n=== {brand} ===")
        variants = QUERY_VARIANTS.get(brand, [f"{brand} Sicilia Italia"])
        collected = []

        for q in variants:
            print(f"Query: {q}")
            try:
                features = query_ors(q, size=50)
            except requests.HTTPError as e:
                print(f"HTTP error su '{q}': {e}")
                continue
            except Exception as e:
                print(f"Errore su '{q}': {e}")
                continue

            print(f"Risultati grezzi: {len(features)}")

            for feat in features:
                props = feat.get("properties", {}) or {}
                if looks_like_sicily(props):
                    collected.append(extract_row(brand, feat))

            time.sleep(1.2)  # per non martellare l'API

        df = pd.DataFrame(collected)
        if not df.empty:
            df = dedupe_df(df).sort_values(
                by=["city", "name"], na_position="last"
            ).reset_index(drop=True)

        out_file = OUT_DIR / f"{slugify(brand)}_sicilia.csv"
        df.to_csv(out_file, index=False, encoding="utf-8-sig")

        count = len(df)
        summary_rows.append(
            {
                "brand": brand,
                "results_saved": count,
                "csv_file": str(out_file),
                "note": "Ricerca ORS geocoder, non conteggio ufficiale locator brand",
            }
        )

        print(f"Salvato: {out_file}")
        print(f"Totale salvato: {count}")

    summary = pd.DataFrame(summary_rows)
    summary_file = OUT_DIR / "riepilogo_brand_sicilia.csv"
    summary.to_csv(summary_file, index=False, encoding="utf-8-sig")

    print("\n======================================")
    print(f"Riepilogo salvato in: {summary_file}")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
