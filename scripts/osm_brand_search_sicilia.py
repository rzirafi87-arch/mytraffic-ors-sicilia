import requests
import pandas as pd
import time
from pathlib import Path

# Bounding box Sicilia: min_lon, min_lat, max_lon, max_lat
BBOX = [12.228, 36.619, 15.652, 38.924]

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

OUT_DIR = Path("output/osm_brand_search_sicilia")
OUT_DIR.mkdir(parents=True, exist_ok=True)


OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter"
]
MAX_RETRIES = 5
RETRY_WAIT = 30  # secondi

# Query OSM per ogni brand con retry e round robin endpoint
for brand in BRANDS:
    print(f"Cerco {brand} in Sicilia...")
    query = f"""
    [out:json][timeout:60];
    (
      node[\"brand\"~\"{brand}\",i][\"shop\"][\"shop\"~\"supermarket|convenience|discount\",i]({BBOX[1]},{BBOX[0]},{BBOX[3]},{BBOX[2]});
      way[\"brand\"~\"{brand}\",i][\"shop\"][\"shop\"~\"supermarket|convenience|discount\",i]({BBOX[1]},{BBOX[0]},{BBOX[3]},{BBOX[2]});
      relation[\"brand\"~\"{brand}\",i][\"shop\"][\"shop\"~\"supermarket|convenience|discount\",i]({BBOX[1]},{BBOX[0]},{BBOX[3]},{BBOX[2]});
    );
    out center;
    """
    success = False
    for attempt in range(1, MAX_RETRIES + 1):
        for url in OVERPASS_URLS:
            try:
                print(f"Tentativo {attempt} su {url}")
                r = requests.post(url, data={"data": query}, timeout=120)
                r.raise_for_status()
                data = r.json()
                elements = data.get("elements", [])
                rows = []
                for el in elements:
                    tags = el.get("tags", {})
                    lat = el.get("lat") or el.get("center", {}).get("lat")
                    lon = el.get("lon") or el.get("center", {}).get("lon")
                    rows.append({
                        "brand": brand,
                        "name": tags.get("name", ""),
                        "city": tags.get("addr:city", ""),
                        "lat": lat,
                        "lon": lon,
                        "shop": tags.get("shop", ""),
                        "osm_id": el.get("id"),
                        "type": el.get("type"),
                    })
                df = pd.DataFrame(rows)
                out_path = OUT_DIR / f"{brand.lower()}_sicilia.csv"
                df.to_csv(out_path, index=False)
                print(f"Salvato: {out_path} ({len(df)} risultati)")
                success = True
                break
            except Exception as e:
                print(f"Errore per {brand} su {url}: {e}")
                time.sleep(RETRY_WAIT)
        if success:
            break
    if not success:
        print(f"[FALLITO] {brand} dopo {MAX_RETRIES} tentativi su tutti gli endpoint.")
    time.sleep(2)

print("Fatto!")
