import requests
import pandas as pd
import time

# INSERISCI QUI LA TUA API KEY ORS
ORS_API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6IjA1ZTQ4MjQ5M2JhZWY2ZDJlOWJmYmM4NTgzZjVkMWE5ZDk0Y2IyODNiY2ZjZTY4MjdkN2EzNWI0IiwiaCI6Im11cm11cjY0In0="

BRANDS = [
    "DECO", "LIDL", "CONAD", "SUPERCONVENIENTE", "PAGHI POCO", "ARD", "MD", "EUROSPIN", "SISA", "CRAI", "COOP", "IL CENTESIMO"
]

# Bounding box Sicilia: [min_lon, min_lat, max_lon, max_lat]
BBOX_SICILIA = [12.228, 36.619, 15.652, 38.924]

ORS_URL = "https://api.openrouteservice.org/pois"
HEADERS = {"Authorization": ORS_API_KEY, "Content-Type": "application/json"}

for brand in BRANDS:
    print(f"Cerco {brand} in Sicilia...")
    query = {
        "request": "pois",
        "geometry": {
            "bbox": [BBOX_SICILIA],
            "geojson": {"type": "Point", "coordinates": [13.5, 37.5]},
            "buffer": 0
        },
        "filters": {
            "category_ids": [9362],  # Supermercati/negozi alimentari
            "name": brand
        },
        "limit": 500
    }
    try:
        r = requests.post(ORS_URL, headers=HEADERS, json=query, timeout=30)
        r.raise_for_status()
        data = r.json()
        features = data.get("features", [])
        rows = []
        for f in features:
            prop = f.get("properties", {})
            geom = f.get("geometry", {})
            coords = geom.get("coordinates", [None, None])
            rows.append({
                "name": prop.get("name", ""),
                "city": prop.get("locality", ""),
                "lat": coords[1],
                "lon": coords[0]
            })
        df = pd.DataFrame(rows)
        out_path = f"output/{brand.lower()}_sicilia.csv"
        df.to_csv(out_path, index=False)
        print(f"Salvato: {out_path} ({len(df)} risultati)")
    except Exception as e:
        print(f"Errore per {brand}: {e}")
    time.sleep(2)  # Rispetta le quote ORS

print("Fatto!")
