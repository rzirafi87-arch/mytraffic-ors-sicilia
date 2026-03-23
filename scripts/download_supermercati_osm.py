import requests
import pandas as pd

# query Overpass: supermercati in Sicilia
query = """
[out:json][timeout:60];
area["name"="Sicilia"]->.searchArea;
(
  node["shop"="supermarket"](area.searchArea);
  way["shop"="supermarket"](area.searchArea);
  relation["shop"="supermarket"](area.searchArea);
);
out center;
"""

url = "https://overpass-api.de/api/interpreter"
response = requests.post(url, data=query)
data = response.json()

rows = []

for el in data["elements"]:
    tags = el.get("tags", {})
    name = tags.get("name", "")
    brand = tags.get("brand", "")
    city = tags.get("addr:city", "")
    if el["type"] == "node":
        lat = el.get("lat")
        lon = el.get("lon")
    else:
        lat = el.get("center", {}).get("lat")
        lon = el.get("center", {}).get("lon")
    rows.append({
        "name": name,
        "brand": brand,
        "city": city,
        "lat": lat,
        "lon": lon
    })

df = pd.DataFrame(rows)

# pulizia base
df = df.dropna(subset=["lat","lon"])

# salva file
df.to_csv("output/supermercati_sicilia_osm.csv", index=False, encoding="utf-8-sig")

print("Tot supermercati trovati:", len(df))
print("File creato: output/supermercati_sicilia_osm.csv")
