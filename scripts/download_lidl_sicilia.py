
import requests
import pandas as pd

url = "https://www.lidl.it/q/storefinder"
params = {
    "query": "Sicilia",
    "limit": 500
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

try:
    r = requests.get(url, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
except Exception as e:
    print("Errore nel download o parsing JSON:", e)
    with open("output/lidl_sicilia.html", "w", encoding="utf-8") as f:
        f.write(r.text)
    print("Risposta salvata in output/lidl_sicilia.html per analisi manuale.")
    exit(1)

stores = data.get("stores", [])
rows = []
for s in stores:
    address = s.get("address", {})
    rows.append({
        "name": s.get("name"),
        "city": address.get("city"),
        "lat": s.get("geoCoordinates", {}).get("latitude"),
        "lon": s.get("geoCoordinates", {}).get("longitude")
    })

df = pd.DataFrame(rows)
df = df[df["city"].notna()]
df.to_csv("output/lidl_sicilia.csv", index=False)
print("LIDL Sicilia trovati:", len(df))
