#!/usr/bin/env python3
import argparse
import csv
import math
import os
import time
from pathlib import Path

import pandas as pd
import requests


def load_api_key(cli_value=None):
    if cli_value:
        return cli_value.strip()

    env_value = os.getenv("ORS_API_KEY")
    if env_value:
        return env_value.strip()

    root = Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == "ORS_API_KEY":
                return value.strip().strip('"').strip("'")

    raise ValueError("API key ORS mancante. Passa --api-key oppure imposta ORS_API_KEY nel .env.")


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def prepare_batch(df, max_air_km=25, max_per_store=15):
    required = ["store_id", "store_lat", "store_lon", "competitor_id", "competitor_lat", "competitor_lon"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonne mancanti nel CSV input: {missing}")

    for c in ["store_lat", "store_lon", "competitor_lat", "competitor_lon"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["store_lat", "store_lon", "competitor_lat", "competitor_lon"]).copy()

    df["air_km"] = df.apply(
        lambda r: haversine_km(r["store_lat"], r["store_lon"], r["competitor_lat"], r["competitor_lon"]),
        axis=1,
    )

    df = df[df["air_km"] <= max_air_km].copy()
    df = df.sort_values(["store_id", "air_km", "competitor_id"]).copy()
    df["rank_store"] = df.groupby("store_id").cumcount() + 1
    df = df[df["rank_store"] <= max_per_store].copy()
    return df


def ors_route(api_key, start_lon, start_lat, end_lon, end_lat, session, max_retries=4):
    url = "https://api.openrouteservice.org/v2/directions/driving-car"
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    payload = {"coordinates": [[start_lon, start_lat], [end_lon, end_lat]]}
    retry_waits = [4, 8, 16, 30]

    for attempt in range(max_retries + 1):
        try:
            r = session.post(url, headers=headers, json=payload, timeout=45)

            if r.status_code == 200:
                data = r.json()
                features = data.get("features", [])
                if not features:
                    return None, None, "NO_FEATURES", ""
                summary = features[0].get("properties", {}).get("summary", {})
                distance_m = summary.get("distance")
                duration_s = summary.get("duration")
                if distance_m is None or duration_s is None:
                    return None, None, "NO_SUMMARY", ""
                return round(distance_m / 1000, 3), round(duration_s / 60, 2), "OK", ""

            text = r.text[:400]
            if r.status_code == 429 and attempt < max_retries:
                time.sleep(retry_waits[min(attempt, len(retry_waits) - 1)])
                continue
            if r.status_code == 404:
                return None, None, "HTTP_404", text
            if r.status_code == 429:
                return None, None, "HTTP_429", text
            return None, None, f"HTTP_{r.status_code}", text

        except requests.RequestException as e:
            if attempt < max_retries:
                time.sleep(retry_waits[min(attempt, len(retry_waits) - 1)])
                continue
            return None, None, "REQUEST_ERROR", str(e)

    return None, None, "UNKNOWN_ERROR", ""


def main():
    parser = argparse.ArgumentParser(description="Costruisce un batch ORS pulito e lo calcola.")
    parser.add_argument("--input", default="output/ors_input_batch1_fixed.csv")
    parser.add_argument("--prefilter-output", default="output/ors_input_prefiltered.csv")
    parser.add_argument("--output", default="output/ors_output_prefiltered.csv")
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--max-air-km", type=float, default=25.0)
    parser.add_argument("--max-per-store", type=int, default=15)
    parser.add_argument("--sleep", type=float, default=1.5)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    input_path = (root / args.input).resolve()
    prefilter_output_path = (root / args.prefilter_output).resolve()
    output_path = (root / args.output).resolve()
    prefilter_output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    api_key = load_api_key(args.api_key)

    if not input_path.exists():
        raise FileNotFoundError(f"Input non trovato: {input_path}")

    raw = pd.read_csv(input_path)
    batch = prepare_batch(raw, max_air_km=args.max_air_km, max_per_store=args.max_per_store)
    batch.to_csv(prefilter_output_path, index=False)

    existing_keys = set()
    if args.resume and output_path.exists():
        old = pd.read_csv(output_path)
        if {"store_id", "competitor_id"}.issubset(old.columns):
            existing_keys = set(zip(old["store_id"].astype(str), old["competitor_id"].astype(str)))

    file_exists = output_path.exists()
    with output_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists or output_path.stat().st_size == 0:
            writer.writerow(["store_id", "competitor_id", "air_km", "distance_km", "duration_min", "status", "message"])

        session = requests.Session()
        total = len(batch)

        for idx, row in batch.reset_index(drop=True).iterrows():
            store_id = str(row["store_id"])
            competitor_id = str(row["competitor_id"])
            key = (store_id, competitor_id)

            if key in existing_keys:
                print(f"[{idx+1}/{total}] SKIP {store_id} -> {competitor_id} (gia presente)")
                continue

            dist_km, dur_min, status, message = ors_route(
                api_key,
                float(row["store_lon"]),
                float(row["store_lat"]),
                float(row["competitor_lon"]),
                float(row["competitor_lat"]),
                session,
            )
            writer.writerow([
                store_id,
                competitor_id,
                round(float(row["air_km"]), 3),
                dist_km if dist_km is not None else "",
                dur_min if dur_min is not None else "",
                status,
                message,
            ])
            print(f"[{idx+1}/{total}] {status} {store_id} -> {competitor_id} | air={round(float(row['air_km']),3)} km={dist_km} min={dur_min}")

            if idx < total - 1:
                time.sleep(args.sleep)

    print(f"\nPrefiltro salvato in: {prefilter_output_path}")
    print(f"Output ORS salvato in: {output_path}")


if __name__ == "__main__":
    main()
