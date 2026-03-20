#!/usr/bin/env python3
import argparse
import csv
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


def ensure_columns(df, required):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonne mancanti nel CSV input: {missing}")


def ors_route(api_key, start_lon, start_lat, end_lon, end_lat, session):
    url = "https://api.openrouteservice.org/v2/directions/driving-car"
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "coordinates": [
            [start_lon, start_lat],
            [end_lon, end_lat],
        ]
    }

    try:
        r = session.post(url, headers=headers, json=payload, timeout=45)
        if r.status_code != 200:
            return None, None, f"HTTP_{r.status_code}: {r.text[:300]}"
        data = r.json()
        routes = data.get("routes", [])
        if not routes:
            return None, None, f"NO_ROUTES: {str(data)[:300]}"
        summary = routes[0].get("summary", {})
        distance_m = summary.get("distance")
        duration_s = summary.get("duration")
        if distance_m is None or duration_s is None:
            return None, None, "NO_SUMMARY"
        return round(distance_m / 1000, 3), round(duration_s / 60, 2), "OK"
    except requests.RequestException as e:
        return None, None, f"REQUEST_ERROR: {e}"


def main():
    parser = argparse.ArgumentParser(description="Calcola batch ORS da CSV store-competitor.")
    parser.add_argument("--input", default="output/ors_input_batch1_fixed.csv", help="Percorso CSV input")
    parser.add_argument("--output", default="output/ors_output_batch1.csv", help="Percorso CSV output")
    parser.add_argument("--api-key", default=None, help="Chiave API OpenRouteService")
    parser.add_argument("--sleep", type=float, default=1.0, help="Pausa tra chiamate in secondi")
    parser.add_argument("--resume", action="store_true", help="Salta le righe già presenti nell'output")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    input_path = (root / args.input).resolve()
    output_path = (root / args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    api_key = load_api_key(args.api_key)

    if not input_path.exists():
        raise FileNotFoundError(f"File input non trovato: {input_path}")

    df = pd.read_csv(input_path)
    required = ["store_id", "store_lat", "store_lon", "competitor_id", "competitor_lat", "competitor_lon"]
    ensure_columns(df, required)

    existing_keys = set()
    if args.resume and output_path.exists():
        old = pd.read_csv(output_path)
        if {"store_id", "competitor_id"}.issubset(old.columns):
            existing_keys = set(zip(old["store_id"].astype(str), old["competitor_id"].astype(str)))

    file_exists = output_path.exists()
    with output_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists or output_path.stat().st_size == 0:
            writer.writerow([
                "store_id",
                "competitor_id",
                "distance_km",
                "duration_min",
                "status",
                "message",
            ])

        session = requests.Session()
        total = len(df)

        for idx, row in df.iterrows():
            store_id = str(row["store_id"])
            competitor_id = str(row["competitor_id"])
            key = (store_id, competitor_id)

            if key in existing_keys:
                print(f"[{idx+1}/{total}] SKIP {store_id} -> {competitor_id} (gia presente)")
                continue

            try:
                s_lat = float(row["store_lat"])
                s_lon = float(row["store_lon"])
                c_lat = float(row["competitor_lat"])
                c_lon = float(row["competitor_lon"])
            except Exception as e:
                writer.writerow([store_id, competitor_id, "", "", "ERROR", f"COORD_PARSE: {e}"])
                print(f"[{idx+1}/{total}] ERROR {store_id} -> {competitor_id}: coordinate non valide")
                continue

            distance_km, duration_min, status = ors_route(api_key, s_lon, s_lat, c_lon, c_lat, session)
            message = "" if status == "OK" else status
            writer.writerow([store_id, competitor_id, distance_km or "", duration_min or "", status, message])
            print(f"[{idx+1}/{total}] {status} {store_id} -> {competitor_id} | km={distance_km} min={duration_min}")

            if idx < total - 1:
                time.sleep(args.sleep)

    print(f"\nCompletato. Output salvato in: {output_path}")


if __name__ == "__main__":
    main()
