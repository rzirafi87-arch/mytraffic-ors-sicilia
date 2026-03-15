#!/usr/bin/env python3
"""Build ORS drive-time matrices for Sicilia datasets.

Use cases:
- store_competitor: drive time between each store and each competitor
- comune_store: drive time between each comune and each store
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import time
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from dotenv import load_dotenv

ORS_URL = "https://api.openrouteservice.org/v2/matrix/driving-car"
MAX_PAIRS_PER_BATCH = 2000
SAVE_EVERY_ROWS = 50
MAX_RETRIES = 5
REQUEST_TIMEOUT_SECONDS = 45


class ORSMatrixBuilder:
    def __init__(self, api_key: str, output_dir: Path) -> None:
        self.api_key = api_key
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": self.api_key,
                "Content-Type": "application/json",
            }
        )

    @staticmethod
    def _norm_id(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and math.isnan(value):
            return ""
        return str(value).strip()

    def _load_csv(self, candidates: Iterable[Path], required_columns: list[str]) -> pd.DataFrame:
        for path in candidates:
            if path.exists():
                df = pd.read_csv(path)
                missing = [c for c in required_columns if c not in df.columns]
                if missing:
                    raise ValueError(f"Il file {path} non contiene le colonne richieste: {missing}")
                return df
        checked = ", ".join(str(p) for p in candidates)
        raise FileNotFoundError(f"Nessun file trovato. Percorsi provati: {checked}")

    def _post_matrix(self, locations: list[list[float]], sources: list[int], destinations: list[int]) -> tuple[list[float], list[float]]:
        payload = {
            "locations": locations,
            "sources": sources,
            "destinations": destinations,
            "metrics": ["distance", "duration"],
            "units": "km",
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self.session.post(ORS_URL, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
                if response.status_code == 429:
                    wait_seconds = min(60, 2**attempt)
                    print(f"[WARN] Rate limit ORS (429). Retry tra {wait_seconds}s (tentativo {attempt}/{MAX_RETRIES})")
                    time.sleep(wait_seconds)
                    continue

                response.raise_for_status()
                data = response.json()
                durations = data.get("durations", [])
                distances = data.get("distances", [])

                if not durations or not distances:
                    raise ValueError(f"Risposta ORS senza durations/distances: {data}")

                return durations[0], distances[0]

            except (requests.RequestException, ValueError) as exc:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"Errore ORS dopo {MAX_RETRIES} tentativi: {exc}") from exc
                wait_seconds = min(60, 2**attempt)
                print(f"[WARN] Errore ORS: {exc}. Retry tra {wait_seconds}s (tentativo {attempt}/{MAX_RETRIES})")
                time.sleep(wait_seconds)

        raise RuntimeError("Errore inatteso nel client ORS")

    def _load_done_pairs(self, output_path: Path, key_fields: tuple[str, str]) -> set[tuple[str, str]]:
        done: set[tuple[str, str]] = set()
        if not output_path.exists():
            return done

        existing = pd.read_csv(output_path, usecols=list(key_fields), dtype=str)
        for _, row in existing.iterrows():
            done.add((self._norm_id(row[key_fields[0]]), self._norm_id(row[key_fields[1]])))
        return done

    def _append_rows(self, output_path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = output_path.exists()
        with output_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows)

    def build_store_competitor(self) -> None:
        stores = self._load_csv(
            [Path("input/negozi_rete.csv"), Path("negozi_rete.csv")],
            ["store_id", "lat", "lon"],
        )
        competitors = self._load_csv(
            [Path("input/competitor_sicilia.csv"), Path("competitor_sicilia.csv")],
            ["competitor_id", "lat", "lon"],
        )

        stores = stores.dropna(subset=["store_id", "lat", "lon"]).copy()
        competitors = competitors.dropna(subset=["competitor_id", "lat", "lon"]).copy()

        stores["store_id"] = stores["store_id"].map(self._norm_id)
        competitors["competitor_id"] = competitors["competitor_id"].map(self._norm_id)

        output_path = self.output_dir / "ors_store_competitor.csv"
        done_pairs = self._load_done_pairs(output_path, ("store_id", "competitor_id"))

        total_pairs = len(stores) * len(competitors)
        print(f"[INFO] store-competitor: {len(stores)} store x {len(competitors)} competitor = {total_pairs} coppie")
        print(f"[INFO] Coppie già presenti: {len(done_pairs)}")

        buffer: list[dict[str, object]] = []
        processed = len(done_pairs)
        since_last_flush = 0

        competitors_records = competitors[["competitor_id", "lat", "lon"]].to_dict("records")
        chunk_size = max(1, min(len(competitors_records), MAX_PAIRS_PER_BATCH))

        for _, store in stores.iterrows():
            store_id = self._norm_id(store["store_id"])
            source_coord = [float(store["lon"]), float(store["lat"])]

            for start in range(0, len(competitors_records), chunk_size):
                chunk = competitors_records[start : start + chunk_size]
                pending = [c for c in chunk if (store_id, self._norm_id(c["competitor_id"])) not in done_pairs]
                if not pending:
                    continue

                locations = [source_coord] + [[float(c["lon"]), float(c["lat"])] for c in pending]
                destinations = list(range(1, len(locations)))

                durations, distances = self._post_matrix(locations=locations, sources=[0], destinations=destinations)

                for idx, comp in enumerate(pending):
                    row = {
                        "store_id": store_id,
                        "competitor_id": self._norm_id(comp["competitor_id"]),
                        "tempo_minuti": round(float(durations[idx]) / 60.0, 2),
                        "distanza_km": round(float(distances[idx]), 3),
                    }
                    done_pairs.add((row["store_id"], row["competitor_id"]))
                    buffer.append(row)
                    processed += 1
                    since_last_flush += 1

                    if since_last_flush >= SAVE_EVERY_ROWS:
                        self._append_rows(
                            output_path,
                            buffer,
                            ["store_id", "competitor_id", "tempo_minuti", "distanza_km"],
                        )
                        print(f"[PROGRESS] Salvate {processed}/{total_pairs} righe")
                        buffer.clear()
                        since_last_flush = 0

        if buffer:
            self._append_rows(output_path, buffer, ["store_id", "competitor_id", "tempo_minuti", "distanza_km"])
            print(f"[PROGRESS] Salvate {processed}/{total_pairs} righe")

        print(f"[DONE] File creato/aggiornato: {output_path}")

    def build_comune_store(self) -> None:
        comuni = self._load_csv(
            [Path("input/comuni_sicilia.csv"), Path("comuni_sicilia.csv")],
            ["comune", "lat", "lon"],
        )
        stores = self._load_csv(
            [Path("input/negozi_rete.csv"), Path("negozi_rete.csv")],
            ["store_id", "lat", "lon"],
        )

        comuni = comuni.dropna(subset=["comune", "lat", "lon"]).copy()
        stores = stores.dropna(subset=["store_id", "lat", "lon"]).copy()

        comuni["comune"] = comuni["comune"].map(self._norm_id)
        stores["store_id"] = stores["store_id"].map(self._norm_id)

        output_path = self.output_dir / "ors_comune_store.csv"
        done_pairs = self._load_done_pairs(output_path, ("comune", "store_id"))

        total_pairs = len(comuni) * len(stores)
        print(f"[INFO] comune-store: {len(comuni)} comuni x {len(stores)} store = {total_pairs} coppie")
        print(f"[INFO] Coppie già presenti: {len(done_pairs)}")

        buffer: list[dict[str, object]] = []
        processed = len(done_pairs)
        since_last_flush = 0

        stores_records = stores[["store_id", "lat", "lon"]].to_dict("records")
        chunk_size = max(1, min(len(stores_records), MAX_PAIRS_PER_BATCH))

        for _, comune in comuni.iterrows():
            comune_name = self._norm_id(comune["comune"])
            source_coord = [float(comune["lon"]), float(comune["lat"])]

            for start in range(0, len(stores_records), chunk_size):
                chunk = stores_records[start : start + chunk_size]
                pending = [s for s in chunk if (comune_name, self._norm_id(s["store_id"])) not in done_pairs]
                if not pending:
                    continue

                locations = [source_coord] + [[float(s["lon"]), float(s["lat"])] for s in pending]
                destinations = list(range(1, len(locations)))

                durations, distances = self._post_matrix(locations=locations, sources=[0], destinations=destinations)

                for idx, store in enumerate(pending):
                    row = {
                        "comune": comune_name,
                        "store_id": self._norm_id(store["store_id"]),
                        "tempo_minuti": round(float(durations[idx]) / 60.0, 2),
                        "distanza_km": round(float(distances[idx]), 3),
                    }
                    done_pairs.add((row["comune"], row["store_id"]))
                    buffer.append(row)
                    processed += 1
                    since_last_flush += 1

                    if since_last_flush >= SAVE_EVERY_ROWS:
                        self._append_rows(
                            output_path,
                            buffer,
                            ["comune", "store_id", "tempo_minuti", "distanza_km"],
                        )
                        print(f"[PROGRESS] Salvate {processed}/{total_pairs} righe")
                        buffer.clear()
                        since_last_flush = 0

        if buffer:
            self._append_rows(output_path, buffer, ["comune", "store_id", "tempo_minuti", "distanza_km"])
            print(f"[PROGRESS] Salvate {processed}/{total_pairs} righe")

        print(f"[DONE] File creato/aggiornato: {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ORS matrices for Sicilia datasets")
    parser.add_argument(
        "mode",
        choices=["store_competitor", "comune_store"],
        help="Which matrix to compute",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="OpenRouteService API key (optional if ORS_API_KEY is set)",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output folder for generated CSV files",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    api_key = args.api_key or os.getenv("ORS_API_KEY")
    if not api_key:
        raise SystemExit("Errore: API key mancante. Imposta ORS_API_KEY o usa --api-key")

    builder = ORSMatrixBuilder(api_key=api_key, output_dir=Path(args.output_dir))
    if args.mode == "store_competitor":
        builder.build_store_competitor()
    else:
        builder.build_comune_store()


if __name__ == "__main__":
    main()
