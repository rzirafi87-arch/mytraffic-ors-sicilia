#!/usr/bin/env python3
"""Build ORS drive-time matrices for Sicilia datasets.

Use cases:
- store_competitor: drive time between each store and each competitor
- comune_store: drive time between each comune and each store
"""Costruisce le matrici ORS store->competitor e comune->store.

Funzionalità principali:
- batch massimo configurabile (default 2000 pair per chiamata)
- retry automatico su errori/transienti
- salvataggio progressivo su CSV
- ripresa da output già esistente
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

ORS_MATRIX_URL = "https://api.openrouteservice.org/v2/matrix/driving-car"


class ORSClient:
    def __init__(
        self,
        api_key: str,
        timeout_s: int = 60,
        max_retries: int = 5,
        backoff_s: float = 5.0,
        rate_limit_wait_s: float = 60.0,
        max_backoff_s: float = 300.0,
    ):
        self.api_key = api_key
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_s = backoff_s
        self.rate_limit_wait_s = rate_limit_wait_s
        self.max_backoff_s = max_backoff_s
        self.session = requests.Session()

    def _compute_retry_sleep(self, attempt: int, is_rate_limit: bool, response: requests.Response | None) -> float:
        """Calcola l'attesa di retry con backoff esponenziale conservativo."""
        # Backoff esponenziale (5, 10, 20, 40, ...) con tetto massimo.
        exp_backoff = min(self.max_backoff_s, self.backoff_s * (2 ** (attempt - 1)))

        if not is_rate_limit:
            return exp_backoff

        retry_after_s: float | None = None
        if response is not None:
            retry_after_header = response.headers.get("Retry-After")
            if retry_after_header is not None:
                try:
                    retry_after_s = float(retry_after_header)
                except ValueError:
                    retry_after_s = None

        # Per i 429 attendiamo almeno 60s, rispettando se possibile Retry-After.
        return max(exp_backoff, self.rate_limit_wait_s, retry_after_s or 0.0)

    def matrix_one_to_many(
        self,
        source_lat: float,
        source_lon: float,
        destinations: list[tuple[float, float]],
    ) -> tuple[list[float | None], list[float | None]]:
        """Ritorna (durate_sec, distanze_m) per una singola sorgente verso N destinazioni."""
        locations = [[float(source_lon), float(source_lat)]] + [
            [float(lon), float(lat)] for lat, lon in destinations
        ]
        payload = {
            "locations": locations,
            "sources": [0],
            "destinations": list(range(1, len(locations))),
            "metrics": ["duration", "distance"],
            "units": "m",
        }
        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                res = self.session.post(
                    ORS_MATRIX_URL,
                    headers=headers,
                    json=payload,
                    timeout=self.timeout_s,
                )
                if res.status_code == 429 or 500 <= res.status_code < 600:
                    raise requests.HTTPError(
                        f"status={res.status_code} body={res.text[:500]}", response=res
                    )
                res.raise_for_status()
                body = res.json()
                durations = body.get("durations", [[None]])[0]
                distances = body.get("distances", [[None]])[0]
                return durations, distances
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                if attempt == self.max_retries:
                    raise RuntimeError(f"ORS matrix fallita dopo {attempt} tentativi: {exc}") from exc

                response = getattr(exc, "response", None)
                is_rate_limit = response is not None and response.status_code == 429

                sleep_s = self._compute_retry_sleep(
                    attempt=attempt,
                    is_rate_limit=is_rate_limit,
                    response=response,
                )
                if is_rate_limit:
                    print(
                        f"[retry {attempt}/{self.max_retries}] ORS 429 Rate Limit Exceeded. "
                        f"Attendo {sleep_s:.1f}s prima del prossimo tentativo."
                    )
                else:
                    print(f"[retry {attempt}/{self.max_retries}] errore ORS: {exc}. Attendo {sleep_s:.1f}s")
                time.sleep(sleep_s)

        raise RuntimeError("Retry loop terminato in modo inatteso")


def ensure_columns(df: pd.DataFrame, expected: Iterable[str], file_label: str) -> None:
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"{file_label}: colonne mancanti {missing}. Colonne trovate: {list(df.columns)}")


def load_existing_pairs(path: Path, src_col: str, dst_col: str) -> set[tuple[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return set()
    existing_df = pd.read_csv(path, dtype=str)
    ensure_columns(existing_df, [src_col, dst_col], f"output {path}")
    return set(existing_df[[src_col, dst_col]].itertuples(index=False, name=None))


def append_rows(path: Path, rows: list[dict], columns: list[str]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    chunk_df = pd.DataFrame(rows, columns=columns)
    write_header = not path.exists() or path.stat().st_size == 0
    chunk_df.to_csv(path, mode="a", index=False, header=write_header)


def compute_matrix(
    client: ORSClient,
    sources: pd.DataFrame,
    destinations: pd.DataFrame,
    source_id_col: str,
    dest_id_col: str,
    output_path: Path,
    output_cols: list[str],
    max_pairs_per_batch: int,
    save_every: int,
    limit_pairs: int | None,
    sleep_seconds: float,
) -> None:
    existing = load_existing_pairs(output_path, source_id_col, dest_id_col)
    pending_rows: list[dict] = []
    created_pairs = 0
    requests_made = 0

    dest_records = destinations[[dest_id_col, "lat", "lon"]].to_dict("records")

    for _, src in sources.iterrows():
        src_id = str(src[source_id_col])
        src_lat, src_lon = float(src["lat"]), float(src["lon"])

        missing_dest = [
            d for d in dest_records if (src_id, str(d[dest_id_col])) not in existing
        ]
        if not missing_dest:
            continue

        i = 0
        while i < len(missing_dest):
            if limit_pairs is not None and created_pairs >= limit_pairs:
                append_rows(output_path, pending_rows, output_cols)
                print(f"Raggiunto limit_pairs={limit_pairs}. Stop anticipato.")
                return

            remaining_allowed = (
                max_pairs_per_batch
                if limit_pairs is None
                else min(max_pairs_per_batch, limit_pairs - created_pairs)
            )
            if remaining_allowed <= 0:
                append_rows(output_path, pending_rows, output_cols)
                print(f"Raggiunto limit_pairs={limit_pairs}. Stop anticipato.")
                return

            chunk = missing_dest[i : i + remaining_allowed]
            i += len(chunk)

            if sleep_seconds > 0 and requests_made > 0:
                time.sleep(sleep_seconds)

            durations, distances = client.matrix_one_to_many(
                source_lat=src_lat,
                source_lon=src_lon,
                destinations=[(float(x["lat"]), float(x["lon"])) for x in chunk],
            )
            requests_made += 1

            for idx, dst in enumerate(chunk):
                dst_id = str(dst[dest_id_col])
                dur = durations[idx] if idx < len(durations) else None
                dist = distances[idx] if idx < len(distances) else None
                row = {
                    source_id_col: src_id,
                    dest_id_col: dst_id,
                    "tempo_minuti": round(float(dur) / 60.0, 2) if dur is not None else None,
                    "distanza_km": round(float(dist) / 1000.0, 3) if dist is not None else None,
                }
                pending_rows.append(row)
                existing.add((src_id, dst_id))
                created_pairs += 1

            if len(pending_rows) >= save_every:
                append_rows(output_path, pending_rows, output_cols)
                print(f"Salvate {len(pending_rows)} righe su {output_path}")
                pending_rows.clear()

    append_rows(output_path, pending_rows, output_cols)
    if pending_rows:
        print(f"Salvate {len(pending_rows)} righe finali su {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build matrici ORS Sicilia")
    parser.add_argument("--stores", default="input/negozi_rete.csv")
    parser.add_argument("--competitors", default="input/competitor_sicilia.csv")
    parser.add_argument("--comuni", default="input/comuni_sicilia.csv")
    parser.add_argument("--output-store-competitor", default="output/ors_store_competitor.csv")
    parser.add_argument("--output-comune-store", default="output/ors_comune_store.csv")
    parser.add_argument("--api-key", default=os.getenv("ORS_API_KEY", ""))
    parser.add_argument("--max-pairs-per-batch", type=int, default=2000)
    parser.add_argument("--save-every", type=int, default=500)
    parser.add_argument(
        "--limit-pairs",
        type=int,
        default=None,
        help="Limita il numero totale di nuove coppie calcolate (utile per test)",
    )
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Pausa (in secondi) tra richieste ORS consecutive.",
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
    args = parse_args()

    stores = pd.read_csv(args.stores)
    competitors = pd.read_csv(args.competitors)
    comuni = pd.read_csv(args.comuni)

    ensure_columns(stores, ["store_id", "lat", "lon"], args.stores)
    ensure_columns(competitors, ["competitor_id", "lat", "lon"], args.competitors)
    ensure_columns(comuni, ["comune", "lat", "lon"], args.comuni)

    stores = stores.dropna(subset=["store_id", "lat", "lon"]).copy()
    competitors = competitors.dropna(subset=["competitor_id", "lat", "lon"]).copy()
    comuni = comuni.dropna(subset=["comune", "lat", "lon"]).copy()

    if args.limit_pairs == 0:
        print("limit_pairs=0: validazione input completata, nessuna chiamata ORS eseguita.")
        return

    if not args.api_key:
        raise ValueError("API key ORS mancante. Passa --api-key oppure imposta ORS_API_KEY.")

    client = ORSClient(api_key=args.api_key, timeout_s=args.timeout, max_retries=args.retries)

    compute_matrix(
        client=client,
        sources=stores,
        destinations=competitors,
        source_id_col="store_id",
        dest_id_col="competitor_id",
        output_path=Path(args.output_store_competitor),
        output_cols=["store_id", "competitor_id", "tempo_minuti", "distanza_km"],
        max_pairs_per_batch=args.max_pairs_per_batch,
        save_every=args.save_every,
        limit_pairs=args.limit_pairs,
        sleep_seconds=args.sleep_seconds,
    )

    compute_matrix(
        client=client,
        sources=comuni,
        destinations=stores,
        source_id_col="comune",
        dest_id_col="store_id",
        output_path=Path(args.output_comune_store),
        output_cols=["comune", "store_id", "tempo_minuti", "distanza_km"],
        max_pairs_per_batch=args.max_pairs_per_batch,
        save_every=args.save_every,
        limit_pairs=args.limit_pairs,
        sleep_seconds=args.sleep_seconds,
    )

    print("Completato.")


if __name__ == "__main__":
    main()
