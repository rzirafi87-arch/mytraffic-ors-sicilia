#!/usr/bin/env python3
"""Costruisce le matrici ORS store->competitor e comune->store.

Funzionalità principali:
- esporta i CSV operativi da MyTraffic_MASTER.xlsx
- batch massimo configurabile (default 2000 pair per chiamata)
- retry automatico su errori/transienti
- salvataggio progressivo su CSV
- ripresa da output già esistente
"""

from __future__ import annotations

import argparse
import os
import re
import time
import zipfile
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET

import pandas as pd
import requests

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


def normalize_column_name(value: str) -> str:
    value = str(value).strip().lower()
    return re.sub(r"[^a-z0-9]+", "", value)


def map_columns(df: pd.DataFrame, mapping: dict[str, list[str]], sheet_name: str) -> pd.DataFrame:
    normalized = {normalize_column_name(col): col for col in df.columns}
    rename_map: dict[str, str] = {}

    for target, aliases in mapping.items():
        source_col = None
        for alias in aliases:
            candidate = normalized.get(normalize_column_name(alias))
            if candidate is not None:
                source_col = candidate
                break
        if source_col is None:
            raise ValueError(
                f"Foglio {sheet_name}: impossibile mappare la colonna '{target}'. "
                f"Colonne trovate: {list(df.columns)}"
            )
        rename_map[source_col] = target

    mapped = df.rename(columns=rename_map)
    return mapped[list(mapping.keys())].copy()


def _xlsx_column_index(cell_ref: str) -> int:
    letters = ''.join(ch for ch in cell_ref if ch.isalpha())
    index = 0
    for letter in letters:
        index = index * 26 + (ord(letter.upper()) - ord('A') + 1)
    return max(index - 1, 0)


def _xlsx_cell_text(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")

    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.findall('.//{*}t'))

    value_node = cell.find('{*}v')
    if value_node is None or value_node.text is None:
        return ""

    raw_value = value_node.text
    if cell_type == "s":
        return shared_strings[int(raw_value)]
    return raw_value


def load_excel_sheet(excel_path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        with zipfile.ZipFile(excel_path) as archive:
            shared_strings = []
            if 'xl/sharedStrings.xml' in archive.namelist():
                shared_root = ET.fromstring(archive.read('xl/sharedStrings.xml'))
                for item in shared_root.findall('{*}si'):
                    shared_strings.append(''.join(node.text or '' for node in item.findall('.//{*}t')))

            workbook_root = ET.fromstring(archive.read('xl/workbook.xml'))
            rels_root = ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))
            relationships = {
                rel.attrib['Id']: rel.attrib['Target']
                for rel in rels_root.findall('{*}Relationship')
            }

            sheet_target = None
            for sheet in workbook_root.findall('.//{*}sheet'):
                if sheet.attrib.get('name') == sheet_name:
                    rel_id = sheet.attrib.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
                    sheet_target = relationships.get(rel_id)
                    break

            if sheet_target is None:
                raise ValueError(f"Foglio Excel non trovato: {sheet_name}")

            sheet_path = sheet_target if sheet_target.startswith('xl/') else f"xl/{sheet_target}"
            sheet_root = ET.fromstring(archive.read(sheet_path))
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"Il file {excel_path} non è un .xlsx valido.") from exc
    except KeyError as exc:
        raise RuntimeError(f"Struttura .xlsx incompleta in {excel_path}: {exc}") from exc

    rows: list[list[str]] = []
    max_width = 0
    for row in sheet_root.findall('.//{*}sheetData/{*}row'):
        values: list[str] = []
        for cell in row.findall('{*}c'):
            col_idx = _xlsx_column_index(cell.attrib.get('r', 'A1'))
            while len(values) < col_idx:
                values.append('')
            values.append(_xlsx_cell_text(cell, shared_strings))
        while values and values[-1] == '':
            values.pop()
        if values:
            rows.append(values)
            max_width = max(max_width, len(values))

    if not rows:
        return pd.DataFrame()

    normalized_rows = [row + [''] * (max_width - len(row)) for row in rows]
    header = normalized_rows[0]
    data = normalized_rows[1:]
    return pd.DataFrame(data, columns=header)


def export_excel_inputs(excel_path: Path, stores_csv: Path, competitors_csv: Path) -> None:
    if not excel_path.exists():
        raise FileNotFoundError(f"File Excel non trovato: {excel_path}")
    if excel_path.stat().st_size == 0:
        raise ValueError(f"File Excel vuoto: {excel_path}")

    stores_mapping = {
        "store_id": ["store_id", "id_store", "id negozio", "id_negozio", "codice_store", "codice negozio"],
        "brand": ["brand", "insegna"],
        "comune": ["comune", "citta", "città"],
        "provincia": ["provincia", "prov"],
        "lat": ["lat", "latitude", "latitudine"],
        "lon": ["lon", "lng", "longitude", "longitudine"],
    }
    competitors_mapping = {
        "competitor_id": ["competitor_id", "id_competitor", "id competitor", "codice_competitor"],
        "brand": ["brand", "insegna"],
        "comune": ["comune", "citta", "città"],
        "indirizzo": ["indirizzo", "address"],
        "lat": ["lat", "latitude", "latitudine"],
        "lon": ["lon", "lng", "longitude", "longitudine"],
        "peso_competitor": ["peso_competitor", "peso competitor", "peso"],
        "livello_competitor": ["livello_competitor", "livello competitor", "livello"],
    }

    stores_raw = load_excel_sheet(excel_path, "02_Negozi")
    competitors_raw = load_excel_sheet(excel_path, "03_Competitor")

    print(f"Excel loaded: {excel_path}")

    stores_df = map_columns(stores_raw, stores_mapping, "02_Negozi")
    competitors_df = map_columns(competitors_raw, competitors_mapping, "03_Competitor")

    stores_csv.parent.mkdir(parents=True, exist_ok=True)
    competitors_csv.parent.mkdir(parents=True, exist_ok=True)
    stores_df.to_csv(stores_csv, index=False)
    competitors_df.to_csv(competitors_csv, index=False)

    print(f"CSV generated: {stores_csv}")
    print(f"CSV generated: {competitors_csv}")


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
    parser.add_argument("--excel", default="MyTraffic_MASTER.xlsx")
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
    args = parse_args()

    export_excel_inputs(
        excel_path=Path(args.excel),
        stores_csv=Path(args.stores),
        competitors_csv=Path(args.competitors),
    )

    stores = pd.read_csv(args.stores)
    competitors = pd.read_csv(args.competitors)
    comuni = pd.read_csv(args.comuni)

    ensure_columns(stores, ["store_id", "brand", "comune", "provincia", "lat", "lon"], args.stores)
    ensure_columns(
        competitors,
        ["competitor_id", "brand", "comune", "indirizzo", "lat", "lon", "peso_competitor", "livello_competitor"],
        args.competitors,
    )
    ensure_columns(comuni, ["comune", "lat", "lon"], args.comuni)

    stores = stores.dropna(subset=["store_id", "lat", "lon"]).copy()
    competitors = competitors.dropna(subset=["competitor_id", "lat", "lon"]).copy()
    comuni = comuni.dropna(subset=["comune", "lat", "lon"]).copy()

    print("ORS processing started")

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
