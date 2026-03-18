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
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET

import pandas as pd
import requests
from dotenv import load_dotenv

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

    @staticmethod
    def _format_response_message(response: requests.Response | None) -> str:
        if response is None:
            return "<nessuna risposta>"
        message = response.text.strip()
        return message if message else "<body vuoto>"

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
                        f"status={res.status_code} body={self._format_response_message(res)}",
                        response=res,
                    )
                res.raise_for_status()
                body = res.json()
                durations = body.get("durations", [[None]])[0]
                distances = body.get("distances", [[None]])[0]
                return durations, distances
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                response = getattr(exc, "response", None)
                if response is not None:
                    print(
                        "ORS request failed: "
                        f"status_code={response.status_code}, "
                        f"response_message={self._format_response_message(response)}"
                    )
                if attempt == self.max_retries:
                    raise RuntimeError(f"ORS matrix fallita dopo {attempt} tentativi: {exc}") from exc

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
    detected_columns = list(df.columns)
    rename_map: dict[str, str] = {}
    detected_mapping: dict[str, str] = {}

    print(f"Foglio {sheet_name}: colonne rilevate {detected_columns}")

    for target, aliases in mapping.items():
        source_col = None
        candidate_aliases = [target, *aliases]
        for alias in candidate_aliases:
            candidate = normalized.get(normalize_column_name(alias))
            if candidate is not None:
                source_col = candidate
                break
        if source_col is None:
            raise ValueError(
                f"Foglio {sheet_name}: colonna obbligatoria '{target}' non trovata. "
                f"Alias tentati: {candidate_aliases}. Colonne disponibili: {detected_columns}"
            )
        rename_map[source_col] = target
        detected_mapping[target] = source_col

    print(f"Foglio {sheet_name}: mapping colonne {detected_mapping}")

    mapped = df.rename(columns=rename_map)
    return mapped[list(mapping.keys())].copy()


def _xlsx_column_index(cell_ref: str) -> int:
    letters = ''.join(ch for ch in cell_ref if ch.isalpha())
    index = 0
    for letter in letters:
        index = index * 26 + (ord(letter.upper()) - ord('A') + 1)
    return max(index - 1, 0)


def _xlsx_column_name(index: int) -> str:
    if index < 0:
        raise ValueError(f"Indice colonna non valido: {index}")
    index += 1
    letters: list[str] = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(ord("A") + remainder))
    return "".join(reversed(letters))


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


def _normalize_key(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_comune(value: object) -> str:
    return _normalize_key(value).casefold()


def _find_header_index(header_map: dict[str, int], aliases: Iterable[str], sheet_name: str, required: bool = True) -> int | None:
    for alias in aliases:
        idx = header_map.get(normalize_column_name(alias))
        if idx is not None:
            return idx
    if required:
        raise ValueError(
            f"Foglio {sheet_name}: nessuna colonna trovata per alias {list(aliases)}. "
            f"Header disponibili: {list(header_map.keys())}"
        )
    return None


def _cell_has_formula(cell: ET.Element | None) -> bool:
    return cell is not None and cell.find("{*}f") is not None


def _clear_cell_value(cell: ET.Element) -> None:
    for child_name in ("{*}v", "{*}is"):
        child = cell.find(child_name)
        if child is not None:
            cell.remove(child)
    if cell.attrib.get("t") == "inlineStr":
        cell.attrib.pop("t", None)


def _set_cell_value(cell: ET.Element, value: object) -> None:
    _clear_cell_value(cell)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return

    if isinstance(value, bool):
        value = int(value)

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        v = ET.SubElement(cell, "v")
        v.text = str(value)
        return

    cell.attrib["t"] = "inlineStr"
    is_node = ET.SubElement(cell, "is")
    t_node = ET.SubElement(is_node, "t")
    t_node.text = str(value)


class XLSXWorkbookEditor:
    def __init__(self, excel_path: Path):
        self.excel_path = excel_path
        self._file_map: dict[str, bytes] = {}
        self._sheet_cache: dict[str, ET.Element] = {}
        self._sheet_paths: dict[str, str] = {}
        self._shared_strings: list[str] = []
        self._load()

    def _load(self) -> None:
        if not self.excel_path.exists():
            raise FileNotFoundError(f"File Excel non trovato: {self.excel_path}")
        if self.excel_path.stat().st_size == 0:
            raise ValueError(f"File Excel vuoto: {self.excel_path}")

        try:
            with zipfile.ZipFile(self.excel_path) as archive:
                self._file_map = {name: archive.read(name) for name in archive.namelist()}
        except zipfile.BadZipFile as exc:
            raise RuntimeError(f"Il file {self.excel_path} non è un .xlsx valido.") from exc

        if "xl/sharedStrings.xml" in self._file_map:
            shared_root = ET.fromstring(self._file_map["xl/sharedStrings.xml"])
            for item in shared_root.findall("{*}si"):
                self._shared_strings.append("".join(node.text or "" for node in item.findall(".//{*}t")))

        workbook_root = ET.fromstring(self._file_map["xl/workbook.xml"])
        rels_root = ET.fromstring(self._file_map["xl/_rels/workbook.xml.rels"])
        relationships = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in rels_root.findall("{*}Relationship")
        }
        for sheet in workbook_root.findall(".//{*}sheet"):
            name = sheet.attrib.get("name")
            rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            target = relationships.get(rel_id)
            if name is None or target is None:
                continue
            self._sheet_paths[name] = target if target.startswith("xl/") else f"xl/{target}"

    def get_sheet_root(self, sheet_name: str) -> ET.Element:
        sheet_path = self._sheet_paths.get(sheet_name)
        if sheet_path is None:
            raise ValueError(f"Foglio Excel non trovato: {sheet_name}")
        if sheet_path not in self._sheet_cache:
            self._sheet_cache[sheet_path] = ET.fromstring(self._file_map[sheet_path])
        return self._sheet_cache[sheet_path]

    def _sheet_data(self, sheet_root: ET.Element) -> ET.Element:
        sheet_data = sheet_root.find("{*}sheetData")
        if sheet_data is None:
            raise ValueError("sheetData mancante nel foglio Excel")
        return sheet_data

    def _rows(self, sheet_root: ET.Element) -> list[ET.Element]:
        return self._sheet_data(sheet_root).findall("{*}row")

    def _find_header_row(
        self,
        sheet_root: ET.Element,
        header_aliases: Iterable[str],
    ) -> tuple[ET.Element, dict[str, int]]:
        rows = self._rows(sheet_root)
        if not rows:
            raise ValueError("Il foglio Excel è vuoto e non contiene header.")

        expected_headers = {normalize_column_name(alias) for alias in header_aliases}
        best_row: ET.Element | None = None
        best_header_map: dict[str, int] = {}
        best_score = -1

        for row in rows:
            current_map: dict[str, int] = {}
            for cell in row.findall("{*}c"):
                cell_value = normalize_column_name(_xlsx_cell_text(cell, self._shared_strings))
                if not cell_value:
                    continue
                col_idx = _xlsx_column_index(cell.attrib.get("r", "A1"))
                current_map[cell_value] = col_idx

            if not current_map:
                continue

            score = len(set(current_map) & expected_headers)
            if score > best_score:
                best_row = row
                best_header_map = current_map
                best_score = score

        if best_row is None:
            raise ValueError("Il foglio Excel è vuoto e non contiene header.")

        return best_row, best_header_map

    def _get_cell(self, row: ET.Element, col_idx: int) -> ET.Element | None:
        target_ref = _xlsx_column_name(col_idx)
        for cell in row.findall("{*}c"):
            ref = cell.attrib.get("r", "")
            if "".join(ch for ch in ref if ch.isalpha()) == target_ref:
                return cell
        return None

    def _create_cell(self, row: ET.Element, row_idx: int, col_idx: int) -> ET.Element:
        target_ref = f"{_xlsx_column_name(col_idx)}{row_idx}"
        new_cell = ET.Element("c", {"r": target_ref})
        inserted = False
        existing_cells = row.findall("{*}c")
        for position, cell in enumerate(existing_cells):
            existing_idx = _xlsx_column_index(cell.attrib.get("r", "A1"))
            if existing_idx > col_idx:
                row.insert(position, new_cell)
                inserted = True
                break
        if not inserted:
            row.append(new_cell)
        return new_cell

    def _get_or_create_row(self, sheet_root: ET.Element, row_idx: int) -> ET.Element:
        sheet_data = self._sheet_data(sheet_root)
        rows = self._rows(sheet_root)
        for position, row in enumerate(rows):
            current_idx = int(row.attrib.get("r", "0"))
            if current_idx == row_idx:
                return row
            if current_idx > row_idx:
                new_row = ET.Element("row", {"r": str(row_idx)})
                sheet_data.insert(position, new_row)
                return new_row
        new_row = ET.Element("row", {"r": str(row_idx)})
        sheet_data.append(new_row)
        return new_row

    def _write_value(self, row: ET.Element, row_idx: int, col_idx: int, value: object, force: bool = False) -> None:
        cell = self._get_cell(row, col_idx)
        if cell is None:
            cell = self._create_cell(row, row_idx, col_idx)
        if not force and _cell_has_formula(cell):
            return
        _set_cell_value(cell, value)

    def _clear_column_values(self, row: ET.Element, col_idx: int) -> None:
        cell = self._get_cell(row, col_idx)
        if cell is None or _cell_has_formula(cell):
            return
        _clear_cell_value(cell)

    def _update_dimension(self, sheet_root: ET.Element) -> None:
        rows = self._rows(sheet_root)
        max_row = 1
        max_col = 0
        for row in rows:
            row_idx = int(row.attrib.get("r", "0"))
            max_row = max(max_row, row_idx)
            for cell in row.findall("{*}c"):
                max_col = max(max_col, _xlsx_column_index(cell.attrib.get("r", "A1")))
        dim = sheet_root.find("{*}dimension")
        if dim is None:
            dim = ET.Element("dimension")
            sheet_root.insert(0, dim)
        dim.attrib["ref"] = f"A1:{_xlsx_column_name(max_col)}{max_row}"

    def upsert_rows(
        self,
        sheet_name: str,
        keys: list[tuple[str, list[str], bool]],
        value_columns: list[tuple[str, list[str], bool]],
        rows_to_write: list[dict[str, object]],
        normalizers: dict[str, callable],
    ) -> dict[str, object]:
        sheet_root = self.get_sheet_root(sheet_name)
        header_aliases = [
            alias
            for _, aliases, _ in [*keys, *value_columns]
            for alias in aliases
        ]
        header_row, header_map = self._find_header_row(sheet_root, header_aliases)
        header_row_idx = int(header_row.attrib.get("r", "1"))
        key_indexes = {
            key_name: _find_header_index(header_map, aliases, sheet_name, required=required)
            for key_name, aliases, required in keys
        }
        value_indexes = {
            col_name: _find_header_index(header_map, aliases, sheet_name, required=required)
            for col_name, aliases, required in value_columns
        }

        rows = self._rows(sheet_root)
        existing_map: dict[tuple[str, ...], ET.Element] = {}
        last_row_idx = max((int(row.attrib.get("r", "0")) for row in rows), default=1)

        data_rows = [row for row in rows if int(row.attrib.get("r", "0")) > header_row_idx]

        for row in data_rows:
            key_parts: list[str] = []
            skip_row = False
            for key_name, _, _ in keys:
                col_idx = key_indexes[key_name]
                if col_idx is None:
                    skip_row = True
                    break
                cell = self._get_cell(row, col_idx)
                raw_value = _xlsx_cell_text(cell, self._shared_strings) if cell is not None else ""
                key_parts.append(normalizers[key_name](raw_value))
            if skip_row or any(part == "" for part in key_parts):
                continue
            existing_map[tuple(key_parts)] = row

        clear_columns = [idx for idx in value_indexes.values() if idx is not None]
        for row in data_rows:
            for col_idx in clear_columns:
                self._clear_column_values(row, col_idx)

        rows_written = 0
        for item in rows_to_write:
            key_tuple = tuple(normalizers[key_name](item.get(key_name, "")) for key_name, _, _ in keys)
            if any(part == "" for part in key_tuple):
                continue
            row = existing_map.get(key_tuple)
            if row is None:
                last_row_idx += 1
                row = self._get_or_create_row(sheet_root, last_row_idx)
                existing_map[key_tuple] = row
                for key_name, _, _ in keys:
                    col_idx = key_indexes[key_name]
                    if col_idx is None:
                        continue
                    self._write_value(row, last_row_idx, col_idx, item.get(key_name), force=True)

            row_idx = int(row.attrib.get("r", str(last_row_idx)))
            for col_name, _, _ in value_columns:
                col_idx = value_indexes[col_name]
                if col_idx is None:
                    continue
                self._write_value(row, row_idx, col_idx, item.get(col_name))
            rows_written += 1

        self._update_dimension(sheet_root)
        written_columns = [col_name for col_name, _, _ in value_columns if value_indexes[col_name] is not None]
        return {
            "sheet_name": sheet_name,
            "header_row": header_row_idx,
            "written_columns": written_columns,
            "rows_written": rows_written,
            "write_success": True,
        }

    def save(self) -> None:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            temp_path = Path(tmp_file.name)

        try:
            with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for name, data in self._file_map.items():
                    if name in self._sheet_cache:
                        archive.writestr(name, ET.tostring(self._sheet_cache[name], encoding="utf-8", xml_declaration=True))
                    else:
                        archive.writestr(name, data)
            temp_path.replace(self.excel_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()


def load_excel_sheet(
    excel_path: Path,
    sheet_name: str,
    header_aliases: Iterable[str] | None = None,
) -> pd.DataFrame:
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

    if not rows:
        return pd.DataFrame()

    header_row_idx = 0
    if header_aliases:
        expected_headers = {normalize_column_name(alias) for alias in header_aliases}
        best_score = -1
        for idx, row in enumerate(rows):
            normalized_row = {normalize_column_name(value) for value in row if str(value).strip()}
            score = len(normalized_row & expected_headers)
            if score > best_score:
                best_score = score
                header_row_idx = idx

    selected_rows = rows[header_row_idx:]
    max_width = max(len(row) for row in selected_rows)
    normalized_rows = [row + [''] * (max_width - len(row)) for row in selected_rows]
    header = normalized_rows[0]
    data = normalized_rows[1:]
    return pd.DataFrame(data, columns=header)


def export_excel_inputs(excel_path: Path, stores_csv: Path, competitors_csv: Path) -> None:
    if not excel_path.exists():
        raise FileNotFoundError(f"File Excel non trovato: {excel_path}")
    if excel_path.stat().st_size == 0:
        raise ValueError(f"File Excel vuoto: {excel_path}")

    stores_mapping = {
        "store_id": ["id", "codice", "codice_pv", "id_store", "id negozio", "id_negozio", "codice_store", "codice negozio"],
        "brand": ["insegna", "brand_rete"],
        "comune": ["città", "citta"],
        "provincia": ["prov"],
        "lat": ["latitude", "latitudine"],
        "lon": ["lng", "longitude", "longitudine"],
    }
    competitors_mapping = {
        "competitor_id": ["id", "codice", "codice_pv", "id_competitor", "id competitor", "codice_competitor"],
        "brand": ["insegna"],
        "comune": ["città", "citta"],
        "indirizzo": ["address"],
        "lat": ["latitude", "latitudine"],
        "lon": ["lng", "longitude", "longitudine"],
        "peso_competitor": ["peso competitor", "peso"],
        "livello_competitor": ["livello competitor", "livello"],
    }

    stores_header_aliases = [alias for aliases in stores_mapping.values() for alias in aliases] + list(stores_mapping.keys())
    competitors_header_aliases = [alias for aliases in competitors_mapping.values() for alias in aliases] + list(competitors_mapping.keys())

    stores_raw = load_excel_sheet(excel_path, "02_Negozi", header_aliases=stores_header_aliases)
    competitors_raw = load_excel_sheet(excel_path, "03_Competitor", header_aliases=competitors_header_aliases)

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


def _deduplicate_results(df: pd.DataFrame, key_columns: list[str], file_label: str) -> list[dict[str, object]]:
    ensure_columns(df, key_columns, file_label)
    working = df.copy()
    for col in key_columns:
        working[col] = working[col].map(_normalize_key)
    working = working[working[key_columns].ne("").all(axis=1)]
    if working.empty:
        return []
    deduped = working.drop_duplicates(subset=key_columns, keep="last")
    return deduped.to_dict("records")


def write_results_to_excel(
    excel_path: Path,
    store_competitor_csv: Path,
    comune_store_csv: Path,
) -> None:
    if not store_competitor_csv.exists():
        raise FileNotFoundError(f"Output non trovato: {store_competitor_csv}")
    if not comune_store_csv.exists():
        raise FileNotFoundError(f"Output non trovato: {comune_store_csv}")

    store_competitor_df = pd.read_csv(store_competitor_csv)
    comune_store_df = pd.read_csv(comune_store_csv)

    ensure_columns(
        store_competitor_df,
        ["store_id", "competitor_id", "tempo_minuti", "distanza_km"],
        str(store_competitor_csv),
    )
    ensure_columns(
        comune_store_df,
        ["comune", "store_id", "tempo_minuti", "distanza_km"],
        str(comune_store_csv),
    )

    store_competitor_rows = _deduplicate_results(
        store_competitor_df,
        ["store_id", "competitor_id"],
        str(store_competitor_csv),
    )
    comune_store_rows = _deduplicate_results(
        comune_store_df,
        ["comune", "store_id"],
        str(comune_store_csv),
    )

    for row in comune_store_rows:
        tempo = row.get("tempo_minuti")
        try:
            row["entro_20_min"] = int(float(tempo) <= 20.0) if tempo is not None and not pd.isna(tempo) else None
        except (TypeError, ValueError):
            row["entro_20_min"] = None

    print("Writing results to Excel")

    editor = XLSXWorkbookEditor(excel_path)
    print("Writing to 18_Ranking_Pro")
    store_competitor_summary = editor.upsert_rows(
        sheet_name="18_Ranking_Pro",
        keys=[
            ("store_id", ["store_id", "id_store", "id negozio", "id_negozio"], True),
            ("competitor_id", ["competitor_id", "id_competitor", "id competitor"], True),
        ],
        value_columns=[
            ("tempo_minuti", ["tempo_minuti", "tempo minuti"], True),
            ("distanza_km", ["distanza_km", "distanza km"], True),
        ],
        rows_to_write=store_competitor_rows,
        normalizers={
            "store_id": _normalize_key,
            "competitor_id": _normalize_key,
        },
    )
    print(
        "18_Ranking_Pro columns written: "
        f"{', '.join(store_competitor_summary['written_columns']) or 'none'}"
    )
    print(f"18_Ranking_Pro rows written: {store_competitor_summary['rows_written']}")
    print(f"18_Ranking_Pro write success: {store_competitor_summary['write_success']}")

    print("Writing to 19_Benchmark_Modello")
    comune_store_summary = editor.upsert_rows(
        sheet_name="19_Benchmark_Modello",
        keys=[
            ("comune", ["comune", "citta", "città"], True),
            ("store_id", ["store_id", "id_store", "id negozio", "id_negozio"], True),
        ],
        value_columns=[
            ("tempo_minuti", ["tempo_minuti", "tempo minuti"], True),
            ("distanza_km", ["distanza_km", "distanza km"], True),
            (
                "entro_20_min",
                ["entro_20_min", "isocrona_20min", "isocrone_20min", "entro20min", "flag_20_min"],
                False,
            ),
        ],
        rows_to_write=comune_store_rows,
        normalizers={
            "comune": _normalize_comune,
            "store_id": _normalize_key,
        },
    )
    print(
        "19_Benchmark_Modello columns written: "
        f"{', '.join(comune_store_summary['written_columns']) or 'none'}"
    )
    print(f"19_Benchmark_Modello rows written: {comune_store_summary['rows_written']}")
    print(f"19_Benchmark_Modello write success: {comune_store_summary['write_success']}")
    editor.save()
    print("Update completed")


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


def get_ors_api_key() -> str:
    load_dotenv()
    api_key = os.getenv("ORS_API_KEY", "").strip()
    print(f"ORS API key loaded: {'YES' if api_key else 'NO'}")
    if api_key:
        masked_key = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) > 8 else "[hidden]"
        print(f"ORS_API_KEY detected from environment: {masked_key}")
        return api_key

    raise ValueError(
        "ORS_API_KEY non è impostata. ORS API key loaded: NO. "
        "Configura la variabile d'ambiente prima di eseguire lo script. "
        "Esempio:\n"
        "  cp .env.example .env\n"
        "  # aggiorna .env con la tua chiave ORS reale\n"
        "  export ORS_API_KEY=\"la_tua_chiave_openrouteservice\"\n"
        "In alternativa, se usi il file .env, inserisci ORS_API_KEY=... in .env e riesegui il comando."
    )



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

    api_key = get_ors_api_key()
    client = ORSClient(api_key=api_key, timeout_s=args.timeout, max_retries=args.retries)

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

    write_results_to_excel(
        excel_path=Path(args.excel),
        store_competitor_csv=Path(args.output_store_competitor),
        comune_store_csv=Path(args.output_comune_store),
    )

    print("Completato.")


if __name__ == "__main__":
    main()
