# Project Audit

## Scope and method
This audit covers the repository contents present in the project root at the time of analysis.

Checks performed:
- full file inventory from the repository root
- code/reference search to see which datasets are actually referenced
- dataset comparison between top-level CSVs and `input/` CSVs
- structural inspection of `My Traffic.xlsx` to understand whether it acts as the main working workbook
- script review to determine which script is final vs obsolete/experimental

---

## 1. Full repository structure

### Root level
- `.env.example`
- `.gitignore`
- `My Traffic.xlsx`
- `PROMPT_CODEX.txt`
- `README.md`
- `competitor_sicilia.csv`
- `comuni_sicilia.csv`
- `docs/`
- `input/`
- `negozi_rete.csv`
- `requirements.txt`
- `scripts/`

### `docs/`
- `PROJECT_AUDIT.md`

### `input/`
- `input/competitor_sicilia.csv`
- `input/comuni_sicilia.csv`
- `input/negozi_rete.csv`

### `scripts/`
- `scripts/build_ors_matrices.py`

### Git metadata
- `.git/` exists as repository metadata and was not audited as business/project content.

---

## 2. Classification of files and folders

| Path | Type | Classification | Notes |
|---|---|---|---|
| `.env.example` | file | INPUT / CONFIG TEMPLATE | Environment variable template; likely intended for `ORS_API_KEY`. |
| `.gitignore` | file | SUPPORT | Standard repo hygiene, not part of business pipeline. |
| `My Traffic.xlsx` | file | INPUT + MAIN WORKING FILE | Central analytical workbook with source, matrix, ranking, dashboard, and simulation sheets. |
| `PROMPT_CODEX.txt` | file | UNUSED / HISTORICAL SPEC | Build brief used to generate the script; not executed by the application. |
| `README.md` | file | DOCUMENTATION | Usage instructions for the script and expected inputs/outputs. |
| `competitor_sicilia.csv` | file | UNUSED / REDUNDANT | Exact duplicate of `input/competitor_sicilia.csv`; not used by the script by default. |
| `comuni_sicilia.csv` | file | UNUSED / REDUNDANT | Exact duplicate of `input/comuni_sicilia.csv`; not used by the script by default. |
| `docs/` | folder | DOCUMENTATION | Documentation folder. |
| `input/` | folder | INPUT | Canonical input dataset folder referenced by the script and README. |
| `input/competitor_sicilia.csv` | file | INPUT | Active competitor dataset used by the script. |
| `input/comuni_sicilia.csv` | file | INPUT | Active comuni dataset used by the script. |
| `input/negozi_rete.csv` | file | INPUT | Active store/network dataset used by the script. |
| `negozi_rete.csv` | file | UNUSED / REDUNDANT | Exact duplicate of `input/negozi_rete.csv`; not used by the script by default. |
| `requirements.txt` | file | SUPPORT / SCRIPT DEPENDENCIES | Python dependencies for the script runtime. |
| `scripts/` | folder | SCRIPTS | Contains the only executable project script. |
| `scripts/build_ors_matrices.py` | file | SCRIPTS / FINAL | Main and only production script in the repository. |

### Expected output files (documented, but not present in repo)
These files are part of the designed pipeline output, but they are **not currently versioned in the repository**:
- `output/ors_store_competitor.csv`
- `output/ors_comune_store.csv`

---

## 3. Verification requested

### 3.1 Which CSV files are actually used by scripts

Only one script exists: `scripts/build_ors_matrices.py`.

Its default CLI arguments point to these three CSV files:
- `input/negozi_rete.csv`
- `input/competitor_sicilia.csv`
- `input/comuni_sicilia.csv`

It also writes to these output paths by default:
- `output/ors_store_competitor.csv`
- `output/ors_comune_store.csv`

#### Evidence from code and docs
- The script defaults use the `input/` versions, not the top-level duplicates.
- `README.md` documents the same `input/` files as the intended inputs.
- `PROMPT_CODEX.txt` also specifies the `input/` files.

#### Conclusion
The CSV files **actually used by the script in normal/default execution** are:
1. `input/negozi_rete.csv`
2. `input/competitor_sicilia.csv`
3. `input/comuni_sicilia.csv`

The three CSV files at repository root are **not referenced by default anywhere in the runnable code** and appear to be convenience copies or leftovers.

---

### 3.2 Which Excel file is the main working file

The main working Excel file is:
- `My Traffic.xlsx`

#### Why it is the main working file
It contains 14 structured business sheets, including:
- setup/configuration (`01_Impostazioni`)
- source master data (`02_Negozi`, `03_Competitor`, `04_Comuni`)
- matrix/model sheets (`05_Comune_Rete`, `06_Store_Competitor`)
- downstream analytical outputs (`07_Traffico_Store`, `08_Ranking_Comuni`, `09_Shortlist`, `10_Dashboard`, `11_Bacino_Popolazione`, `12_White_Space`, `13_Pressione_Provincia`, `14_Simulatore_Apertura`)

This layout strongly indicates that the workbook is the real business analysis environment, while the Python code is a supporting extraction/generation utility for ORS matrices.

#### Important architectural note
The Python script does **not** currently read from or write back to `My Traffic.xlsx`.
The workbook and the script coexist in the same repository, but the script operates only on CSV files and emits CSV outputs.

---

### 3.3 Duplicated or inconsistent datasets

#### Exact duplicates found
The following root-level files are exact duplicates of the `input/` datasets:
- `competitor_sicilia.csv` == `input/competitor_sicilia.csv`
- `comuni_sicilia.csv` == `input/comuni_sicilia.csv`
- `negozi_rete.csv` == `input/negozi_rete.csv`

These duplicate pairs match in:
- file size
- file hash
- row count
- column structure
- full DataFrame equality check

#### Inconsistency assessment
No inconsistency was found **between the duplicated CSV pairs**. They are identical.

#### Potential workflow inconsistency risk
There **is** a repository-design inconsistency:
- canonical inputs are in `input/`
- identical copies also exist at repo root

This creates ambiguity for future users because they may edit one copy and not the other. At the moment, the script clearly prefers `input/`, so the root-level copies should be treated as redundant and potentially risky if they drift later.

#### Workbook vs CSV consistency
The workbook appears to contain richer, business-facing versions of the same entities (`Negozi`, `Competitor`, `Comuni`) plus many derived sheets. However:
- column names differ from the CSV schema in several sheets
- workbook sheets contain many extra fields not present in CSVs
- workbook matrix sheets contain derived measures, proxy times, legacy references, and analytical flags

Therefore:
- the workbook is **not** a strict file-for-file equivalent of the CSV pipeline
- it is better understood as the master analytical model, not a clean duplicate of the script inputs/outputs

---

## 4. Script audit

### 4.1 Which script is the correct final one

The correct final script is:
- `scripts/build_ors_matrices.py`

#### Why
It is the only executable script in the repository and it matches the documented objective exactly:
- reads the three input CSVs
- validates required columns
- computes `store -> competitor`
- computes `comune -> store`
- calls OpenRouteService Matrix API
- supports retry/backoff behavior
- appends incremental CSV output
- resumes from existing outputs
- supports test limiting with `--limit-pairs`

This is also the script referenced by:
- `README.md`
- `PROMPT_CODEX.txt`

---

### 4.2 Which scripts are obsolete or experimental

No obsolete or experimental Python scripts are present in the repository.

However, there are two items that are **not runtime scripts** and may represent historical implementation scaffolding:
- `PROMPT_CODEX.txt`: historical generation brief/specification
- duplicated root-level CSVs: likely pre-organization leftovers or convenience exports

So the script situation is clean:
- **final/active**: `scripts/build_ors_matrices.py`
- **obsolete scripts**: none found
- **experimental scripts**: none found

---

## 5. Practical conclusions

### Canonical inputs
Use only:
- `input/negozi_rete.csv`
- `input/competitor_sicilia.csv`
- `input/comuni_sicilia.csv`

### Canonical script
Use only:
- `scripts/build_ors_matrices.py`

### Canonical business workbook
Treat as main analysis file:
- `My Traffic.xlsx`

### Redundant items
Candidates for later cleanup (not modified in this audit):
- `competitor_sicilia.csv`
- `comuni_sicilia.csv`
- `negozi_rete.csv`
- possibly `PROMPT_CODEX.txt` if no longer needed for project traceability

### Missing/generated outputs
Expected generated outputs are documented but not committed:
- `output/ors_store_competitor.csv`
- `output/ors_comune_store.csv`

---

## 6. Recommended next cleanup steps (not executed in this audit)

1. Decide whether `input/` is the single source of truth for CSVs.
2. If yes, remove the three redundant root-level CSV copies.
3. Decide whether `PROMPT_CODEX.txt` should be archived, kept for traceability, or removed.
4. Decide whether the ORS output CSVs should remain generated artifacts only, or whether sample outputs should be stored under `output/` or `examples/`.
5. If the workbook is the main operational artifact, document how ORS CSV outputs are imported into `My Traffic.xlsx`, because that linkage is currently implied but not automated in code.

---

## 7. Bottom line

- The repository is structurally small and clean.
- The active automation layer consists of exactly **one** script.
- The real canonical CSV inputs are the files inside `input/`.
- The top-level CSVs are exact duplicates and are currently redundant.
- `My Traffic.xlsx` is the main working business file.
- No obsolete scripts were found; only redundant data copies and a historical prompt/spec file were identified.
