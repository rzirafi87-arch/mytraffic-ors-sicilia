# MYTRAFFIC_ORS

Obiettivo:
Calcolare matrici di drive-time reali in Sicilia con OpenRouteService.

## Input
Lo script cerca prima nella cartella `input/` e, in fallback, nella root del repository:
- `input/negozi_rete.csv` (oppure `negozi_rete.csv`)
- `input/competitor_sicilia.csv` (oppure `competitor_sicilia.csv`)
- `input/comuni_sicilia.csv` (oppure `comuni_sicilia.csv`)

## Output attesi
- `output/ors_store_competitor.csv`
- `output/ors_comune_store.csv`

## Setup
1. Installa dipendenze:
   ```bash
   pip install -r requirements.txt
   ```
2. Configura la chiave ORS:
   ```bash
   cp .env.example .env
   ```
   Poi imposta `ORS_API_KEY=<la_tua_chiave>` nel file `.env`.

## Esecuzione
### Matrice store -> competitor
```bash
python scripts/run_store_competitor.py
```

### Matrice comune -> store
```bash
python scripts/run_comune_store.py
```

### Esecuzione via script unico
```bash
python scripts/build_ors_matrices.py store_competitor
python scripts/build_ors_matrices.py comune_store
```

## Funzionalità implementate
- Calcolo tempi/distanze reali con endpoint matrix di OpenRouteService.
- Limite batch rispettato: massimo **2000 coppie origine-destinazione** per richiesta.
- Gestione errori API e retry automatico con backoff esponenziale.
- Salvataggio progressivo ogni 50 righe.
- Ripresa automatica da output esistente (skip delle coppie già calcolate).
- Stampa del progresso durante l'elaborazione.

## Formato output
### `output/ors_store_competitor.csv`
- `store_id`
- `competitor_id`
- `tempo_minuti`
- `distanza_km`

### `output/ors_comune_store.csv`
- `comune`
- `store_id`
- `tempo_minuti`
- `distanza_km`
