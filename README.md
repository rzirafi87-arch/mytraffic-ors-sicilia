# MYTRAFFIC_ORS

Script per calcolare matrici di drive-time reali in Sicilia con OpenRouteService.

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
- `input/negozi_rete.csv`
- `input/competitor_sicilia.csv`
- `input/comuni_sicilia.csv`

Lo script usa i **nomi colonna reali** presenti nei file:

- `negozi_rete.csv`: `store_id`, `brand`, `comune`, `provincia`, `lat`, `lon`
- `competitor_sicilia.csv`: `competitor_id`, `brand`, `comune`, `indirizzo`, `lat`, `lon`, `peso_competitor`, `livello_competitor`
- `comuni_sicilia.csv`: `comune`, `provincia`, `popolazione`, `lat`, `lon`

## Output
- `output/ors_store_competitor.csv`
  - `store_id`, `competitor_id`, `tempo_minuti`, `distanza_km`
- `output/ors_comune_store.csv`
  - `comune`, `store_id`, `tempo_minuti`, `distanza_km`

## Requisiti
```bash
pip install -r requirements.txt
```

## Esecuzione
Imposta la chiave ORS:

```bash
export ORS_API_KEY="la_tua_chiave"
```

Esegui:

```bash
python scripts/build_ors_matrices.py
```

## Opzioni utili
- `--max-pairs-per-batch` (default `2000`): massimo numero di coppie origine-destinazione per singola chiamata ORS.
- `--save-every` (default `500`): salva progressivamente ogni N righe calcolate.
- `--limit-pairs` (default nessun limite): limita il numero di nuove coppie da calcolare (utile per test iniziali).
- `--timeout` e `--retries`: gestione timeout e retry automatici.

Esempio test iniziale (nessuna chiamata ORS, solo validazione input):

```bash
python scripts/build_ors_matrices.py --limit-pairs 0
```

Esempio test con piccolo batch reale:

```bash
python scripts/build_ors_matrices.py --limit-pairs 100 --max-pairs-per-batch 2000
```

## Comportamento implementato
- Batch massimo per chiamata rispettato (default 2000).
- Retry automatico su timeout/errori di rete/429/5xx.
- Salvataggio progressivo su CSV.
- Ripresa da output esistenti senza ricalcolare coppie già presenti.
