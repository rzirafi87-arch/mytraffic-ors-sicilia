# MYTRAFFIC_ORS

Script per calcolare matrici di drive-time reali in Sicilia con OpenRouteService.

## Fonte dati
- File Excel master ufficiale: `MyTraffic_MASTER.xlsx`
- Ignora qualsiasi file Excel precedente: `MyTraffic_MASTER.xlsx` è l'unica fonte di verità per le operazioni future.
- I CSV in `input/` sono input operativi derivati dal file master Excel.
- Audit repository: script, documentazione e passaggi di pipeline presenti in questo repo non fanno riferimento ad altri file Excel.

## Pipeline attiva ORS
1. `MyTraffic_MASTER.xlsx` resta la sorgente dati ufficiale.
2. `python scripts/build_ors_matrices.py` legge direttamente il file Excel master.
3. Lo script esporta automaticamente i fogli `02_Negozi` e `03_Competitor` in `input/negozi_rete.csv` e `input/competitor_sicilia.csv`.
4. Dopo l'esportazione CSV, lo script esegue il calcolo ORS e salva gli output in `output/ors_store_competitor.csv` e `output/ors_comune_store.csv`.
5. Terminato il calcolo, lo script reintegra i risultati in `MyTraffic_MASTER.xlsx` aggiornando i fogli `18_Distanze_Reali` e `19_Isochrone_20min` senza toccare le formule già presenti.

## Input operativi
- `input/negozi_rete.csv`
- `input/competitor_sicilia.csv`
- `input/comuni_sicilia.csv`

Lo script usa i **nomi colonna reali** presenti nei file:

- `negozi_rete.csv`: `store_id`, `brand`, `comune`, `provincia`, `lat`, `lon`
- `competitor_sicilia.csv`: `competitor_id`, `brand`, `comune`, `indirizzo`, `lat`, `lon`, `peso_competitor`, `livello_competitor`
- `comuni_sicilia.csv`: `comune`, `provincia`, `popolazione`, `lat`, `lon`

Mapping atteso dal file Excel master:
- Foglio `02_Negozi` → `store_id`, `brand`, `comune`, `provincia`, `lat`, `lon`
- Foglio `03_Competitor` → `competitor_id`, `brand`, `comune`, `indirizzo`, `lat`, `lon`, `peso_competitor`, `livello_competitor`

## Output
- `output/ors_store_competitor.csv`
  - `store_id`, `competitor_id`, `tempo_minuti`, `distanza_km`
- `output/ors_comune_store.csv`
  - `comune`, `store_id`, `tempo_minuti`, `distanza_km`
- `MyTraffic_MASTER.xlsx`
  - Foglio `18_Distanze_Reali`: aggiorna le colonne `tempo_minuti` e `distanza_km` abbinate a `store_id` + `competitor_id`
  - Foglio `19_Isochrone_20min`: aggiorna `tempo_minuti`, `distanza_km` e, se presente, una colonna flag `<=20 min` abbinate a `comune` + `store_id`

## Requisiti
```bash
pip install -r requirements.txt
```

## Esecuzione
Imposta la chiave ORS:

```bash
export ORS_API_KEY="la_tua_chiave"
```

Esegui la pipeline completa end-to-end:

```bash
python scripts/build_ors_matrices.py --excel MyTraffic_MASTER.xlsx
```

Logging atteso nelle prime fasi:
- `Excel loaded`
- `CSV generated`
- `ORS processing started`
- `Writing results to Excel`
- `Update completed`

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
- Aggiornamento Excel con logica overwrite: pulisce i valori precedenti nelle colonne ORS e riscrive i risultati senza duplicare righe.
- Preserva le formule esistenti: le celle formula non vengono sovrascritte.
