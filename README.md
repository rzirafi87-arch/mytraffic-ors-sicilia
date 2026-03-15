# MYTRAFFIC_ORS

Obiettivo:
Calcolare matrici di drive-time reali in Sicilia con OpenRouteService.

## Input
- input/negozi_rete.csv
- input/competitor_sicilia.csv
- input/comuni_sicilia.csv

## Output attesi
- output/ors_store_competitor.csv
- output/ors_comune_store.csv

## Vincoli
- max 2000 richieste per batch
- salvataggio progressivo
- uso coordinate lat/lon
- gestione errori, timeout e retry
- ripresa da file output già esistenti

## Note sui campi
negozi_rete.csv
- store_id
- brand
- comune
- provincia
- lat
- lon

competitor_sicilia.csv
- competitor_id
- brand
- comune
- indirizzo
- lat
- lon
- peso_competitor
- livello_competitor

comuni_sicilia.csv
- comune
- provincia
- popolazione
- lat
- lon
