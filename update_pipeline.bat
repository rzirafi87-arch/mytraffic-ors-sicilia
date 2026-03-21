@echo off
REM Pipeline automatica MyTraffic ORS - Esegui come doppio click

REM 1. Normalizza brand competitor
python scripts\fix_brand_competitor.py

REM 2. Pulisci e importa output ORS in Excel
python scripts\patch_workbook_from_ors.py

REM 3. Aggiorna formule e compatibilità
python scripts\fix_formulas_compat.py

REM 4. Aggiorna fogli presentazione e check brand
python scripts\auto_presentation_and_brand_check.py

REM 5. (Opzionale) Popola fogli riepilogo competitor
python scripts\populate_competitor_sheets.py

REM 6. (Opzionale) Altri step custom
REM python scripts\load_ors_into_workbook.py

ECHO Pipeline completata. Premi un tasto per chiudere.
pause >nul
