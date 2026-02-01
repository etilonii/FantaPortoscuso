# Gestione Dati (FantaPortoscuso)

## Flusso ufficiale
1) Inserisci i nuovi file in `data/incoming/`
2) Esegui lo script di pulizia/aggiornamento
3) Lo script:
   - aggiorna i **current** in root `data/`
   - salva una copia **datata** in `data/history/`
   - aggiorna i report in `data/reports/` (se previsti)

## File “current” (sempre in `data/`)
- `quotazioni.csv`
- `rose_fantaportoscuso.csv`
- `statistiche_giocatori.csv`
- `infortunati_clean.txt`, `infortunati_whitelist.txt`, `infortunati_weights.csv`
- `nuovi_arrivi.txt`, `nuovi_arrivi_weights.csv`

## Cartelle
- `data/incoming/` → solo file nuovi grezzi in attesa di pulizia
- `data/history/` → copie datate dopo ogni update
- `data/reports/` → report automatici
- `data/raw/` → sorgenti grezze non pronte
- `data/templates/` → template di import
- `data/db/` → DB/CSV master
- `data/tmp/` → temporanei

## Regole semplici
- Non modificare a mano i file in `data/history/`
- Se un file è “current”, non duplicarlo in root
- Gli aggiornamenti passano sempre da `incoming` → script → `history`
