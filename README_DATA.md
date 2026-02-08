# Gestione Dati (FantaPortoscuso)

## Flusso ufficiale
1) Inserisci i nuovi file in `data/incoming/`
2) Esegui lo script di pulizia/aggiornamento
3) Lo script:
   - aggiorna i "current" in root `data/`
   - salva una copia datata in `data/history/`
   - aggiorna i report in `data/reports/` (se previsti)

## Pipeline v2 (staging DB + CSV madre)
Obiettivo: avere un flusso ordinato per dominio (classifica, rose, quotazioni, stats),
con import da `incoming`, normalizzazione, scrittura su SQLite e export CSV dal DB.

### Cartelle per dominio
- `data/incoming/<dominio>/` -> sorgenti grezze (`csv/xlsx/xls`)
- `data/staging/<dominio>/` -> CSV normalizzati datati (runtime)
- `data/marts/<dominio>/` -> CSV madre esportati dal DB (runtime)
- `data/db/pipeline_v2.db` -> SQLite staging/marts (runtime)
- `data/db/pipeline_v2_state.json` -> metadati run (runtime)

### Comando pipeline v2
```powershell
python .\scripts\pipeline_v2.py
```

Opzioni utili:
- `--domains classifica,rose,quotazioni,stats` (default: tutti)
- `--prefer-current` (usa prima i file correnti in `data/`)
- `--no-legacy-write` (non sovrascrive i CSV legacy in root `data/`)
- `--source-<dominio> <path>` per forzare un input specifico

### Integrazione con update-all
Lo script `scripts/update-all.ps1` supporta ora:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\update-all.ps1 -UsePipelineV2 -ForceStats
```
Con `-UsePipelineV2` il passo rose/quotazioni usa `pipeline_v2.py` (classifica+rose+quotazioni),
mentre stats/report restano gestiti dal flusso esistente.

## File "current" (sempre in `data/`)
- `quotazioni.csv`
- `rose_fantaportoscuso.csv`
- `statistiche_giocatori.csv`
- `infortunati_clean.txt`, `infortunati_whitelist.txt`, `infortunati_weights.csv`
- `nuovi_arrivi.txt`, `nuovi_arrivi_weights.csv`

## Cartelle
- `data/incoming/` -> solo file nuovi grezzi in attesa di pulizia
- `data/history/` -> copie datate dopo ogni update
- `data/reports/` -> report automatici
- `data/raw/` -> sorgenti grezze non pronte
- `data/templates/` -> template di import
- `data/db/` -> DB/CSV master
- `data/tmp/` -> temporanei

## Regole semplici
- Non modificare a mano i file in `data/history/`
- Se un file e' "current", non duplicarlo in root
- Gli aggiornamenti passano sempre da `incoming` -> script -> `history`

## Regole asterisco (*)
- L'asterisco indica giocatore uscito dal listone.
- Nei placeholder mercato, "Nome" e "Nome *" sono trattati come lo stesso giocatore.
- Il ruolo/squadra si risolve prima dalle rose, poi dalle quotazioni correnti,
  e infine dalle quotazioni storiche se serve.
