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

Per fissare/aggiornare la giornata corrente nello stato dati:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\update-all.ps1 -UsePipelineV2 -Matchday 24
```

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
- `data/reports/formazioni_giornata.csv` -> formazioni reali per giornata (opzionale)
- `data/raw/` -> sorgenti grezze non pronte
- `data/templates/` -> template di import
- `data/db/` -> DB/CSV master
- `data/tmp/` -> temporanei

## Formazioni reali (giornata)
- Template: `data/templates/formazioni_giornata_template.csv`
- File letti dall'API (ordine priorita'):
  - ultimo file in `data/incoming/formazioni/` (`.csv/.xlsx/.xls`)
  - `data/reports/formazioni_giornata.csv|xlsx`
  - `data/reports/formazioni_reali.csv|xlsx`
  - `data/db/formazioni_giornata.csv`
- Colonne attese: `giornata, team, modulo, portiere, difensori, centrocampisti, attaccanti`
- Separatore giocatori per reparto: `;`
- Se il file non c'e' (o manca la giornata), la sezione Formazioni mostra fallback "miglior XI" ordinato per classifica.

## Fixtures e giornata corrente
- Script: `python .\scripts\update_fixtures.py`
- Sorgente predefinita:
  - ultimo file in `data/incoming/fixtures/` (`.csv/.xlsx/.xls`)
  - fallback `data/templates/fixtures_rounds_template.csv`
- Output: `data/db/fixtures.csv` con colonne:
  - `round, team, opponent, home_away`
  - opzionali risultati: `home_score, away_score, team_score, opponent_score`
- Template con risultati: `data/templates/fixtures_results_template.csv`
- La giornata corrente e' risolta in questo ordine:
  1) `status.json.matchday` (se presente)
  2) inferenza da risultati in `data/db/fixtures.csv`
  3) fallback da `data/stats/partite.csv`

## Regolamento (scoring live)
- File unico: `data/config/regolamento.json`
- Contiene:
  - valori bonus/malus (es. gol `+3`, assist `+1`, ammonizione `-0.5`)
  - configurazione `6 politico`
  - moduli modificatori (difesa, capitano)
- Usato per il calcolo da `Live` -> `Formazioni` (voto/fantavoto e totale live).

## Regole semplici
- Non modificare a mano i file in `data/history/`
- Se un file e' "current", non duplicarlo in root
- Gli aggiornamenti passano sempre da `incoming` -> script -> `history`

## Regole asterisco (*)
- L'asterisco indica giocatore uscito dal listone.
- Nei placeholder mercato, "Nome" e "Nome *" sono trattati come lo stesso giocatore.
- Il ruolo/squadra si risolve prima dalle rose, poi dalle quotazioni correnti,
  e infine dalle quotazioni storiche se serve.

## Sync automatico Leghe (Railway / server)
Obiettivo: scaricare gli XLSX da Leghe Fantacalcio e aggiornare i CSV in `data/` senza intervento manuale.

Nota formazioni:
- la fonte primaria aggiornata dal sync e' `data/tmp/formazioni_page.html` (payload appkey nell'HTML)
- l'export `formazioni.xlsx` viene tentato come supporto, ma non e' bloccante

### Variabili env (backend)
- `LEGHE_ALIAS`, `LEGHE_USERNAME`, `LEGHE_PASSWORD` (obbligatorie)
- opzionali: `LEGHE_COMPETITION_ID`, `LEGHE_COMPETITION_NAME`, `LEGHE_FORMATIONS_MATCHDAY`

### Scheduler in-app
- `AUTO_LEGHE_SYNC_ENABLED=1`
- `AUTO_LEGHE_SYNC_INTERVAL_HOURS=12`
- `AUTO_LEGHE_SYNC_ON_START=1` (opzionale)

### Trigger manuale (admin)
Endpoint:
- `POST /data/admin/leghe/sync` (richiede `X-Admin-Key` o bearer admin)
- query: `force=1` per bypassare l'intervallo, `formations_matchday=NN` per forzare la giornata
