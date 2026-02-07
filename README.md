# FantaPortoscuso

Monorepo con backend FastAPI e frontend React/Vite per analisi lega, rose, mercato e statistiche.

## Stack
- Backend: FastAPI (`apps/api`)
- Frontend: React + Vite (`apps/web`)
- Data layer: CSV/XLSX in `data/` + script batch in `scripts/`

## Funzionalita principali

### 1. Accesso e permessi
- Login via key monouso con binding device.
- Sezione admin visibile solo per key `is_admin=true`.
- Gestione key/team/admin via API admin e pannello Admin.

### 2. Home
- Metriche lega (team, giocatori).
- Ricerca globale giocatori.
- Top preview da piu sezioni.

### 3. Statistiche giocatori
- Classifiche per statistica (gol, assist, ammonizioni, espulsioni, cleansheet, autogol).
- Ricerca per nome nel ranking.
- Dati da `data/stats/*.csv` e `data/statistiche_giocatori.csv`.

### 4. Rose
- Rosa per team con filtri ruolo/squadra reale e ricerca giocatore.
- Totali spesa/valore attuale dalla rosa corrente.

### 5. Plusvalenze
- Ranking team con filtro periodo (`dall'inizio` / `da dicembre`).
- Valori acquisto/attuale coerenti con `rose_fantaportoscuso.csv`.

### 6. Listone
- Listone per ruolo con ordinamenti e ricerca.
- Quotazioni da `data/quotazioni.csv`.

### 7. Giocatori piu acquistati
- Classifica aggregata per ruolo.
- Ricerca giocatore.
- Filtro range posizione classifica (`Posizione da` / `Posizione a`).
- Pulsante reset filtri + etichetta range attivo.

### 8. Mercato
- Timeline trasferimenti dall'ultimo diff rose.
- Filtri team e UI mercato con countdown.
- Ranking "piu acquistati" e "piu svincolati" nella finestra mercato.

### 9. Admin
- Stato update dati.
- Force refresh mercato.
- Operazioni key/team/admin.

### 10. Ranking forza squadra (report locali)
- `team_strength_ranking.csv`: forza totale rosa.
- `team_starting_strength_ranking.csv`: forza miglior XI possibile.
- `team_starting_xi.csv`: modulo e titolari scelti per team.
- `team_strength_players.csv`: dettaglio forza per giocatore.

## Avvio locale

### Opzione rapida
```powershell
run-all.bat
```

### Manuale
```powershell
# backend
python -m uvicorn apps.api.app.main:app --reload --host 0.0.0.0 --port 8001

# frontend
cd apps/web
npm run dev
```

## Config frontend
File consigliato: `apps/web/.env`
```env
VITE_API_BASE=http://localhost:8001
```

## Aggiornamento dati giornaliero
Metti i nuovi file in:
- `data/incoming/rose/`
- `data/incoming/quotazioni/`
- `data/incoming/stats/`
- (opzionale) `data/incoming/teams/`

Esegui:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\update-all.ps1 -SyncRose -ForceStats
```

Lo script ora fa in sequenza:
1. update rose/quotazioni
2. clean stats
3. rebuild ranking forza squadra
4. snapshot report datati in `data/reports/history/`

## Script ranking forza
```powershell
python .\scripts\build_team_strength_ranking.py
```

Opzioni:
- `--snapshot` salva copie datate in `data/reports/history/`
- `--snapshot-date YYYY-MM-DD` forza la data snapshot
- `--strict` blocca il job su errori validazione rosa

## Struttura cartelle essenziale
- `apps/api`: API FastAPI
- `apps/web`: frontend React
- `scripts`: automazioni import/clean/update/report
- `data`: dataset correnti
- `data/history`: storico datato
- `data/reports`: report correnti
- `data/reports/history`: snapshot report

## Note operative
- I file in `data/history/` non vanno modificati a mano.
- I file correnti usati dal sito sono in `data/`.
- Per dettagli data pipeline: `README_DATA.md`.
