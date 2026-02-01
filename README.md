# FantaPortoscuso

Monorepo con backend FastAPI e frontend React (Vite).

## Struttura
- `apps/api`: backend FastAPI
- `apps/web`: frontend React
- `data/`: dataset e output (rose, quotazioni, stats, reports)
- `scripts/`: script di pulizia/aggiornamento

## Avvio rapido
```
run-all.bat
```
Avvia:
- aggiornamento dati (script unico)
- API su `http://0.0.0.0:8001`
- Web su `http://localhost:5173`

## Configurazione frontend
Se serve, crea il file locale:
```
apps/web/.env
```
Con il contenuto (vedi `apps/web/.env.example`):
```
VITE_API_BASE=http://localhost:8001
```

## Aggiornamento dati (un comando)
```
powershell -ExecutionPolicy Bypass -File .\scripts\update-all.ps1 -SyncRose -ForceStats
```
Usa automaticamente i file più recenti in `data/incoming`.

## Flusso dati giornaliero
Metti i file in:
- `data/incoming/rose/`
- `data/incoming/quotazioni/`
- `data/incoming/stats/`

Il task schedulato aggiorna automaticamente ogni giorno alle 01:00
e all’accesso (se il PC era spento).

Dettagli completi in `README_DATA.md`.
