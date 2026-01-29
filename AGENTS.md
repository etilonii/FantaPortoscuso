## FantaPortoscuso - Agent Notes

### Overview
- Monorepo with FastAPI backend and React frontend.
- Backend serves data under `/data/*` and auth under `/auth/*`.
- Frontend lives in `apps/web` and calls the API via `API_BASE`.

### Run
- Preferred: `run-all.bat` (starts backend + frontend).
- Backend main: `apps/api/app/main.py` (FastAPI/Uvicorn) — lo strumento di analisi standalone è `scripts/tools/main.py` (non parte dell'API).

### Tests
- Py tests: `tests/` (use `pytest` oppure, se non disponibile, `python -m unittest tests/test_market_engine.py`).
- Example: `pytest -q` (richiede le dipendenze aggiornate nel `apps/api/requirements.txt`).

### Key Data Paths
- CSV data: `data/`
- Reports: `data/reports/`
- Market/rose: `data/rose_fantaportoscuso.csv`, `data/quotazioni.csv`

### Market Suggestions
- Payload endpoint: `GET /data/market/payload` with header `X-Access-Key`.
- Suggest endpoint: `POST /data/market/suggest`.
- Team access keys are stored in `TeamKey` (DB).

### Conventions
- Keep changes minimal and localized.
- Prefer `rg` for search.
