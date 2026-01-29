@echo off
set ROOT=%~dp0
set FORCE_STATS=
set IMPORT_DB=
set NO_REPORT=--no-report
set RUN_UPDATE=
set RUN_STATS=
if "%1"=="--force-stats" set FORCE_STATS=-Force
if "%1"=="--import-db" set IMPORT_DB=1
if "%2"=="--import-db" set IMPORT_DB=1
if "%1"=="--force-all" (
  set FORCE_STATS=-Force
  set IMPORT_DB=1
  set RUN_UPDATE=1
  set RUN_STATS=1
)
if "%2"=="--force-all" (
  set FORCE_STATS=-Force
  set IMPORT_DB=1
  set RUN_UPDATE=1
  set RUN_STATS=1
)
if "%1"=="--update" set RUN_UPDATE=1
if "%2"=="--update" set RUN_UPDATE=1
if "%1"=="--stats" set RUN_STATS=1
if "%2"=="--stats" set RUN_STATS=1

if "%RUN_UPDATE%"=="1" start "FantaPortoscuso Update" powershell -NoExit -ExecutionPolicy Bypass -Command "Set-Location -Path '%ROOT%'; python .\scripts\update_quotazioni_mantra.py; python .\scripts\process_player_stats_raw.py %NO_REPORT%; python .\scripts\update_data.py --auto --sync-rose"
if "%RUN_STATS%"=="1" start "FantaPortoscuso Stats" powershell -NoExit -ExecutionPolicy Bypass -Command "Set-Location -Path '%ROOT%'; .\scripts\clean-stats-all.ps1 %FORCE_STATS%"
if "%IMPORT_DB%"=="1" start "FantaPortoscuso DB Import" powershell -NoExit -ExecutionPolicy Bypass -Command "Set-Location -Path '%ROOT%'; python .\scripts\import_db.py"
start "FantaPortoscuso API" powershell -NoExit -ExecutionPolicy Bypass -Command "Set-Location -Path '%ROOT%'; python -m uvicorn apps.api.app.main:app --reload --host 0.0.0.0 --port 8000"
start "FantaPortoscuso API 8001" powershell -NoExit -ExecutionPolicy Bypass -Command "Set-Location -Path '%ROOT%'; python -m uvicorn apps.api.app.main:app --reload --host 0.0.0.0 --port 8001"
start "FantaPortoscuso Web" powershell -NoExit -ExecutionPolicy Bypass -Command "Set-Location -Path '%ROOT%apps\\web'; npm run dev"
