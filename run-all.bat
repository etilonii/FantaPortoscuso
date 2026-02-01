@echo off
set ROOT=%~dp0

start "FantaPortoscuso Update" powershell -NoExit -ExecutionPolicy Bypass -Command "Set-ExecutionPolicy Bypass -Scope Process -Force; Set-Location -Path '%ROOT%'; .\scripts\update-all.ps1 -SyncRose -ForceStats"
start "FantaPortoscuso API" powershell -NoExit -ExecutionPolicy Bypass -Command "Set-ExecutionPolicy Bypass -Scope Process -Force; Set-Location -Path '%ROOT%'; python -m uvicorn apps.api.app.main:app --reload --host 0.0.0.0 --port 8001"
start "FantaPortoscuso Web" powershell -NoExit -ExecutionPolicy Bypass -Command "Set-ExecutionPolicy Bypass -Scope Process -Force; Set-Location -Path '%ROOT%apps\\web'; $env:VITE_API_BASE='http://localhost:8001'; npm run dev"
