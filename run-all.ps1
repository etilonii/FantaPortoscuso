param(
  [switch]$ForceStats,
  [switch]$ForceAll,
  [switch]$ImportDb
)
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process

$root = "C:\Users\Kekko\PycharmProjects\FantaPortoscuso"
$noReport = "--no-report"
$doImport = $ImportDb -or $ForceAll
$doForce = $ForceStats -or $ForceAll
$cleanStatsCmd = if ($ForceStats) {
  "Set-Location -Path `"$root`"; .\\scripts\\clean-stats-all.ps1 -Force"
} else {
  "Set-Location -Path `"$root`"; .\\scripts\\clean-stats-all.ps1"
}
$importDbCmd = "Set-Location -Path `"$root`"; python .\\scripts\\import_db.py"
$cleanStatsCmd = if ($doForce) {
  "Set-Location -Path `"$root`"; .\\scripts\\clean-stats-all.ps1 -Force"
} else {
  "Set-Location -Path `"$root`"; .\\scripts\\clean-stats-all.ps1"
}
$updateCmd = "Set-Location -Path `"$root`"; python .\\scripts\\update_quotazioni_mantra.py; python .\\scripts\\process_player_stats_raw.py $noReport; python .\\scripts\\update_data.py --auto --sync-rose"
$apiCmd = "Set-Location -Path `"$root`"; python -m uvicorn apps.api.app.main:app --reload --host 0.0.0.0 --port 8000"
$webCmd = "Set-Location -Path `"$root\\apps\\web`"; npm run dev"

Start-Process PowerShell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $updateCmd
Start-Process PowerShell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $cleanStatsCmd
if ($doImport) {
  Start-Process PowerShell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $importDbCmd
}
Start-Process PowerShell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $apiCmd
Start-Process PowerShell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $webCmd
