param(
  [string]$DateStamp,
  [switch]$ForceStats,
  [int]$Keep = 5,
  [switch]$SyncRose
)

$ErrorActionPreference = "Stop"
$root = "C:\Users\Kekko\PycharmProjects\FantaPortoscuso"
$reportDir = "$root\data\reports"
$logPath = "$reportDir\update_all_log.txt"
if (-not (Test-Path $reportDir)) {
  New-Item -ItemType Directory -Force -Path $reportDir | Out-Null
}
$startStamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
Add-Content -Path $logPath -Value "$startStamp | update-all start"

if (-not $DateStamp) {
  $DateStamp = (Get-Date).ToString("yyyy-MM-dd")
}

Write-Host "==> Aggiornamento ROSE/QUOTAZIONI ($DateStamp)"
$updateArgs = @("--auto", "--date", $DateStamp, "--keep", "$Keep")
if ($SyncRose) {
  $updateArgs += "--sync-rose"
}
python "$root\scripts\update_data.py" @updateArgs

Write-Host "==> Aggiornamento STATISTICHE"
$statsArgs = @()
if ($ForceStats) {
  $statsArgs += "--force"
}
python "$root\scripts\clean_stats_batch.py" @statsArgs

$endStamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
Add-Content -Path $logPath -Value "$endStamp | update-all done"
Write-Host "==> Fatto."
