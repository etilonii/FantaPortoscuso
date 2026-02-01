param(
  [string]$DateStamp,
  [switch]$ForceStats,
  [int]$Keep = 5,
  [switch]$SyncRose
)

$ErrorActionPreference = "Stop"
$root = "C:\Users\Kekko\PycharmProjects\FantaPortoscuso"

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

Write-Host "==> Fatto."
