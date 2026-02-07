param(
  [string]$DateStamp,
  [switch]$ForceStats,
  [int]$Keep = 5,
  [switch]$SyncRose
)

$ErrorActionPreference = "Stop"
$scriptDir = if ($PSScriptRoot) {
  $PSScriptRoot
} else {
  Split-Path -Parent $MyInvocation.MyCommand.Path
}
$root = Split-Path -Parent $scriptDir
$dataDir = "$root\data"
$reportDir = "$root\data\reports"
$logPath = "$reportDir\update_all_log.txt"
$statusPath = "$root\data\status.json"
$backupDir = "$root\data\backups"
if (-not (Test-Path $dataDir)) {
  New-Item -ItemType Directory -Force -Path $dataDir | Out-Null
}
if (-not (Test-Path $reportDir)) {
  New-Item -ItemType Directory -Force -Path $reportDir | Out-Null
}
function Write-UpdateLog([string]$message) {
  $stamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Add-Content -Path $logPath -Value "$stamp | $message"
}

Write-UpdateLog "update-all start"

if (-not $DateStamp) {
  $DateStamp = (Get-Date).ToString("yyyy-MM-dd")
}

function Get-SeasonFromDate([datetime]$dt) {
  if ($dt.Month -ge 7) {
    $startYear = $dt.Year
    $endShort = ($dt.Year + 1).ToString().Substring(2, 2)
    return "$startYear-$endShort"
  }
  $prevYear = $dt.Year - 1
  $currShort = $dt.Year.ToString().Substring(2, 2)
  return "$prevYear-$currShort"
}

function Write-DataStatus([string]$result, [string]$message) {
  $now = Get-Date
  $season = Get-SeasonFromDate -dt $now
  $statusObj = @{
    last_update = $now.ToString("o")
    result = $result
    message = $message
    season = $season
  }
  $statusObj | ConvertTo-Json -Depth 4 | Set-Content -Path $statusPath -Encoding UTF8
}

$dbCandidates = @(
  "$root\app.db",
  "$root\data\app.db",
  "$root\apps\api\app.db"
)

try {
  if (-not (Test-Path $backupDir)) {
    New-Item -ItemType Directory -Force -Path $backupDir | Out-Null
  }

  $dbPath = $null
  foreach ($candidate in $dbCandidates) {
    if (Test-Path $candidate) {
      $dbPath = $candidate
      break
    }
  }

  $dbForLog = if ($dbPath) { $dbPath } else { "not_found" }
  Write-UpdateLog "backup start | db=$dbForLog"

  $backupStamp = (Get-Date).ToString("yyyyMMdd_HHmmss_fff")
  $thisBackup = Join-Path $backupDir $backupStamp
  New-Item -ItemType Directory -Force -Path $thisBackup | Out-Null

  if ($dbPath) {
    $copied = $false
    for ($attempt = 1; $attempt -le 3; $attempt++) {
      try {
        Copy-Item -Path $dbPath -Destination (Join-Path $thisBackup "app.db") -Force -ErrorAction Stop
        $copied = $true
        break
      }
      catch {
        if ($attempt -lt 3) {
          Start-Sleep -Seconds 1
        } else {
          Write-UpdateLog "backup db copy failed | attempts=3 | error=$($_.Exception.Message)"
        }
      }
    }
    if (-not $copied) {
      Write-UpdateLog "backup db copy failed | db=$dbPath"
    }
  } else {
    Write-UpdateLog "backup db missing | searched=$($dbCandidates -join ';')"
  }

  if (Test-Path $statusPath) {
    try {
      Copy-Item -Path $statusPath -Destination (Join-Path $thisBackup "status.json") -Force -ErrorAction Stop
    }
    catch {
      Write-UpdateLog "backup status copy failed | error=$($_.Exception.Message)"
    }
  }

  Write-UpdateLog "backup ok | dest=$thisBackup"

  $backupFolders = @(
    Get-ChildItem -Path $backupDir -Directory -ErrorAction SilentlyContinue |
      Sort-Object LastWriteTime -Descending
  )
  if ($backupFolders.Count -gt $Keep) {
    $toRemove = $backupFolders | Select-Object -Skip $Keep
    foreach ($folder in $toRemove) {
      try {
        Remove-Item -Path $folder.FullName -Recurse -Force -ErrorAction Stop
      }
      catch {
        Write-UpdateLog "backup cleanup remove failed | folder=$($folder.FullName) | error=$($_.Exception.Message)"
      }
    }
  }
  Write-UpdateLog "backup cleanup | kept=$Keep"
}
catch {
  Write-UpdateLog "backup error | message=$($_.Exception.Message)"
}

$overallMessage = "Aggiornamento completato con successo."
$isSuccess = $false

try {
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

  Write-Host "==> Aggiornamento CLASSIFICHE FORZA"
  python "$root\scripts\build_team_strength_ranking.py" --snapshot --snapshot-date "$DateStamp"

  $isSuccess = $true
  Write-DataStatus -result "ok" -message $overallMessage
}
catch {
  $overallMessage = "Errore update: $($_.Exception.Message)"
  Write-DataStatus -result "error" -message $overallMessage
  throw
}
finally {
  Write-UpdateLog "update-all done | success=$isSuccess | message=$overallMessage"
  Write-Host "==> Fatto."
}
