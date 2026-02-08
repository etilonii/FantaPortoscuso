param(
  [string]$DateStamp,
  [switch]$ForceStats,
  [int]$Keep = 5,
  [switch]$SyncRose,
  [switch]$UsePipelineV2
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

$updateId = (Get-Date).ToString("yyyyMMdd_HHmmss_fff")
$steps = @{
  rose = "pending"
  stats = "pending"
  strength = "pending"
}

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
    update_id = $updateId
    steps = $steps
  }
  $tmpStatusPath = "$statusPath.tmp"
  try {
    $statusObj | ConvertTo-Json -Depth 4 | Set-Content -Path $tmpStatusPath -Encoding UTF8
    $moved = $false
    for ($attempt = 1; $attempt -le 3; $attempt++) {
      try {
        Move-Item -Path $tmpStatusPath -Destination $statusPath -Force -ErrorAction Stop
        $moved = $true
        break
      }
      catch {
        if ($attempt -lt 3) {
          Start-Sleep -Milliseconds 300
        } else {
          throw
        }
      }
    }
    if (-not $moved) {
      throw "status move failed after retries"
    }
  }
  catch {
    Write-UpdateLog "status write failed | result=$result | error=$($_.Exception.Message)"
    if (Test-Path $tmpStatusPath) {
      try {
        Remove-Item -Path $tmpStatusPath -Force -ErrorAction SilentlyContinue
      }
      catch {}
    }
    throw
  }
}

function Get-StepRunningMessage([string]$stepName) {
  switch ($stepName) {
    "rose" { return "Aggiornamento in corso: Rose/Quotazioni..." }
    "stats" { return "Aggiornamento in corso: Statistiche..." }
    "strength" { return "Aggiornamento in corso: Classifiche Forza..." }
    default { return "Aggiornamento in corso..." }
  }
}

function Get-StepErrorMessage([string]$stepLabel) {
  return "Errore durante: $stepLabel - sto terminando..."
}

# Scrive subito uno stato iniziale visibile prima dei backup e dei task python.
Write-DataStatus -result "running" -message "Aggiornamento in corso..."

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
  if ($UsePipelineV2) {
    Write-Host "==> Aggiornamento CLASSIFICA/ROSE/QUOTAZIONI (Pipeline V2, $DateStamp)"
  } else {
    Write-Host "==> Aggiornamento ROSE/QUOTAZIONI ($DateStamp)"
  }
  $updateArgs = @("--auto", "--date", $DateStamp, "--keep", "$Keep")
  if ($SyncRose) {
    $updateArgs += "--sync-rose"
  }
  $steps.rose = "running"
  Write-DataStatus -result "running" -message (Get-StepRunningMessage -stepName "rose")
  try {
    if ($UsePipelineV2) {
      python "$root\scripts\pipeline_v2.py" --domains "classifica,rose,quotazioni" --date "$DateStamp"
      if ($SyncRose) {
        python "$root\scripts\update_data.py" --date "$DateStamp" --keep "$Keep" --sync-rose
      }
    }
    else {
      python "$root\scripts\update_data.py" @updateArgs
    }
    $steps.rose = "ok"
    Write-DataStatus -result "running" -message (Get-StepRunningMessage -stepName "stats")
  }
  catch {
    $steps.rose = "error"
    Write-DataStatus -result "running" -message (Get-StepErrorMessage -stepLabel "Rose/Quotazioni")
    throw
  }

  Write-Host "==> Aggiornamento STATISTICHE"
  $statsArgs = @()
  if ($ForceStats) {
    $statsArgs += "--force"
  }
  $steps.stats = "running"
  Write-DataStatus -result "running" -message (Get-StepRunningMessage -stepName "stats")
  try {
    python "$root\scripts\clean_stats_batch.py" @statsArgs
    $steps.stats = "ok"
    Write-DataStatus -result "running" -message (Get-StepRunningMessage -stepName "strength")
  }
  catch {
    $steps.stats = "error"
    Write-DataStatus -result "running" -message (Get-StepErrorMessage -stepLabel "Statistiche")
    throw
  }

  Write-Host "==> Aggiornamento CLASSIFICHE FORZA"
  $steps.strength = "running"
  Write-DataStatus -result "running" -message (Get-StepRunningMessage -stepName "strength")
  try {
    python "$root\scripts\build_team_strength_ranking.py" --snapshot --snapshot-date "$DateStamp"
    $steps.strength = "ok"
    Write-DataStatus -result "running" -message "Aggiornamento in corso: Finalizzazione..."
  }
  catch {
    $steps.strength = "error"
    Write-DataStatus -result "running" -message (Get-StepErrorMessage -stepLabel "Classifiche Forza")
    throw
  }

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
