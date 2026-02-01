Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process

$root = "C:\Users\Kekko\PycharmProjects\FantaPortoscuso"
$updateCmd = "Set-Location -Path `"$root`"; .\\scripts\\update-all.ps1 -SyncRose -ForceStats"
$apiCmd = "Set-Location -Path `"$root`"; python -m uvicorn apps.api.app.main:app --reload --host 0.0.0.0 --port 8001"
$webCmd = "Set-Location -Path `"$root\\apps\\web`"; npm run dev"

Start-Process PowerShell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $updateCmd
Start-Process PowerShell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $apiCmd
Start-Process PowerShell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $webCmd
