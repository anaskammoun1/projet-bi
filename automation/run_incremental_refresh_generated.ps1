$ErrorActionPreference = "Stop"

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = "C:\Users\Omar Safi\Desktop\ProjetPowerBI\projet-bi\logs\incremental_refresh_$timestamp.log"

& "C:\Users\Omar Safi\Downloads\apache-hop-client-2.17.0\hop\hop-run.bat" -f "C:\Users\Omar Safi\Desktop\ProjetPowerBI\projet-bi\hop\wf_refresh_incremental.hwf" -r "local" *> $logFile

if ($LASTEXITCODE -ne 0) {
    throw "Incremental refresh failed. See log: $logFile"
}

Write-Host "Incremental refresh finished. Log: $logFile"
