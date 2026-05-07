param(
    [string]$HopRun = "hop-run",
    [string]$ProjectDir = "C:\Users\Omar Safi\Desktop\ProjetPowerBI\projet-bi",
    [string]$RunConfiguration = "local"
)

$ErrorActionPreference = "Stop"

$workflow = Join-Path $ProjectDir "hop\wf_refresh_incremental.hwf"
$logDir = Join-Path $ProjectDir "logs"

if (-not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $logDir "incremental_refresh_$timestamp.log"

& $HopRun `
    -f $workflow `
    -r $RunConfiguration `
    *> $logFile

if ($LASTEXITCODE -ne 0) {
    throw "Incremental refresh failed. See log: $logFile"
}

Write-Host "Incremental refresh finished. Log: $logFile"
