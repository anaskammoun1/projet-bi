param(
    [string]$TaskName = "ProjetPowerBI Incremental Refresh",
    [string]$HopRun = "C:\Users\Omar Safi\Downloads\apache-hop-client-2.17.0\hop\hop-run.bat",
    [string]$ProjectDir = "C:\Users\Omar Safi\Desktop\ProjetPowerBI\projet-bi",
    [string]$RunConfiguration = "local",
    [string]$RunAt = "02:00"
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($HopRun)) {
    $candidate = Get-Command hop-run -ErrorAction SilentlyContinue
    if ($candidate) {
        $HopRun = $candidate.Source
    }
}

if ([string]::IsNullOrWhiteSpace($HopRun) -or -not (Test-Path -LiteralPath $HopRun)) {
    throw @"
Could not find hop-run.

Run this to locate it:
Get-ChildItem -Path "C:\Users\Omar Safi" -Recurse -Filter hop-run.bat -ErrorAction SilentlyContinue

Then rerun this script with:
powershell -ExecutionPolicy Bypass -File ".\projet-bi\automation\register_refresh_task_manual.ps1" -HopRun "FULL_PATH_TO_HOP_RUN_BAT"
"@
}

$workflow = Join-Path $ProjectDir "hop\wf_refresh_incremental.hwf"
$logDir = Join-Path $ProjectDir "logs"
$runnerScript = Join-Path $ProjectDir "automation\run_incremental_refresh_generated.ps1"

if (-not (Test-Path -LiteralPath $workflow)) {
    throw "Workflow not found: $workflow"
}

if (-not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

$runnerContent = @"
`$ErrorActionPreference = "Stop"

`$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
`$logFile = "$logDir\incremental_refresh_`$timestamp.log"

& "$HopRun" -f "$workflow" -r "$RunConfiguration" *> `$logFile

if (`$LASTEXITCODE -ne 0) {
    throw "Incremental refresh failed. See log: `$logFile"
}

Write-Host "Incremental refresh finished. Log: `$logFile"
"@

Set-Content -LiteralPath $runnerScript -Value $runnerContent -Encoding UTF8

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$runnerScript`""

$trigger = New-ScheduledTaskTrigger -Daily -At $RunAt

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Description "Automatic incremental refresh for ProjetPowerBI warehouse" `
    -Force

Write-Host "Scheduled task created: $TaskName"
Write-Host "Runs daily at: $RunAt"
Write-Host "Hop runner: $HopRun"
Write-Host "Generated runner: $runnerScript"
Write-Host "Logs folder: $logDir"
