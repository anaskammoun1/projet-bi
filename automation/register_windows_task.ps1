param(
    [string]$TaskName = "ProjetPowerBI Incremental Refresh",
    [string]$ProjectDir = "C:\Users\Omar Safi\Desktop\ProjetPowerBI\projet-bi",
    [string]$RunAt = "02:00"
)

$ErrorActionPreference = "Stop"

$scriptPath = Join-Path $ProjectDir "automation\run_incremental_refresh.ps1"

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""

$trigger = New-ScheduledTaskTrigger -Daily -At $RunAt

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Description "Automatic incremental refresh for ProjetPowerBI warehouse" `
    -Force
