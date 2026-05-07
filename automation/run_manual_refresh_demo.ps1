param(
    [string]$HopRun = "hop-run",
    [string]$Psql = "C:\Program Files\PostgreSQL\18\bin\psql.exe",
    [string]$ProjectDir = "C:\Users\Omar Safi\Desktop\ProjetPowerBI\projet-bi",
    [string]$RunConfiguration = "local",
    [string]$DbHost = "localhost",
    [string]$DbPort = "5433",
    [string]$DbName = "air_quality_dw",
    [string]$DbUser = "codex_audit"
)

$ErrorActionPreference = "Stop"

$workflow = Join-Path $ProjectDir "hop\wf_refresh_incremental.hwf"
$checks = Join-Path $ProjectDir "sql\09_manual_refresh_demo_checks.sql"

Write-Host ""
Write-Host "=== BEFORE REFRESH ==="
& $Psql -h $DbHost -p $DbPort -U $DbUser -d $DbName -P pager=off -f $checks

Write-Host ""
Write-Host "=== RUNNING MANUAL INCREMENTAL REFRESH ==="
& $HopRun -f $workflow -r $RunConfiguration

if ($LASTEXITCODE -ne 0) {
    throw "Manual incremental refresh failed."
}

Write-Host ""
Write-Host "=== AFTER REFRESH ==="
& $Psql -h $DbHost -p $DbPort -U $DbUser -d $DbName -P pager=off -f $checks
