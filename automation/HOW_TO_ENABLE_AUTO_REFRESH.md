# Enable Automatic Refresh

## 1. Register The Daily Task

From the project root:

```powershell
cd "C:\Users\Omar Safi\Desktop\ProjetPowerBI"
powershell -ExecutionPolicy Bypass -File ".\projet-bi\automation\register_refresh_task_manual.ps1" -RunAt "02:00"
```

The script already uses this Apache Hop path:

```text
C:\Users\Omar Safi\Downloads\apache-hop-client-2.17.0\hop\hop-run.bat
```

## 2. Check That It Exists

```powershell
Get-ScheduledTask -TaskName "ProjetPowerBI Incremental Refresh"
```

## 3. Run It Manually Once

```powershell
Start-ScheduledTask -TaskName "ProjetPowerBI Incremental Refresh"
```

## 4. Check Logs

Logs are written to:

```text
projet-bi/logs/
```

## 5. Check Database Refresh Status

```sql
SELECT *
FROM public.vw_etl_refresh_status
ORDER BY started_at_utc DESC
LIMIT 5;
```
