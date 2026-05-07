# Air Quality BI Warehouse Report

Date: 2026-04-29

## Objective

Build a clean PostgreSQL data warehouse for air-quality dashboarding in Metabase. The warehouse combines station-level air-pollution measurements, hourly weather enrichment, and annual country-level air-quality indicators.

## Active Workflow

Main workflow:

- `projet-bi/hop/wf_entrepot_global.hwf`

Incremental workflow:

- `projet-bi/hop/wf_refresh_incremental.hwf`

Execution order:

1. `00_reset_database.sql`
2. `script_sql.sql`
3. `sql_create_pollution_staging.sql`
4. `pl_api_pollution_raw.hpl`
5. `sql_insert_dim_station_from_pollution.sql`
6. `wf_meteo.hwf`
7. `pl_api_sensor_measurements_raw.hpl`
8. `sql_flatten_stg_sensor_measurements.sql`
9. `sql_insert_dim_temps_from_pollution.sql`
10. `sql_insert_fait_pollution_heure.sql`
11. `wf_sante.hwf`
12. `04_views.sql`

## Incremental Refresh

The project supports both automatic and manual incremental refresh. The full rebuild workflow remains available for clean resets, while the incremental workflow refreshes only new or missing data.

Incremental workflow:

- `projet-bi/hop/wf_refresh_incremental.hwf`

Incremental behavior:

- does not run `00_reset_database.sql`
- does not recreate the whole schema
- refreshes OpenAQ station metadata
- requests OpenAQ measurements only after the latest loaded fact timestamp per station and pollutant
- limits each OpenAQ measurement request to a 7-day chunk to avoid API timeouts during manual demos
- requests Open-Meteo weather only after the latest stored `stg_meteo_json_raw.end_date` per station
- upserts facts using existing unique constraints
- stores `inserted_at_utc` on fact rows so new inserted data can be demonstrated
- refreshes Metabase views
- records execution status in `etl_refresh_log`

Refresh control SQL files:

- `06_create_incremental_refresh_objects.sql`
- `07_start_incremental_refresh.sql`
- `08_finish_incremental_refresh.sql`
- `sql_create_meteo_staging_if_missing.sql`
- `09_manual_refresh_demo_checks.sql`
- `10_add_inserted_at_columns.sql`

Automation scripts:

- `automation/run_incremental_refresh.ps1`
- `automation/register_windows_task.ps1`
- `automation/run_manual_refresh_demo.ps1`

Windows Task Scheduler example:

```powershell
powershell -ExecutionPolicy Bypass -File .\projet-bi\automation\register_windows_task.ps1 -RunAt "02:00"
```

### Automatic Mode

Automatic mode is used in production-like operation. Windows Task Scheduler launches the incremental workflow every day at the configured time. The scheduled task calls:

- `automation/run_incremental_refresh.ps1`

This mode proves that the warehouse can update without a full rebuild and without manual intervention.

### Manual Presentation Mode

Manual mode is used during the presentation. The goal is to show the refresh mechanism clearly:

1. Show current row counts with `09_manual_refresh_demo_checks.sql`.
2. Run `hop/wf_refresh_incremental.hwf` manually in Apache Hop.
3. Show the new entry in `vw_etl_refresh_status`.
4. Show `inserted_at_utc` or `rows_inserted_last_15_minutes` to prove when new rows entered the warehouse.
5. Refresh Metabase dashboards.

For old stations with a large backlog, the refresh may need multiple runs. Each run advances the next 7-day window per sensor, so the process stays incremental and avoids large API calls.

Optional one-command demo:

```powershell
powershell -ExecutionPolicy Bypass -File .\projet-bi\automation\run_manual_refresh_demo.ps1
```

Manual demo SQL:

- `sql/09_manual_refresh_demo_checks.sql`

Monitoring:

```sql
SELECT *
FROM public.vw_etl_refresh_status
ORDER BY started_at_utc DESC;
```

## Data Sources

API sources:

- OpenAQ `/v3/locations/{location_id}`
- OpenAQ `/v3/sensors/{sensor_id}/measurements`
- Open-Meteo archive/hourly weather

CSV sources:

- `pm25-air-pollution.csv`
- `death-rate-household-and-ambient-air-pollution.csv`

## Warehouse Tables

Dimensions:

- `dim_pays`: active station countries.
- `dim_station`: station identity, coordinates, and measurement coverage.
- `dim_temps`: hourly time dimension.
- `dim_polluant`: pollutant names and standard units.
- `dim_meteo_classe`: weather bands for temperature, humidity, wind, and rain.

Facts:

- `fait_pollution_heure`: hourly pollution fact enriched with weather.
- `fait_indicateur_air_annuel`: annual PM2.5 exposure and death-rate indicators.

Both fact tables include `inserted_at_utc`, which stores the UTC timestamp when a row first entered the warehouse. During an upsert, existing rows keep their original insertion timestamp, while newly inserted rows receive the current refresh timestamp.

Staging:

- `stg_pollution_json_raw`
- `stg_sensor_measurements_raw`
- `stg_pollution_mesure`
- `stg_meteo_json_raw`
- `stg_pm25_csv_raw`
- `stg_air_death_csv_raw`

## BI Views

### `vw_active_station_countries`

Purpose: country filter and active station overview.

Fields:

- `pays_code`
- `pays_nom`
- `active_station_count`
- `latest_station_measurement_utc`

### `vw_pollution_annuelle_station`

Purpose: main Metabase view for pollution analysis.

Use for:

- average pollutant value by country/station/year
- AQI trend by pollutant
- WHO threshold exceedance counts
- weather context by station/year

### `vw_air_indicators_active_countries`

Purpose: annual country-level CSV indicators.

Use for:

- country PM2.5 exposure trend
- 2019 air-pollution death-rate comparison

### `vw_correlation_sante_pollution`

Purpose: compact PM2.5 station-vs-country comparison.

This view intentionally keeps only country-year rows where station PM2.5 and country PM2.5 exposure overlap. This avoids null-heavy dashboard cards.

Current valid overlap rows:

- India, 2016
- United Kingdom, 2016

### `vw_etl_refresh_status`

Purpose: monitor automatic incremental refresh runs.

Use for:

- last refresh status
- rows added during the refresh
- start and finish timestamps
- operational troubleshooting

## Cleanup Summary

Removed local artifacts:

- browser cache/profile folders
- generated schema images and HTML
- old WHO reference dump
- archived legacy project copies
- legacy Hop/PDI experiments
- unused empty SQL files

Schema definitions were also slimmed for future rebuilds:

- removed unused location request dates from `stg_pollution_json_raw`
- removed carried-through OpenAQ/sensor IDs from `stg_pollution_mesure`
- removed unused station metadata columns from `dim_station`
- removed unused load timestamp columns from final facts

## Live Database Column Cleanup

The live database still requires table ownership before in-place column drops can run with `codex_audit`.

Admin ownership grant:

```sql
ALTER TABLE public.stg_pollution_json_raw OWNER TO codex_audit;
ALTER TABLE public.stg_pollution_mesure OWNER TO codex_audit;
ALTER TABLE public.dim_station OWNER TO codex_audit;
ALTER TABLE public.fait_pollution_heure OWNER TO codex_audit;
ALTER TABLE public.fait_indicateur_air_annuel OWNER TO codex_audit;
```

Then run:

```sql
\i projet-bi/sql/05_drop_unused_columns.sql
```

A full clean rebuild from Hop will create the slimmed schema automatically because the SQL files have already been updated.

## Metabase Dashboard Plan

Two local Metabase dashboards were created without overwriting existing objects:

- `Projet BI Air Quality - Dashboard 2026-05-07 15:26`
  - URL: `http://localhost:3000/dashboard/34`
  - Purpose: general project overview and ETL monitoring.
- `Projet BI Air Quality - Analyse multi-niveaux 2026-05-07 15:35`
  - URL: `http://localhost:3000/dashboard/35`
  - Purpose: evaluation-ready analysis dashboard.
- `Projet BI Air Quality - Story Drilldown 2026-05-07 15:44`
  - URL: `http://localhost:3000/dashboard/36`
  - Purpose: final presentation dashboard with a clearer analytical story.
- `Air Quality & Weather Impact Analysis - Amazing Copy 2026-05-07 16:15`
  - URL: `http://localhost:3000/dashboard/38`
  - Purpose: improved copy of dashboard 37. The original dashboard 37 was preserved unchanged.

Dashboard 35 addresses the evaluation criteria directly:

- Pertinent indicators:
  - active countries and active stations
  - station-level pollution volume and AQI
  - WHO threshold exceedances
  - weather versus pollution relationship
  - PM2.5 station averages versus country exposure indicators
  - recent incremental inserts using `inserted_at_utc`
- Navigation and drill-down:
  - start from global KPIs
  - filter by country
  - drill down to station
  - refine by year and pollutant
  - inspect detailed station-pollutant rows
- Filters:
  - `Pays`
  - `Station`
  - `Annee`
  - `Polluant`
- Multi-level analysis:
  - country level: active station coverage and annual indicators
  - station level: AQI and average pollutant values
  - pollutant level: PM2.5, PM10, NO2, O3 and other tracked pollutants
  - time level: annual trend and recent ETL insert timestamp
  - operational level: incremental refresh monitoring

Final dashboard 36 improves readability and presentation value:

- KPI strip at the top for immediate context.
- Map-based station navigation.
- Trend chart for temporal analysis.
- Ranked AQI chart for station drill-down.
- WHO exceedance chart for health relevance.
- Weather scatter plot for cross-domain analysis.
- PM2.5 station-versus-country comparison for multi-source analysis.
- Incremental-refresh table using `inserted_at_utc` as proof that the workflow adds new rows without rebuilding.
- Final detail table for the full drill-down path.

Suggested demonstration scenario:

1. Start from global KPIs.
2. Select a country using `Pays`.
3. Select `pm25` using `Polluant`.
4. Explain active stations on the map.
5. Compare yearly trend, AQI ranking, and WHO exceedances.
6. Use weather scatter plot to discuss explanatory context.
7. End with the incremental refresh card and the drill-down detail table.

Dashboard 38 adds a more polished weather-impact narrative:

- KPI strip:
  - measurements collected
  - average AQI
  - WHO exceedance rate
  - latest ETL insertion timestamp
- Global filters:
  - `Continent`
  - `Ville`
  - `Annee`
  - `Polluant`
- Navigation:
  - continent-level comparison
  - city drill-down
  - monthly pollutant trend
  - final detail table
- Weather impact:
  - temperature versus pollution scatter plot
  - humidity versus pollution scatter plot
- Health and operational proof:
  - WHO exceedances by season
  - PM2.5 station average versus country exposure
  - incremental refresh table based on `inserted_at_utc`

## Known Risks

- OpenAQ API key is stored in pipeline XML. Move it to a Hop variable before sharing.
- Some pollutants arrive in mixed units such as `ppb` and `ug/m3`; unit normalization should be reviewed before high-stakes interpretation.
- Beijing and Cairo are staged but excluded from `dim_station` by the active-station filter.
