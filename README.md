# projet-bi

Canonical Apache Hop and PostgreSQL project for the air-quality BI warehouse.

## Entry Point

- `hop/wf_entrepot_global.hwf`
- `hop/wf_refresh_incremental.hwf`

This is the source of truth. It runs the full rebuild in this order:

1. Reset database objects.
2. Create the clean schema.
3. Create pollution staging tables.
4. Load OpenAQ location JSON.
5. Load active stations and active countries.
6. Load Open-Meteo weather JSON.
7. Load OpenAQ sensor measurements.
8. Flatten measurements.
9. Fill the hourly time dimension.
10. Load the hourly pollution fact.
11. Load annual CSV indicators.
12. Create BI views.

## Active Hop Files

Workflows:

- `hop/wf_entrepot_global.hwf`
- `hop/wf_refresh_incremental.hwf`
- `hop/wf_meteo.hwf`
- `hop/wf_pollution.hwf`
- `hop/wf_sante.hwf`

Pipelines:

- `hop/pl_api_pollution_raw.hpl`
- `hop/pl_api_sensor_measurements_raw.hpl`
- `hop/pl_api_meteo_raw.hpl`
- `hop/pl_file_pm25_csv_raw.hpl`
- `hop/pl_file_air_death_csv_raw.hpl`

## Active SQL Files

- `sql/00_reset_database.sql`
- `sql/script_sql.sql`
- `sql/sql_create_pollution_staging.sql`
- `sql/sql_insert_dim_station_from_pollution.sql`
- `sql/sql_flatten_stg_sensor_measurements.sql`
- `sql/sql_insert_dim_temps_from_pollution.sql`
- `sql/sql_insert_fait_pollution_heure.sql`
- `sql/sql_load_air_quality_csv_indicators.sql`
- `sql/04_views.sql`
- `sql/05_drop_unused_columns.sql`
- `sql/06_create_incremental_refresh_objects.sql`
- `sql/07_start_incremental_refresh.sql`
- `sql/08_finish_incremental_refresh.sql`
- `sql/sql_create_meteo_staging_if_missing.sql`
- `sql/09_manual_refresh_demo_checks.sql`
- `sql/10_add_inserted_at_columns.sql`
- `sql/99_validation_report.sql`

## Incremental Refresh

Use this workflow after one successful full rebuild:

- `hop/wf_refresh_incremental.hwf`

It does not reset the warehouse. It refreshes station metadata, requests only new OpenAQ measurements after the latest loaded fact timestamp, requests only missing Open-Meteo weather after the latest stored weather `end_date`, upserts facts, refreshes CSV indicators, recreates BI views, and logs the run in `etl_refresh_log`.

OpenAQ measurement requests are capped to 7-day chunks per station and pollutant. This keeps manual demos responsive and avoids timeout-prone large catch-up calls.

Inserted rows are timestamped with `inserted_at_utc` in:

- `fait_pollution_heure`
- `fait_indicateur_air_annuel`

### Automatic Mode

Use this for the real daily operation.

- `automation/register_windows_task.ps1`
- `automation/register_refresh_task_manual.ps1`
- `automation/HOW_TO_ENABLE_AUTO_REFRESH.md`

Example:

```powershell
powershell -ExecutionPolicy Bypass -File .\projet-bi\automation\register_windows_task.ps1 -RunAt "02:00"
```

For this machine, use the ready script with the configured Apache Hop path:

```powershell
powershell -ExecutionPolicy Bypass -File .\projet-bi\automation\register_refresh_task_manual.ps1 -RunAt "02:00"
```

### Manual Presentation Mode

Use this during the presentation to show the refresh being triggered manually.

- Open `hop/wf_refresh_incremental.hwf` in Apache Hop and click Run.
- Or run `automation/run_manual_refresh_demo.ps1` to show before/after counts around the refresh.

Demo checks:

- `sql/09_manual_refresh_demo_checks.sql`

## Database Model

Dimensions:

- `dim_pays`: active countries from OpenAQ stations.
- `dim_station`: active OpenAQ stations with coordinates and measurement date coverage.
- `dim_temps`: hourly calendar.
- `dim_polluant`: pollutant catalog.
- `dim_meteo_classe`: weather class bands.

Facts:

- `fait_pollution_heure`: one row per station, hour, and pollutant.
- `fait_indicateur_air_annuel`: one row per country, year, and annual indicator.

Staging:

- `stg_pollution_json_raw`
- `stg_sensor_measurements_raw`
- `stg_pollution_mesure`
- `stg_meteo_json_raw`
- `stg_pm25_csv_raw`
- `stg_air_death_csv_raw`

## Metabase Views

- `vw_active_station_countries`: active station count by country.
- `vw_pollution_annuelle_station`: annual pollution and weather averages by station and pollutant.
- `vw_air_indicators_active_countries`: annual PM2.5 exposure and death-rate indicators.
- `vw_correlation_sante_pollution`: compact PM2.5 comparison rows where station data and country PM2.5 data overlap.
- `vw_etl_refresh_status`: incremental refresh execution log.

## Metabase Dashboards

Created dashboards in local Metabase:

- `Projet BI Air Quality - Dashboard 2026-05-07 15:26`
  - URL: `http://localhost:3000/dashboard/34`
  - Purpose: general project indicators and ETL monitoring.
- `Projet BI Air Quality - Analyse multi-niveaux 2026-05-07 15:35`
  - URL: `http://localhost:3000/dashboard/35`
  - Purpose: evaluation-ready dashboard with global filters and multi-level analysis.
- `Projet BI Air Quality - Story Drilldown 2026-05-07 15:44`
  - URL: `http://localhost:3000/dashboard/36`
  - Purpose: polished presentation dashboard with KPI strip, map navigation, risk analysis, weather analysis, PM2.5 comparison, incremental-refresh proof, and final drill-down table.
- `Air Quality & Weather Impact Analysis - Amazing Copy 2026-05-07 16:15`
  - URL: `http://localhost:3000/dashboard/38`
  - Purpose: improved copy of dashboard 37, keeping the original intact while adding a cleaner presentation story, global filters, section headings, weather-impact analysis, health indicators, and final drill-down.

Dashboard 35 includes four global filters:

- `Pays`
- `Station`
- `Annee`
- `Polluant`

The analysis flow is designed as a drill-down:

1. Start with global KPIs and active station map.
2. Filter by country.
3. Drill into stations and pollutants.
4. Compare AQI, WHO threshold exceedances, weather impact, PM2.5 country exposure, and recent incremental inserts.

Recommended presentation flow with dashboard 36:

1. Open `http://localhost:3000/dashboard/36`.
2. Show the four KPI cards: active countries, active stations, total measurements, last ETL insert.
3. Use the `Pays` filter to select one country.
4. Use the map to explain station coverage.
5. Use `Polluant = pm25` to focus on PM2.5.
6. Read the trend, critical stations, WHO exceedances, and weather scatter plot.
7. End with `Refresh incremental visible` and `Table drill-down finale` to prove new rows are added to the existing warehouse.

Recommended presentation flow with dashboard 38:

1. Open `http://localhost:3000/dashboard/38`.
2. Start with the KPI strip: measurements, AQI, WHO exceedance rate, latest ETL insertion.
3. Apply filters in this order: `Continent`, `Ville`, `Annee`, `Polluant`.
4. Use the map to navigate geographically.
5. Compare continents, monthly trends, critical cities, and seasonal WHO exceedances.
6. Explain weather impact with temperature and humidity scatter plots.
7. Finish with PM2.5 country exposure, incremental refresh proof, and the final drill-down table.

## Notes

- The active CSV sources are the two root CSV files.
- The old `data.csv` / WHO health pipeline is not part of the current workflow.
- The OpenAQ API key is still stored in Hop pipeline XML and should be moved to a Hop variable before sharing the project.
