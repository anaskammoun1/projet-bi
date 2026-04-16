# projet-bi

Canonical project structure for the BI warehouse.

## Active folders

- `hop/`: active Apache Hop pipelines and workflows
- `sql/`: active PostgreSQL DDL, load SQL, indexes, and views
- `dashboards/`: dashboard exports or placeholders
- `rapport/`: report material

## Active Hop files

### Workflows

- `hop/wf_meteo.hwf`
- `hop/wf_pollution.hwf`
- `hop/wf_sante.hwf`

### Pipelines

- `hop/pl_api_meteo_raw.hpl`
- `hop/pl_api_pollution_raw.hpl`
- `hop/pl_api_sensor_measurements_raw.hpl`
- `hop/pl_api_who_raw.hpl`
- `hop/pl_file_who_csv_raw.hpl`
- `hop/pl_load_who_sante_annuelle.hpl`
- `hop/t01_who_dim_pays.hpl`

## Active SQL files

- `sql/script_sql.sql`
- `sql/01_create_dimensions.sql`
- `sql/02_create_fait.sql`
- `sql/03_indexes.sql`
- `sql/04_views.sql`
- `sql/sql_flatten_stg_sensor_measurements.sql`
- `sql/sql_insert_dim_station_from_pollution.sql`
- `sql/sql_insert_dim_temps_from_pollution.sql`
- `sql/sql_insert_fait_pollution_heure.sql`

## Legacy folders

- `legacy/hop/`: archived Hop experiments and wrongly named pipelines
- `legacy/pdi/`: archived Pentaho / PDI `.ktr` and `.kjb` jobs

These legacy files are kept for reference only and should not be used for the main flow.

## Current conventions

- use files in `hop/` and `sql/` as the source of truth
- use repo-root `data.csv` as the health CSV source for `wf_sante`
- use `HANDOFF_2026-04-16.md` in the repo root for the latest execution state and pending tasks
