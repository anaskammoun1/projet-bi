SELECT
    'before_or_after_refresh_counts' AS check_name,
    (SELECT COUNT(*) FROM public.fait_pollution_heure) AS pollution_fact_rows,
    (SELECT COUNT(*) FROM public.fait_indicateur_air_annuel) AS annual_indicator_rows,
    (SELECT COUNT(*) FROM public.stg_pollution_mesure) AS staging_pollution_rows,
    (SELECT COUNT(*) FROM public.stg_meteo_json_raw) AS staging_weather_rows,
    (SELECT MAX(inserted_at_utc) FROM public.fait_pollution_heure) AS latest_pollution_inserted_at_utc,
    (SELECT MAX(inserted_at_utc) FROM public.fait_indicateur_air_annuel) AS latest_indicator_inserted_at_utc;

SELECT
    'recently_inserted_pollution_rows' AS check_name,
    COUNT(*) AS rows_inserted_last_15_minutes
FROM public.fait_pollution_heure
WHERE inserted_at_utc >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '15 minutes';

SELECT
    station_code,
    COUNT(*) AS measurement_rows,
    MIN(date_heure_utc) AS first_measurement_utc,
    MAX(date_heure_utc) AS latest_measurement_utc
FROM public.stg_pollution_mesure
GROUP BY station_code
ORDER BY station_code;

SELECT *
FROM public.vw_etl_refresh_status
ORDER BY started_at_utc DESC
LIMIT 5;
