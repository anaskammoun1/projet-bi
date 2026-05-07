SELECT 'dim_pays' AS check_name, COUNT(*) AS rows
FROM public.dim_pays
UNION ALL
SELECT 'dim_station', COUNT(*)
FROM public.dim_station
UNION ALL
SELECT 'fait_pollution_heure', COUNT(*)
FROM public.fait_pollution_heure
UNION ALL
SELECT 'fait_indicateur_air_annuel', COUNT(*)
FROM public.fait_indicateur_air_annuel;

SELECT
    pays_code,
    pays_nom,
    continent,
    source_active_station
FROM public.dim_pays
ORDER BY pays_code;

SELECT
    'fait_pollution_heure_null_cells' AS check_name,
    COUNT(*) FILTER (WHERE id_temps IS NULL) +
    COUNT(*) FILTER (WHERE id_station IS NULL) +
    COUNT(*) FILTER (WHERE id_polluant IS NULL) +
    COUNT(*) FILTER (WHERE id_meteo_classe IS NULL) +
    COUNT(*) FILTER (WHERE valeur IS NULL) +
    COUNT(*) FILTER (WHERE unite IS NULL) +
    COUNT(*) FILTER (WHERE indice_aqi IS NULL) +
    COUNT(*) FILTER (WHERE seuil_oms_depasse IS NULL) +
    COUNT(*) FILTER (WHERE temperature_c IS NULL) +
    COUNT(*) FILTER (WHERE humidite_relative_pct IS NULL) +
    COUNT(*) FILTER (WHERE vitesse_vent_10m_ms IS NULL) +
    COUNT(*) FILTER (WHERE precipitation_mm IS NULL) +
    COUNT(*) FILTER (WHERE rain_flag IS NULL) AS null_cells
FROM public.fait_pollution_heure;

SELECT
    'fait_indicateur_air_annuel_null_cells' AS check_name,
    COUNT(*) FILTER (WHERE id_pays IS NULL) +
    COUNT(*) FILTER (WHERE annee IS NULL) +
    COUNT(*) FILTER (WHERE indicator_code IS NULL) +
    COUNT(*) FILTER (WHERE indicator_nom IS NULL) +
    COUNT(*) FILTER (WHERE indicator_valeur IS NULL) +
    COUNT(*) FILTER (WHERE unite IS NULL) +
    COUNT(*) FILTER (WHERE source_fichier IS NULL) AS null_cells
FROM public.fait_indicateur_air_annuel;

WITH weather AS (
    SELECT
        station_code,
        (json_array_elements_text((api_result::json -> 'hourly' -> 'time')))::timestamp AS ts
    FROM public.stg_meteo_json_raw
)
SELECT
    s.station_code,
    COUNT(DISTINCT pm.date_heure_utc) AS pollution_rows,
    MIN(pm.date_heure_utc) AS pollution_min_utc,
    MAX(pm.date_heure_utc) AS pollution_max_utc,
    MIN(w.ts) AS weather_min_utc,
    MAX(w.ts) AS weather_max_utc
FROM public.dim_station s
LEFT JOIN public.stg_pollution_mesure pm
    ON pm.station_code = s.station_code
LEFT JOIN weather w
    ON w.station_code = s.station_code
GROUP BY s.station_code
ORDER BY s.station_code;
