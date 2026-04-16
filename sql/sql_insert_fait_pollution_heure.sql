-- Insert pollution facts at hourly grain and enrich them with hourly weather.
-- Source tables:
--   - public.stg_pollution_mesure
--   - public.stg_meteo_json_raw
-- Dimension joins:
--   - public.dim_station via station_code
--   - public.dim_temps via hourly timestamp bucket
--   - public.dim_meteo_classe via derived weather classes

WITH normalized AS (
    SELECT
        pm.station_code,
        pm.date_heure_utc,
        LOWER(pm.parameter_code) AS parameter_code,
        pm.valeur,
        pm.unite,
        CASE
            WHEN LOWER(pm.parameter_code) = 'co'
             AND (
                 LOWER(COALESCE(pm.unite, '')) LIKE 'ug/m3%'
                 OR LOWER(COALESCE(pm.unite, '')) LIKE 'ug m-3%'
                 OR LOWER(COALESCE(pm.unite, '')) LIKE CHR(181) || 'g/m3%'
                 OR LOWER(COALESCE(pm.unite, '')) LIKE CHR(181) || 'g m-3%'
             )
                THEN pm.valeur / 1000.0
            ELSE pm.valeur
        END AS normalized_value
    FROM public.stg_pollution_mesure pm
    WHERE pm.date_heure_utc IS NOT NULL
),
hourly_pivot AS (
    SELECT
        n.station_code,
        date_trunc('hour', n.date_heure_utc) AS bucket_utc,
        AVG(n.normalized_value) FILTER (WHERE n.parameter_code = 'co') AS co_mg_m3,
        AVG(n.normalized_value) FILTER (WHERE n.parameter_code = 'no2') AS no2_ug_m3,
        AVG(n.normalized_value) FILTER (WHERE n.parameter_code = 'pm25') AS pm25_ug_m3,
        AVG(n.normalized_value) FILTER (WHERE n.parameter_code = 'pm10') AS pm10_ug_m3,
        AVG(n.normalized_value) FILTER (WHERE n.parameter_code = 'o3') AS o3_ug_m3,
        AVG(n.normalized_value) FILTER (WHERE n.parameter_code = 'benzene') AS benzene_ug_m3,
        AVG(n.normalized_value) FILTER (WHERE n.parameter_code = 'nox') AS nox_ppb
    FROM normalized n
    GROUP BY
        n.station_code,
        date_trunc('hour', n.date_heure_utc)
    HAVING
        COUNT(*) FILTER (
            WHERE n.parameter_code IN ('co', 'no2', 'pm25', 'pm10', 'o3', 'benzene', 'nox')
        ) > 0
),
weather_hourly AS (
    SELECT
        r.station_code,
        (time_v.value)::timestamp AS bucket_utc,
        NULLIF(temp_v.value, '')::numeric(6,2) AS temperature_c,
        NULLIF(hum_v.value, '')::numeric(6,2) AS humidite_relative_pct,
        NULLIF(wind_v.value, '')::numeric(6,2) AS wind_speed_10m_kmh,
        NULLIF(prec_v.value, '')::numeric(6,2) AS precipitation_mm
    FROM public.stg_meteo_json_raw r
    CROSS JOIN LATERAL json_array_elements_text((r.api_result::json -> 'hourly' -> 'time')) WITH ORDINALITY AS time_v(value, ord)
    JOIN LATERAL json_array_elements_text((r.api_result::json -> 'hourly' -> 'temperature_2m')) WITH ORDINALITY AS temp_v(value, ord2)
        ON time_v.ord = temp_v.ord2
    JOIN LATERAL json_array_elements_text((r.api_result::json -> 'hourly' -> 'relative_humidity_2m')) WITH ORDINALITY AS hum_v(value, ord3)
        ON time_v.ord = hum_v.ord3
    JOIN LATERAL json_array_elements_text((r.api_result::json -> 'hourly' -> 'wind_speed_10m')) WITH ORDINALITY AS wind_v(value, ord4)
        ON time_v.ord = wind_v.ord4
    JOIN LATERAL json_array_elements_text((r.api_result::json -> 'hourly' -> 'precipitation')) WITH ORDINALITY AS prec_v(value, ord5)
        ON time_v.ord = prec_v.ord5
),
weather_classified AS (
    SELECT
        w.station_code,
        w.bucket_utc,
        w.temperature_c,
        w.humidite_relative_pct,
        ROUND(w.wind_speed_10m_kmh / 3.6, 2) AS vitesse_vent_10m_ms,
        COALESCE(w.precipitation_mm, 0)::numeric(6,2) AS precipitation_mm,
        (COALESCE(w.precipitation_mm, 0) > 0) AS rain_flag,
        CASE
            WHEN w.temperature_c < 0 THEN 'tr_s froid'
            WHEN w.temperature_c < 10 THEN 'froid'
            WHEN w.temperature_c < 20 THEN 'modere'
            ELSE 'chaud'
        END AS temp_bande,
        CASE
            WHEN w.humidite_relative_pct < 40 THEN 'sec'
            WHEN w.humidite_relative_pct <= 70 THEN 'normal'
            ELSE 'humide'
        END AS humidite_bande,
        CASE
            WHEN w.wind_speed_10m_kmh < 10 THEN 'calme'
            WHEN w.wind_speed_10m_kmh <= 30 THEN 'modere'
            ELSE 'fort'
        END AS vent_bande,
        CASE
            WHEN COALESCE(w.precipitation_mm, 0) = 0 THEN 'aucune'
            WHEN w.precipitation_mm < 2 THEN 'legere'
            WHEN w.precipitation_mm < 10 THEN 'moderee'
            ELSE 'forte'
        END AS pluie_classe
    FROM weather_hourly w
),
weather_enriched AS (
    SELECT
        wc.station_code,
        wc.bucket_utc,
        wc.temperature_c,
        wc.humidite_relative_pct,
        wc.vitesse_vent_10m_ms,
        wc.precipitation_mm,
        wc.rain_flag,
        mc.id_meteo_classe
    FROM weather_classified wc
    LEFT JOIN public.dim_meteo_classe mc
        ON mc.temp_bande = wc.temp_bande
       AND mc.humidite_bande = wc.humidite_bande
       AND mc.vent_bande = wc.vent_bande
       AND mc.pluie_classe = wc.pluie_classe
       AND mc.source_classe = 'Open-Meteo'
)
INSERT INTO public.fait_pollution_heure (
    id_temps,
    id_station,
    id_meteo_classe,
    co_mg_m3,
    no2_ug_m3,
    pm25_ug_m3,
    pm10_ug_m3,
    o3_ug_m3,
    benzene_ug_m3,
    nox_ppb,
    temperature_c,
    humidite_relative_pct,
    vitesse_vent_10m_ms,
    precipitation_mm,
    rain_flag,
    charge_le_utc
)
SELECT
    t.id_temps,
    st.id_station,
    we.id_meteo_classe,
    hp.co_mg_m3,
    hp.no2_ug_m3,
    hp.pm25_ug_m3,
    hp.pm10_ug_m3,
    hp.o3_ug_m3,
    hp.benzene_ug_m3,
    hp.nox_ppb,
    we.temperature_c,
    we.humidite_relative_pct,
    we.vitesse_vent_10m_ms,
    COALESCE(we.precipitation_mm, 0)::numeric(6,2),
    COALESCE(we.rain_flag, false),
    CURRENT_TIMESTAMP
FROM hourly_pivot hp
JOIN public.dim_station st
    ON st.station_code = hp.station_code
JOIN public.dim_temps t
    ON t.date_heure_utc = hp.bucket_utc
LEFT JOIN weather_enriched we
    ON we.station_code = hp.station_code
   AND we.bucket_utc = hp.bucket_utc
ON CONFLICT (id_temps, id_station) DO UPDATE
SET
    id_meteo_classe = COALESCE(EXCLUDED.id_meteo_classe, public.fait_pollution_heure.id_meteo_classe),
    co_mg_m3 = COALESCE(EXCLUDED.co_mg_m3, public.fait_pollution_heure.co_mg_m3),
    no2_ug_m3 = COALESCE(EXCLUDED.no2_ug_m3, public.fait_pollution_heure.no2_ug_m3),
    pm25_ug_m3 = COALESCE(EXCLUDED.pm25_ug_m3, public.fait_pollution_heure.pm25_ug_m3),
    pm10_ug_m3 = COALESCE(EXCLUDED.pm10_ug_m3, public.fait_pollution_heure.pm10_ug_m3),
    o3_ug_m3 = COALESCE(EXCLUDED.o3_ug_m3, public.fait_pollution_heure.o3_ug_m3),
    benzene_ug_m3 = COALESCE(EXCLUDED.benzene_ug_m3, public.fait_pollution_heure.benzene_ug_m3),
    nox_ppb = COALESCE(EXCLUDED.nox_ppb, public.fait_pollution_heure.nox_ppb),
    temperature_c = COALESCE(EXCLUDED.temperature_c, public.fait_pollution_heure.temperature_c),
    humidite_relative_pct = COALESCE(EXCLUDED.humidite_relative_pct, public.fait_pollution_heure.humidite_relative_pct),
    vitesse_vent_10m_ms = COALESCE(EXCLUDED.vitesse_vent_10m_ms, public.fait_pollution_heure.vitesse_vent_10m_ms),
    precipitation_mm = COALESCE(EXCLUDED.precipitation_mm, public.fait_pollution_heure.precipitation_mm),
    rain_flag = COALESCE(EXCLUDED.rain_flag, public.fait_pollution_heure.rain_flag),
    charge_le_utc = CURRENT_TIMESTAMP;
