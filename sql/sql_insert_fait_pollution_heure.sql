WITH pollutant_catalog AS (
    SELECT * FROM (VALUES
        ('co',   'Carbon monoxide',       'mg/m3'),
        ('no',   'Nitric oxide',          'ug/m3'),
        ('no2',  'Nitrogen dioxide',      'ug/m3'),
        ('nox',  'Nitrogen oxides',       'ppb'),
        ('o3',   'Ozone',                 'ug/m3'),
        ('pm10', 'Particulate matter 10', 'ug/m3'),
        ('pm25', 'Particulate matter 2.5','ug/m3'),
        ('so2',  'Sulfur dioxide',        'ug/m3')
    ) AS v(parameter_code, parameter_nom, unite_standard)
),
upsert_polluant AS (
    INSERT INTO public.dim_polluant (parameter_code, parameter_nom, unite_standard)
    SELECT parameter_code, parameter_nom, unite_standard
    FROM pollutant_catalog
    ON CONFLICT (parameter_code) DO UPDATE
    SET parameter_nom = EXCLUDED.parameter_nom,
        unite_standard = EXCLUDED.unite_standard
    RETURNING id_polluant, parameter_code
),
normalized AS (
    SELECT
        pm.station_code,
        date_trunc('hour', pm.date_heure_utc) AS bucket_utc,
        LOWER(pm.parameter_code) AS parameter_code,
        AVG(
            CASE
                WHEN LOWER(pm.parameter_code) = 'co'
                 AND (
                     LOWER(pm.unite) LIKE 'ug/m3%'
                     OR LOWER(pm.unite) LIKE 'ug m-3%'
                     OR LOWER(pm.unite) LIKE CHR(181) || 'g/m3%'
                     OR LOWER(pm.unite) LIKE CHR(181) || 'g m-3%'
                 )
                    THEN pm.valeur / 1000.0
                ELSE pm.valeur
            END
        )::numeric(12,4) AS valeur,
        CASE
            WHEN LOWER(pm.parameter_code) = 'co' THEN 'mg/m3'
            WHEN LOWER(pm.parameter_code) = 'nox' THEN 'ppb'
            ELSE 'ug/m3'
        END AS unite
    FROM public.stg_pollution_mesure pm
    WHERE pm.valeur IS NOT NULL
      AND pm.parameter_code IN ('co', 'no', 'no2', 'nox', 'o3', 'pm10', 'pm25', 'so2')
    GROUP BY
        pm.station_code,
        date_trunc('hour', pm.date_heure_utc),
        LOWER(pm.parameter_code)
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
        station_code,
        bucket_utc,
        temperature_c,
        humidite_relative_pct,
        ROUND(wind_speed_10m_kmh / 3.6, 2)::numeric(6,2) AS vitesse_vent_10m_ms,
        COALESCE(precipitation_mm, 0)::numeric(6,2) AS precipitation_mm,
        (COALESCE(precipitation_mm, 0) > 0) AS rain_flag,
        CASE
            WHEN temperature_c < 0 THEN 'tres froid'
            WHEN temperature_c < 10 THEN 'froid'
            WHEN temperature_c < 20 THEN 'modere'
            ELSE 'chaud'
        END AS temp_bande,
        CASE
            WHEN humidite_relative_pct < 40 THEN 'sec'
            WHEN humidite_relative_pct <= 70 THEN 'normal'
            ELSE 'humide'
        END AS humidite_bande,
        CASE
            WHEN wind_speed_10m_kmh < 10 THEN 'calme'
            WHEN wind_speed_10m_kmh <= 30 THEN 'modere'
            ELSE 'fort'
        END AS vent_bande,
        CASE
            WHEN COALESCE(precipitation_mm, 0) = 0 THEN 'aucune'
            WHEN precipitation_mm < 2 THEN 'legere'
            WHEN precipitation_mm < 10 THEN 'moderee'
            ELSE 'forte'
        END AS pluie_classe
    FROM weather_hourly
    WHERE temperature_c IS NOT NULL
      AND humidite_relative_pct IS NOT NULL
      AND wind_speed_10m_kmh IS NOT NULL
),
upsert_meteo AS (
    INSERT INTO public.dim_meteo_classe (
        temp_bande,
        humidite_bande,
        vent_bande,
        pluie_classe,
        source_classe
    )
    SELECT DISTINCT
        temp_bande,
        humidite_bande,
        vent_bande,
        pluie_classe,
        'Open-Meteo'
    FROM weather_classified
    ON CONFLICT (temp_bande, humidite_bande, vent_bande, pluie_classe, source_classe) DO UPDATE
    SET source_classe = EXCLUDED.source_classe
    RETURNING
        id_meteo_classe,
        temp_bande,
        humidite_bande,
        vent_bande,
        pluie_classe,
        source_classe
),
joined AS (
    SELECT
        t.id_temps,
        st.id_station,
        dp.id_polluant,
        mc.id_meteo_classe,
        n.parameter_code,
        n.valeur,
        n.unite,
        wc.temperature_c,
        wc.humidite_relative_pct,
        wc.vitesse_vent_10m_ms,
        wc.precipitation_mm,
        wc.rain_flag
    FROM normalized n
    JOIN public.dim_station st
        ON st.station_code = n.station_code
       AND st.is_active = true
    JOIN public.dim_temps t
        ON t.date_heure_utc = n.bucket_utc
    JOIN upsert_polluant dp
        ON dp.parameter_code = n.parameter_code
    JOIN weather_classified wc
        ON wc.station_code = n.station_code
       AND wc.bucket_utc = n.bucket_utc
    JOIN upsert_meteo mc
        ON mc.temp_bande = wc.temp_bande
       AND mc.humidite_bande = wc.humidite_bande
       AND mc.vent_bande = wc.vent_bande
       AND mc.pluie_classe = wc.pluie_classe
       AND mc.source_classe = 'Open-Meteo'
)
INSERT INTO public.fait_pollution_heure (
    id_temps,
    id_station,
    id_polluant,
    id_meteo_classe,
    valeur,
    unite,
    indice_aqi,
    seuil_oms_depasse,
    temperature_c,
    humidite_relative_pct,
    vitesse_vent_10m_ms,
    precipitation_mm,
    rain_flag
)
SELECT
    id_temps,
    id_station,
    id_polluant,
    id_meteo_classe,
    valeur,
    unite,
    CASE
        WHEN parameter_code = 'pm25' THEN LEAST(500, GREATEST(0, ROUND(valeur * 4)::int))
        WHEN parameter_code = 'pm10' THEN LEAST(500, GREATEST(0, ROUND(valeur * 2)::int))
        WHEN parameter_code = 'no2' THEN LEAST(500, GREATEST(0, ROUND(valeur * 1.5)::int))
        WHEN parameter_code = 'o3' THEN LEAST(500, GREATEST(0, ROUND(valeur * 1.2)::int))
        ELSE LEAST(500, GREATEST(0, ROUND(valeur)::int))
    END AS indice_aqi,
    CASE
        WHEN parameter_code = 'pm25' THEN valeur > 15
        WHEN parameter_code = 'pm10' THEN valeur > 45
        WHEN parameter_code = 'no2' THEN valeur > 25
        WHEN parameter_code = 'o3' THEN valeur > 100
        ELSE false
    END AS seuil_oms_depasse,
    temperature_c,
    humidite_relative_pct,
    vitesse_vent_10m_ms,
    precipitation_mm,
    rain_flag
FROM joined
ON CONFLICT (id_temps, id_station, id_polluant) DO UPDATE
SET id_meteo_classe = EXCLUDED.id_meteo_classe,
    valeur = EXCLUDED.valeur,
    unite = EXCLUDED.unite,
    indice_aqi = EXCLUDED.indice_aqi,
    seuil_oms_depasse = EXCLUDED.seuil_oms_depasse,
    temperature_c = EXCLUDED.temperature_c,
    humidite_relative_pct = EXCLUDED.humidite_relative_pct,
    vitesse_vent_10m_ms = EXCLUDED.vitesse_vent_10m_ms,
    precipitation_mm = EXCLUDED.precipitation_mm,
    rain_flag = EXCLUDED.rain_flag;
