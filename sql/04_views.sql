CREATE OR REPLACE VIEW public.vw_active_station_countries AS
SELECT
    p.pays_code,
    p.pays_nom,
    COUNT(*) AS active_station_count,
    MAX(s.datetime_last_utc) AS latest_station_measurement_utc
FROM public.dim_pays p
JOIN public.dim_station s
    ON s.id_pays = p.id_pays
WHERE s.is_active = true
GROUP BY
    p.pays_code,
    p.pays_nom;

CREATE OR REPLACE VIEW public.vw_pollution_annuelle_station AS
SELECT
    p.pays_nom,
    s.station_code,
    s.station_nom,
    t.annee,
    dp.parameter_code,
    dp.parameter_nom,
    AVG(f.valeur) AS valeur_moyenne,
    AVG(f.indice_aqi) AS aqi_moyen,
    SUM(CASE WHEN f.seuil_oms_depasse THEN 1 ELSE 0 END) AS nb_depassements,
    AVG(f.temperature_c) AS temperature_moyenne_c,
    AVG(f.humidite_relative_pct) AS humidite_moyenne_pct,
    MAX(f.inserted_at_utc) AS latest_inserted_at_utc
FROM public.fait_pollution_heure f
JOIN public.dim_station s
    ON f.id_station = s.id_station
JOIN public.dim_pays p
    ON s.id_pays = p.id_pays
JOIN public.dim_temps t
    ON f.id_temps = t.id_temps
JOIN public.dim_polluant dp
    ON f.id_polluant = dp.id_polluant
GROUP BY
    p.pays_nom,
    s.station_code,
    s.station_nom,
    t.annee,
    dp.parameter_code,
    dp.parameter_nom;

CREATE OR REPLACE VIEW public.vw_air_indicators_active_countries AS
SELECT
    p.pays_code,
    p.pays_nom,
    f.annee,
    f.indicator_code,
    f.indicator_nom,
    f.indicator_valeur,
    f.unite,
    f.source_fichier,
    f.inserted_at_utc
FROM public.fait_indicateur_air_annuel f
JOIN public.dim_pays p
    ON p.id_pays = f.id_pays
WHERE p.source_active_station = true;

DROP VIEW IF EXISTS public.vw_correlation_sante_pollution;

CREATE VIEW public.vw_correlation_sante_pollution AS
WITH annual_pollution AS (
    SELECT
        p.id_pays,
        p.pays_code,
        p.pays_nom,
        t.annee,
        AVG(f.valeur) FILTER (WHERE dp.parameter_code = 'pm25') AS station_pm25_moyen,
        AVG(f.indice_aqi) FILTER (WHERE dp.parameter_code = 'pm25') AS station_pm25_aqi_moyen,
        COUNT(*) FILTER (WHERE dp.parameter_code = 'pm25') AS station_pm25_nb_mesures,
        MAX(f.inserted_at_utc) FILTER (WHERE dp.parameter_code = 'pm25') AS latest_inserted_at_utc
    FROM public.fait_pollution_heure f
    JOIN public.dim_station st
        ON f.id_station = st.id_station
    JOIN public.dim_pays p
        ON st.id_pays = p.id_pays
    JOIN public.dim_temps t
        ON f.id_temps = t.id_temps
    JOIN public.dim_polluant dp
        ON f.id_polluant = dp.id_polluant
    GROUP BY
        p.id_pays,
        p.pays_code,
        p.pays_nom,
        t.annee
),
csv_pm25 AS (
    SELECT
        id_pays,
        annee,
        indicator_valeur AS country_pm25_exposure
    FROM public.fait_indicateur_air_annuel
    WHERE indicator_code = 'PM25_EXPOSURE'
)
SELECT
    ap.pays_code,
    ap.pays_nom,
    ap.annee,
    ap.station_pm25_moyen,
    ap.station_pm25_aqi_moyen,
    ap.station_pm25_nb_mesures,
    cp.country_pm25_exposure,
    ap.station_pm25_moyen - cp.country_pm25_exposure AS ecart_station_vs_pays_pm25,
    ap.latest_inserted_at_utc
FROM annual_pollution ap
JOIN csv_pm25 cp
    ON cp.id_pays = ap.id_pays
   AND cp.annee = ap.annee
WHERE ap.station_pm25_moyen IS NOT NULL;
