CREATE VIEW public.vw_correlation_sante_pollution AS
WITH pollution_annuelle AS (
    SELECT
        p.id_pays,
        p.pays_nom,
        t.annee,
        ROUND(AVG(f.pm25_ug_m3)::numeric, 2) AS pm25_moyen,
        ROUND(AVG(f.indice_aqi)::numeric, 0) AS aqi_moyen
    FROM fait_pollution_heure f
    JOIN dim_station st ON f.id_station = st.id_station
    JOIN dim_pays p     ON st.id_pays   = p.id_pays
    JOIN dim_temps t    ON f.id_temps   = t.id_temps
    GROUP BY
        p.id_pays,
        p.pays_nom,
        t.annee
)
SELECT
    pa.pays_nom,
    pa.annee,
    pa.pm25_moyen,
    pa.aqi_moyen,
    s.indicator_code,
    s.indicator_nom,
    s.indicator_valeur
FROM pollution_annuelle pa
JOIN fait_sante_annuelle s
    ON s.id_pays = pa.id_pays
   AND s.annee   = pa.annee
ORDER BY pa.pays_nom, pa.annee;


CREATE VIEW public.vw_pollution_annuelle_station AS

SELECT
    p.pays_nom,
    s.station_nom,
    t.annee,

    AVG(f.pm25_ug_m3) AS pm25_moyen,
    AVG(f.pm10_ug_m3) AS pm10_moyen,
    AVG(f.no2_ug_m3) AS no2_moyen,

    AVG(f.indice_aqi) AS aqi_moyen,
    SUM(CASE WHEN f.seuil_oms_depasse THEN 1 ELSE 0 END) AS nb_depassements

FROM fait_pollution_heure f

JOIN dim_station s
    ON f.id_station = s.id_station

JOIN dim_pays p
    ON s.id_pays = p.id_pays

JOIN dim_temps t
    ON f.id_temps = t.id_temps

GROUP BY
    p.pays_nom,
    s.station_nom,
    t.annee;
