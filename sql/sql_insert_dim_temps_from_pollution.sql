INSERT INTO public.dim_temps (
    date_heure_utc,
    date_locale,
    annee,
    trimestre,
    mois,
    semaine,
    jour,
    heure,
    jour_semaine,
    is_weekend,
    saison
)
SELECT DISTINCT
    bucket_utc AS date_heure_utc,
    bucket_utc::date AS date_locale,
    EXTRACT(YEAR FROM bucket_utc)::smallint AS annee,
    EXTRACT(QUARTER FROM bucket_utc)::smallint AS trimestre,
    EXTRACT(MONTH FROM bucket_utc)::smallint AS mois,
    EXTRACT(WEEK FROM bucket_utc)::smallint AS semaine,
    EXTRACT(DAY FROM bucket_utc)::smallint AS jour,
    EXTRACT(HOUR FROM bucket_utc)::smallint AS heure,
    TRIM(TO_CHAR(bucket_utc, 'Day'))::varchar(10) AS jour_semaine,
    (EXTRACT(ISODOW FROM bucket_utc) IN (6, 7)) AS is_weekend,
    CASE
        WHEN EXTRACT(MONTH FROM bucket_utc) IN (12, 1, 2) THEN 'Hiver'
        WHEN EXTRACT(MONTH FROM bucket_utc) IN (3, 4, 5) THEN 'Printemps'
        WHEN EXTRACT(MONTH FROM bucket_utc) IN (6, 7, 8) THEN 'Ete'
        ELSE 'Automne'
    END AS saison
FROM (
    SELECT date_trunc('hour', pm.date_heure_utc) AS bucket_utc
    FROM public.stg_pollution_mesure pm
    UNION
    SELECT (json_array_elements_text((api_result::json -> 'hourly' -> 'time')))::timestamp AS bucket_utc
    FROM public.stg_meteo_json_raw
) x
WHERE bucket_utc IS NOT NULL
ON CONFLICT (date_heure_utc) DO NOTHING;
