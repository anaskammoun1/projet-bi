-- Insert hourly time dimensions derived from pollution timestamps.
-- Source: public.stg_pollution_mesure
-- Target: public.dim_temps

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
    x.bucket_utc AS date_heure_utc,
    x.bucket_utc::date AS date_locale,
    EXTRACT(YEAR FROM x.bucket_utc)::smallint AS annee,
    EXTRACT(QUARTER FROM x.bucket_utc)::smallint AS trimestre,
    EXTRACT(MONTH FROM x.bucket_utc)::smallint AS mois,
    EXTRACT(WEEK FROM x.bucket_utc)::smallint AS semaine,
    EXTRACT(DAY FROM x.bucket_utc)::smallint AS jour,
    EXTRACT(HOUR FROM x.bucket_utc)::smallint AS heure,
    TRIM(TO_CHAR(x.bucket_utc, 'Day'))::varchar(10) AS jour_semaine,
    (EXTRACT(ISODOW FROM x.bucket_utc) IN (6, 7)) AS is_weekend,
    CASE
        WHEN EXTRACT(MONTH FROM x.bucket_utc) IN (12, 1, 2) THEN 'Hiver'
        WHEN EXTRACT(MONTH FROM x.bucket_utc) IN (3, 4, 5) THEN 'Printemps'
        WHEN EXTRACT(MONTH FROM x.bucket_utc) IN (6, 7, 8) THEN 'Ete'
        ELSE 'Automne'
    END AS saison
FROM (
    SELECT DISTINCT date_trunc('hour', pm.date_heure_utc) AS bucket_utc
    FROM public.stg_pollution_mesure pm
    WHERE pm.date_heure_utc IS NOT NULL
) AS x
ON CONFLICT (date_heure_utc) DO NOTHING;
