DELETE FROM public.stg_pm25_csv_raw
WHERE code IS NULL
   OR code !~ '^[A-Z]{3}$'
   OR year_value !~ '^[0-9]{4}$'
   OR pm25_exposure_ug_m3 !~ '^-?[0-9]+(\.[0-9]+)?$';

DELETE FROM public.stg_air_death_csv_raw
WHERE code IS NULL
   OR code !~ '^[A-Z]{3}$'
   OR year_value !~ '^[0-9]{4}$'
   OR death_rate_per_100k !~ '^-?[0-9]+(\.[0-9]+)?$';

INSERT INTO public.fait_indicateur_air_annuel (
    id_pays,
    annee,
    indicator_code,
    indicator_nom,
    indicator_valeur,
    unite,
    source_fichier
)
SELECT
    p.id_pays,
    s.year_value::smallint,
    'PM25_EXPOSURE',
    'Population-weighted PM2.5 exposure',
    s.pm25_exposure_ug_m3::numeric(14,6),
    'ug/m3',
    'pm25-air-pollution.csv'
FROM public.stg_pm25_csv_raw s
JOIN public.dim_pays p
    ON p.pays_code = s.code
   AND p.source_active_station = true
WHERE s.code ~ '^[A-Z]{3}$'
  AND s.year_value ~ '^[0-9]{4}$'
  AND s.pm25_exposure_ug_m3 ~ '^-?[0-9]+(\.[0-9]+)?$'
ON CONFLICT (id_pays, annee, indicator_code) DO UPDATE
SET indicator_nom = EXCLUDED.indicator_nom,
    indicator_valeur = EXCLUDED.indicator_valeur,
    unite = EXCLUDED.unite,
    source_fichier = EXCLUDED.source_fichier;

INSERT INTO public.fait_indicateur_air_annuel (
    id_pays,
    annee,
    indicator_code,
    indicator_nom,
    indicator_valeur,
    unite,
    source_fichier
)
SELECT
    p.id_pays,
    s.year_value::smallint,
    'AIR_POLLUTION_DEATH_RATE',
    'Age-standardized mortality rate attributed to household and ambient air pollution',
    s.death_rate_per_100k::numeric(14,6),
    'deaths per 100,000 population',
    'death-rate-household-and-ambient-air-pollution.csv'
FROM public.stg_air_death_csv_raw s
JOIN public.dim_pays p
    ON p.pays_code = s.code
   AND p.source_active_station = true
WHERE s.code ~ '^[A-Z]{3}$'
  AND s.year_value ~ '^[0-9]{4}$'
  AND s.death_rate_per_100k ~ '^-?[0-9]+(\.[0-9]+)?$'
ON CONFLICT (id_pays, annee, indicator_code) DO UPDATE
SET indicator_nom = EXCLUDED.indicator_nom,
    indicator_valeur = EXCLUDED.indicator_valeur,
    unite = EXCLUDED.unite,
    source_fichier = EXCLUDED.source_fichier;
