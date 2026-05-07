DROP VIEW IF EXISTS public.vw_correlation_sante_pollution;
DROP VIEW IF EXISTS public.vw_pollution_annuelle_station;
DROP VIEW IF EXISTS public.vw_active_station_countries;
DROP VIEW IF EXISTS public.vw_air_indicators_active_countries;

DROP TABLE IF EXISTS public.fait_indicateur_air_annuel;
DROP TABLE IF EXISTS public.fait_pollution_heure;
DROP TABLE IF EXISTS public.dim_meteo_classe;
DROP TABLE IF EXISTS public.dim_polluant;
DROP TABLE IF EXISTS public.dim_temps;
DROP TABLE IF EXISTS public.dim_station;
DROP TABLE IF EXISTS public.dim_pays;

DROP TABLE IF EXISTS public.stg_air_death_csv_raw;
DROP TABLE IF EXISTS public.stg_pm25_csv_raw;
DROP TABLE IF EXISTS public.stg_pollution_mesure;
DROP TABLE IF EXISTS public.stg_sensor_measurements_raw;
DROP TABLE IF EXISTS public.stg_pollution_json_raw;
DROP TABLE IF EXISTS public.stg_meteo_json_raw;
