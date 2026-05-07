BEGIN;

CREATE TABLE IF NOT EXISTS public.dim_pays
(
    id_pays serial PRIMARY KEY,
    pays_code char(3) NOT NULL UNIQUE,
    pays_nom varchar(100) NOT NULL,
    continent varchar(50) NOT NULL DEFAULT 'Unknown',
    source_active_station boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS public.dim_station
(
    id_station serial PRIMARY KEY,
    station_code varchar(50) NOT NULL UNIQUE,
    openaq_location_id integer NOT NULL UNIQUE,
    station_nom varchar(150) NOT NULL,
    ville varchar(100) NOT NULL,
    latitude numeric(9,6) NOT NULL,
    longitude numeric(9,6) NOT NULL,
    type_zone varchar(30) NOT NULL DEFAULT 'monitor',
    timezone_name varchar(50) NOT NULL DEFAULT 'UTC',
    datetime_first_utc timestamp without time zone NOT NULL,
    datetime_last_utc timestamp without time zone NOT NULL,
    is_active boolean NOT NULL,
    id_pays integer NOT NULL REFERENCES public.dim_pays(id_pays)
);

CREATE TABLE IF NOT EXISTS public.dim_temps
(
    id_temps serial PRIMARY KEY,
    date_heure_utc timestamp without time zone NOT NULL UNIQUE,
    date_locale date NOT NULL,
    annee smallint NOT NULL,
    trimestre smallint NOT NULL,
    mois smallint NOT NULL,
    semaine smallint NOT NULL,
    jour smallint NOT NULL,
    heure smallint NOT NULL,
    jour_semaine varchar(10) NOT NULL,
    is_weekend boolean NOT NULL,
    saison varchar(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.dim_polluant
(
    id_polluant serial PRIMARY KEY,
    parameter_code varchar(50) NOT NULL UNIQUE,
    parameter_nom varchar(100) NOT NULL,
    unite_standard varchar(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.dim_meteo_classe
(
    id_meteo_classe serial PRIMARY KEY,
    temp_bande varchar(20) NOT NULL,
    humidite_bande varchar(20) NOT NULL,
    vent_bande varchar(20) NOT NULL,
    pluie_classe varchar(20) NOT NULL,
    source_classe varchar(30) NOT NULL,
    CONSTRAINT dim_meteo_classe_unique UNIQUE
        (temp_bande, humidite_bande, vent_bande, pluie_classe, source_classe)
);

CREATE TABLE IF NOT EXISTS public.fait_pollution_heure
(
    id_fait bigserial PRIMARY KEY,
    id_temps integer NOT NULL REFERENCES public.dim_temps(id_temps),
    id_station integer NOT NULL REFERENCES public.dim_station(id_station),
    id_polluant integer NOT NULL REFERENCES public.dim_polluant(id_polluant),
    id_meteo_classe integer NOT NULL REFERENCES public.dim_meteo_classe(id_meteo_classe),
    valeur numeric(12,4) NOT NULL,
    unite varchar(30) NOT NULL,
    indice_aqi smallint NOT NULL,
    seuil_oms_depasse boolean NOT NULL,
    temperature_c numeric(6,2) NOT NULL,
    humidite_relative_pct numeric(6,2) NOT NULL,
    vitesse_vent_10m_ms numeric(6,2) NOT NULL,
    precipitation_mm numeric(6,2) NOT NULL DEFAULT 0,
    rain_flag boolean NOT NULL DEFAULT false,
    inserted_at_utc timestamp without time zone NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    CONSTRAINT fait_pollution_heure_unique UNIQUE (id_temps, id_station, id_polluant)
);

CREATE TABLE IF NOT EXISTS public.fait_indicateur_air_annuel
(
    id_fait bigserial PRIMARY KEY,
    id_pays integer NOT NULL REFERENCES public.dim_pays(id_pays),
    annee smallint NOT NULL,
    indicator_code varchar(50) NOT NULL,
    indicator_nom varchar(180) NOT NULL,
    indicator_valeur numeric(14,6) NOT NULL,
    unite varchar(80) NOT NULL,
    source_fichier varchar(120) NOT NULL,
    inserted_at_utc timestamp without time zone NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    CONSTRAINT fait_indicateur_air_annuel_unique UNIQUE
        (id_pays, annee, indicator_code)
);

CREATE INDEX IF NOT EXISTS idx_station_pays
    ON public.dim_station(id_pays);

CREATE INDEX IF NOT EXISTS idx_station_active
    ON public.dim_station(is_active, datetime_last_utc);

CREATE INDEX IF NOT EXISTS idx_pollution_station_temps
    ON public.fait_pollution_heure(id_station, id_temps);

CREATE INDEX IF NOT EXISTS idx_pollution_polluant
    ON public.fait_pollution_heure(id_polluant);

CREATE INDEX IF NOT EXISTS idx_indicateur_pays_annee
    ON public.fait_indicateur_air_annuel(id_pays, annee);

COMMIT;
