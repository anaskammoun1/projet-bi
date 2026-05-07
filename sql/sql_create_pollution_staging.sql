DROP TABLE IF EXISTS public.stg_pollution_json_raw;
CREATE TABLE public.stg_pollution_json_raw (
    pays_code varchar(3) NOT NULL,
    pays_nom varchar(100) NOT NULL,
    station_code varchar(50) NOT NULL,
    city varchar(100) NOT NULL,
    openaq_location_id integer NOT NULL,
    api_result text NOT NULL
);

DROP TABLE IF EXISTS public.stg_sensor_measurements_raw;
CREATE TABLE public.stg_sensor_measurements_raw (
    station_code varchar(50) NOT NULL,
    openaq_location_id integer NOT NULL,
    sensor_id integer NOT NULL,
    parameter_code varchar(50) NOT NULL,
    api_result text NOT NULL
);
