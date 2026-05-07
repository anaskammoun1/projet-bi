CREATE TABLE IF NOT EXISTS public.stg_meteo_json_raw (
    station_code character varying(50) NOT NULL,
    latitude numeric(9,6) NOT NULL,
    longitude numeric(9,6) NOT NULL,
    start_date character varying(10) NOT NULL,
    end_date character varying(10) NOT NULL,
    api_result text NOT NULL
);
