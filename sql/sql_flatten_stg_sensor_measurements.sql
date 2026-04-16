-- Flatten OpenAQ v3 sensor measurements into the staging table used by the
-- pollution fact load.
-- Source: public.stg_sensor_measurements_raw
-- Target: public.stg_pollution_mesure
-- Output columns:
--   station_code, openaq_location_id, sensor_id, parameter_code,
--   date_heure_utc, valeur, unite

CREATE TABLE IF NOT EXISTS public.stg_pollution_mesure
(
    station_code        varchar(50) NOT NULL,
    openaq_location_id  integer NOT NULL,
    sensor_id           integer NOT NULL,
    parameter_code      varchar(50) NOT NULL,
    date_heure_utc      timestamp without time zone NOT NULL,
    valeur              numeric(12,4),
    unite               varchar(30)
);

TRUNCATE TABLE public.stg_pollution_mesure;

INSERT INTO public.stg_pollution_mesure (
    station_code,
    openaq_location_id,
    sensor_id,
    parameter_code,
    date_heure_utc,
    valeur,
    unite
)
SELECT DISTINCT
    r.station_code,
    r.openaq_location_id,
    r.sensor_id,
    LOWER(COALESCE(
        m.elem -> 'parameter' ->> 'name',
        m.elem ->> 'parameter',
        r.parameter_code
    )) AS parameter_code,
    COALESCE(
        (NULLIF(m.elem -> 'period' -> 'datetimeFrom' ->> 'utc', ''))::timestamptz AT TIME ZONE 'UTC',
        (NULLIF(m.elem -> 'datetimeFrom' ->> 'utc', ''))::timestamptz AT TIME ZONE 'UTC',
        (NULLIF(m.elem -> 'period' -> 'datetimeTo' ->> 'utc', ''))::timestamptz AT TIME ZONE 'UTC',
        (NULLIF(m.elem -> 'datetimeTo' ->> 'utc', ''))::timestamptz AT TIME ZONE 'UTC'
    ) AS date_heure_utc,
    NULLIF(m.elem ->> 'value', '')::numeric(12,4) AS valeur,
    COALESCE(
        NULLIF(m.elem -> 'parameter' ->> 'units', ''),
        NULLIF(m.elem ->> 'unit', '')
    ) AS unite
FROM public.stg_sensor_measurements_raw r
CROSS JOIN LATERAL jsonb_array_elements(
    COALESCE(r.api_result::jsonb -> 'results', '[]'::jsonb)
) AS m(elem)
WHERE r.api_result IS NOT NULL
  AND COALESCE(
      m.elem -> 'period' -> 'datetimeFrom' ->> 'utc',
      m.elem -> 'datetimeFrom' ->> 'utc',
      m.elem -> 'period' -> 'datetimeTo' ->> 'utc',
      m.elem -> 'datetimeTo' ->> 'utc'
  ) IS NOT NULL;
