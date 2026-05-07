ALTER TABLE public.stg_pollution_json_raw
    DROP COLUMN IF EXISTS datetime_from,
    DROP COLUMN IF EXISTS datetime_to;

ALTER TABLE public.stg_pollution_mesure
    DROP COLUMN IF EXISTS openaq_location_id,
    DROP COLUMN IF EXISTS sensor_id;

ALTER TABLE public.dim_station
    DROP COLUMN IF EXISTS openaq_location_id,
    DROP COLUMN IF EXISTS type_zone,
    DROP COLUMN IF EXISTS timezone_name;

ALTER TABLE public.fait_pollution_heure
    DROP COLUMN IF EXISTS charge_le_utc;

ALTER TABLE public.fait_indicateur_air_annuel
    DROP COLUMN IF EXISTS charge_le_utc;
