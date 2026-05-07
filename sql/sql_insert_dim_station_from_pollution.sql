-- Load only OpenAQ stations with recent measurements.
-- A station is treated as active when datetimeLast is within the last 180 days.

WITH parsed AS (
    SELECT DISTINCT ON (r.station_code)
        r.pays_code,
        r.pays_nom,
        r.station_code,
        r.openaq_location_id,
        COALESCE(NULLIF(item ->> 'name', ''), r.station_code) AS station_nom,
        COALESCE(
            NULLIF(item ->> 'locality', ''),
            NULLIF(item ->> 'city', ''),
            NULLIF(r.city, ''),
            'Unknown'
        ) AS ville,
        COALESCE(
            NULLIF(item -> 'coordinates' ->> 'latitude', ''),
            NULLIF(item ->> 'latitude', '')
        )::numeric(9,6) AS latitude,
        COALESCE(
            NULLIF(item -> 'coordinates' ->> 'longitude', ''),
            NULLIF(item ->> 'longitude', '')
        )::numeric(9,6) AS longitude,
        LEFT(COALESCE(NULLIF(item ->> 'locationType', ''), 'monitor'), 30) AS type_zone,
        LEFT(COALESCE(NULLIF(item ->> 'timezone', ''), 'UTC'), 50) AS timezone_name,
        (item -> 'datetimeFirst' ->> 'utc')::timestamptz AT TIME ZONE 'UTC' AS datetime_first_utc,
        (item -> 'datetimeLast' ->> 'utc')::timestamptz AT TIME ZONE 'UTC' AS datetime_last_utc
    FROM public.stg_pollution_json_raw r
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(r.api_result::jsonb -> 'results') = 'array'
                THEN r.api_result::jsonb -> 'results'
            ELSE jsonb_build_array(r.api_result::jsonb)
        END
    ) AS item
    WHERE r.api_result IS NOT NULL
      AND r.pays_code ~ '^[A-Z]{3}$'
      AND item -> 'coordinates' ->> 'latitude' IS NOT NULL
      AND item -> 'coordinates' ->> 'longitude' IS NOT NULL
      AND item -> 'datetimeFirst' ->> 'utc' IS NOT NULL
      AND item -> 'datetimeLast' ->> 'utc' IS NOT NULL
    ORDER BY
        r.station_code,
        CASE
            WHEN COALESCE(item ->> 'id', '') ~ '^[0-9]+$'
             AND (item ->> 'id')::integer = r.openaq_location_id THEN 0
            ELSE 1
        END
),
active_station AS (
    SELECT
        *,
        (datetime_last_utc >= (CURRENT_DATE - INTERVAL '180 days')) AS is_active
    FROM parsed
    WHERE datetime_last_utc >= (CURRENT_DATE - INTERVAL '180 days')
),
upsert_pays AS (
    INSERT INTO public.dim_pays (
        pays_code,
        pays_nom,
        continent,
        source_active_station
    )
    SELECT DISTINCT
        pays_code,
        pays_nom,
        CASE
            WHEN pays_code IN ('FRA', 'GBR', 'DEU', 'ESP') THEN 'Europe'
            WHEN pays_code IN ('SEN', 'EGY') THEN 'Africa'
            WHEN pays_code IN ('IND', 'CHN') THEN 'Asia'
            ELSE 'Other'
        END,
        true
    FROM active_station
    ON CONFLICT (pays_code) DO UPDATE
    SET pays_nom = EXCLUDED.pays_nom,
        continent = EXCLUDED.continent,
        source_active_station = true
    RETURNING id_pays, pays_code
)
INSERT INTO public.dim_station (
    station_code,
    openaq_location_id,
    station_nom,
    ville,
    latitude,
    longitude,
    type_zone,
    timezone_name,
    datetime_first_utc,
    datetime_last_utc,
    is_active,
    id_pays
)
SELECT
    a.station_code,
    a.openaq_location_id,
    a.station_nom,
    a.ville,
    a.latitude,
    a.longitude,
    a.type_zone,
    a.timezone_name,
    a.datetime_first_utc,
    a.datetime_last_utc,
    a.is_active,
    p.id_pays
FROM active_station a
JOIN upsert_pays p
    ON p.pays_code = a.pays_code
ON CONFLICT (station_code) DO UPDATE
SET openaq_location_id = EXCLUDED.openaq_location_id,
    station_nom = EXCLUDED.station_nom,
    ville = EXCLUDED.ville,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    type_zone = EXCLUDED.type_zone,
    timezone_name = EXCLUDED.timezone_name,
    datetime_first_utc = EXCLUDED.datetime_first_utc,
    datetime_last_utc = EXCLUDED.datetime_last_utc,
    is_active = EXCLUDED.is_active,
    id_pays = EXCLUDED.id_pays;
