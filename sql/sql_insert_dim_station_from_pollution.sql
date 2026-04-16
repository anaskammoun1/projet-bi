-- Upsert shared country and station dimensions from OpenAQ raw location JSON.
-- Source: public.stg_pollution_json_raw
-- Targets: public.dim_pays, public.dim_station

WITH raw_location AS (
    SELECT DISTINCT ON (r.station_code)
        r.pays_code,
        r.pays_nom,
        r.station_code,
        COALESCE(NULLIF(result_item ->> 'name', ''), r.station_code) AS station_nom,
        COALESCE(
            NULLIF(result_item ->> 'locality', ''),
            NULLIF(result_item ->> 'city', ''),
            NULLIF(r.city, ''),
            'Unknown'
        ) AS ville,
        COALESCE(
            NULLIF(result_item -> 'coordinates' ->> 'latitude', ''),
            NULLIF(result_item ->> 'latitude', '')
        )::numeric(9,6) AS latitude,
        COALESCE(
            NULLIF(result_item -> 'coordinates' ->> 'longitude', ''),
            NULLIF(result_item ->> 'longitude', '')
        )::numeric(9,6) AS longitude,
        NULLIF(
            COALESCE(
                NULLIF(result_item ->> 'elevation', ''),
                NULLIF(result_item ->> 'elevation_m', '')
            ),
            ''
        )::numeric::smallint AS altitude_m,
        LEFT(
            COALESCE(
                NULLIF(result_item ->> 'locationType', ''),
                NULLIF(result_item ->> 'locality', ''),
                'unknown'
            ),
            30
        ) AS type_zone,
        LEFT(
            COALESCE(
                NULLIF(result_item ->> 'timezone', ''),
                NULLIF(result_item ->> 'timezoneName', ''),
                'UTC'
            ),
            50
        ) AS timezone_name
FROM public.stg_pollution_json_raw r
CROSS JOIN LATERAL jsonb_array_elements(
    CASE
        WHEN jsonb_typeof(r.api_result::jsonb -> 'results') = 'array'
            THEN r.api_result::jsonb -> 'results'
        ELSE jsonb_build_array(r.api_result::jsonb)
    END
) AS result_item
    WHERE r.api_result IS NOT NULL
    ORDER BY
        r.station_code,
        CASE
            WHEN COALESCE(result_item ->> 'id', '') ~ '^[0-9]+$'
             AND (result_item ->> 'id')::integer = r.openaq_location_id THEN 0
            ELSE 1
        END
),
upsert_pays AS (
    INSERT INTO public.dim_pays (pays_code, pays_nom, continent)
    SELECT DISTINCT
        rl.pays_code,
        rl.pays_nom,
        NULL::varchar(50) AS continent
    FROM raw_location rl
    WHERE rl.pays_code IS NOT NULL
      AND rl.pays_nom IS NOT NULL
    ON CONFLICT (pays_code) DO UPDATE
    SET pays_nom = EXCLUDED.pays_nom,
        continent = COALESCE(public.dim_pays.continent, EXCLUDED.continent)
    RETURNING pays_code
)
INSERT INTO public.dim_station (
    station_code,
    station_nom,
    ville,
    latitude,
    longitude,
    altitude_m,
    type_zone,
    timezone_name,
    id_pays
)
SELECT
    rl.station_code,
    rl.station_nom,
    rl.ville,
    rl.latitude,
    rl.longitude,
    rl.altitude_m,
    rl.type_zone,
    rl.timezone_name,
    p.id_pays
FROM raw_location rl
JOIN public.dim_pays p
    ON p.pays_code = rl.pays_code
WHERE rl.station_code IS NOT NULL
  AND rl.latitude IS NOT NULL
  AND rl.longitude IS NOT NULL
ON CONFLICT (station_code) DO UPDATE
SET station_nom = EXCLUDED.station_nom,
    ville = EXCLUDED.ville,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    altitude_m = EXCLUDED.altitude_m,
    type_zone = EXCLUDED.type_zone,
    timezone_name = EXCLUDED.timezone_name,
    id_pays = EXCLUDED.id_pays;
