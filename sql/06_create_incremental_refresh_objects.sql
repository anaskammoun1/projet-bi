CREATE TABLE IF NOT EXISTS public.etl_refresh_log
(
    id_refresh bigserial PRIMARY KEY,
    refresh_type varchar(30) NOT NULL,
    status varchar(20) NOT NULL,
    started_at_utc timestamp without time zone NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    finished_at_utc timestamp without time zone,
    pollution_rows_before bigint,
    pollution_rows_after bigint,
    annual_indicator_rows_before bigint,
    annual_indicator_rows_after bigint,
    message text
);

ALTER TABLE public.fait_pollution_heure
    ADD COLUMN IF NOT EXISTS inserted_at_utc timestamp without time zone
    NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');

ALTER TABLE public.fait_indicateur_air_annuel
    ADD COLUMN IF NOT EXISTS inserted_at_utc timestamp without time zone
    NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');

CREATE OR REPLACE VIEW public.vw_etl_refresh_status AS
SELECT
    id_refresh,
    refresh_type,
    status,
    started_at_utc,
    finished_at_utc,
    pollution_rows_before,
    pollution_rows_after,
    pollution_rows_after - pollution_rows_before AS pollution_rows_added,
    annual_indicator_rows_before,
    annual_indicator_rows_after,
    annual_indicator_rows_after - annual_indicator_rows_before AS annual_indicator_rows_added,
    message
FROM public.etl_refresh_log;
