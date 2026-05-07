ALTER TABLE public.fait_pollution_heure
    ADD COLUMN IF NOT EXISTS inserted_at_utc timestamp without time zone
    NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');

ALTER TABLE public.fait_indicateur_air_annuel
    ADD COLUMN IF NOT EXISTS inserted_at_utc timestamp without time zone
    NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC');

COMMENT ON COLUMN public.fait_pollution_heure.inserted_at_utc
IS 'UTC timestamp when the fact row was first inserted into the warehouse.';

COMMENT ON COLUMN public.fait_indicateur_air_annuel.inserted_at_utc
IS 'UTC timestamp when the annual indicator row was first inserted into the warehouse.';
