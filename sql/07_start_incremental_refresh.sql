INSERT INTO public.etl_refresh_log (
    refresh_type,
    status,
    pollution_rows_before,
    annual_indicator_rows_before,
    message
)
SELECT
    'incremental',
    'running',
    (SELECT COUNT(*) FROM public.fait_pollution_heure),
    (SELECT COUNT(*) FROM public.fait_indicateur_air_annuel),
    'Incremental refresh started';
