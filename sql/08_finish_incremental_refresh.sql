WITH latest_running AS (
    SELECT id_refresh
    FROM public.etl_refresh_log
    WHERE refresh_type = 'incremental'
      AND status = 'running'
    ORDER BY started_at_utc DESC
    LIMIT 1
)
UPDATE public.etl_refresh_log l
SET status = 'success',
    finished_at_utc = CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
    pollution_rows_after = (SELECT COUNT(*) FROM public.fait_pollution_heure),
    annual_indicator_rows_after = (SELECT COUNT(*) FROM public.fait_indicateur_air_annuel),
    message = 'Incremental refresh finished successfully'
FROM latest_running r
WHERE l.id_refresh = r.id_refresh;
