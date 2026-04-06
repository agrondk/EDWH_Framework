-- ============================================================
-- IICS SQL Task: T5_CAPTURE_ERRORS
-- Purpose : Read ETL_ERROR_LOG for any errors raised during
--           today's run of STAFFING_SCHEDULE and COST.
--           Always executes — reached from both success and
--           failure paths (ON_SUCCESS and ON_FAILURE of T2/T3).
--
-- Output columns map to IICS OUTPUTPARAM definitions:
--   ERROR_COUNT   → $$v_error_count
--   ERROR_SUMMARY → $$v_error_summary
--                   (truncated to 4000 chars for email body)
--
-- Uses view: GPC_DM.V_IICS_ERROR_SUMMARY
--   (created by ddl/07_views.sql — see IICS views section)
-- ============================================================

SELECT
    COUNT(*)                                                    AS ERROR_COUNT,

    -- Concatenate top 10 error messages for the notification email
    SUBSTR(
        LISTAGG(
            '[' || TO_CHAR(ERROR_TIME,'HH24:MI:SS') || '] '
            || ENTITY_NAME || ' | '
            || ERROR_CODE  || ' | '
            || SUBSTR(ERROR_MESSAGE, 1, 200),
            CHR(10)
        ) WITHIN GROUP (ORDER BY ERROR_TIME),
        1, 4000
    )                                                           AS ERROR_SUMMARY

FROM (
    -- Limit to top 10 most recent errors to keep email readable
    SELECT *
    FROM   GPC_DM.V_IICS_ERROR_SUMMARY
    WHERE  TRUNC(ERROR_TIME) = TRUNC(SYSDATE)
    ORDER  BY ERROR_TIME DESC
    FETCH FIRST 10 ROWS ONLY
)
