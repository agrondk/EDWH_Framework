-- ============================================================
-- IICS SQL Task: T4_CAPTURE_RUN_LOG
-- Purpose : Read the latest run entries from ETL_RUN_LOG
--           for today's execution of both entities.
--           Populates taskflow variables used in email
--           notifications.
--
-- Output columns map to IICS OUTPUTPARAM definitions:
--   RUN_DATE               → $$v_run_date
--   STAFFING_RUN_ID        → $$v_staffing_run_id
--   COST_RUN_ID            → $$v_cost_run_id
--   STAFFING_STATUS        → $$v_staffing_status
--   COST_STATUS            → $$v_cost_status
--   STAFFING_ROWS_INSERTED → $$v_staffing_rows_inserted
--   COST_ROWS_INSERTED     → $$v_cost_rows_inserted
--
-- Uses view: GPC_DM.V_IICS_RUN_SUMMARY
--   (created by ddl/07_views.sql)
-- ============================================================

SELECT
    TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS')                               AS RUN_DATE,

    MAX(CASE WHEN ENTITY_NAME = 'STAFFING_SCHEDULE' THEN RUN_ID        END)  AS STAFFING_RUN_ID,
    MAX(CASE WHEN ENTITY_NAME = 'COST'              THEN RUN_ID        END)  AS COST_RUN_ID,

    MAX(CASE WHEN ENTITY_NAME = 'STAFFING_SCHEDULE' THEN STATUS        END)  AS STAFFING_STATUS,
    MAX(CASE WHEN ENTITY_NAME = 'COST'              THEN STATUS        END)  AS COST_STATUS,

    NVL(MAX(CASE WHEN ENTITY_NAME = 'STAFFING_SCHEDULE' THEN ROWS_INSERTED END), 0) AS STAFFING_ROWS_INSERTED,
    NVL(MAX(CASE WHEN ENTITY_NAME = 'COST'              THEN ROWS_INSERTED END), 0) AS COST_ROWS_INSERTED

FROM
    GPC_DM.V_IICS_RUN_SUMMARY
WHERE
    TRUNC(START_TIME) = TRUNC(SYSDATE)
