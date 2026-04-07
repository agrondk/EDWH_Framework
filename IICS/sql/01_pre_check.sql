-- ============================================================
-- IICS SQL Task: T1_PRE_CHECK
-- Purpose : Verify source tables are accessible before the run
--           starts and that no stuck (open) ETL_RUN_LOG entry
--           exists for today's date for either entity.
--           Returns BLOCKED_COUNT = 0 if safe to proceed.
--           Returns BLOCKED_COUNT > 0 to route to T8 failure.
-- ============================================================

SELECT
    -- Count any open runs (started today but never ended)
    (
        SELECT COUNT(*)
        FROM   GPC_DM.ETL_RUN_LOG r
        JOIN   GPC_DM.ETL_ENTITY  e ON e.ENTITY_ID = r.ENTITY_ID
        WHERE  e.ENTITY_NAME IN ('STAFFING_SCHEDULE', 'COST')
        AND    TRUNC(r.START_TIME) = TRUNC(SYSDATE)
        AND    r.END_TIME IS NULL
    )                                               AS BLOCKED_COUNT,

    -- Verify source tables are reachable (returns 1 if OK, ORA-error if not)
    (SELECT 1 FROM KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB WHERE ROWNUM = 1) AS STAFFING_SOURCE_OK,
    (SELECT 1 FROM KBR_IHUB.APAC_PCDM_COST_IHUB               WHERE ROWNUM = 1) AS COST_SOURCE_OK,

    TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS')       AS CHECK_TIME

FROM DUAL
