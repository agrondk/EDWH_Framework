-- ============================================================
-- IICS SQL Task: T1_PRE_CHECK
-- Purpose : Verify both ETL entities are in a runnable state
--           (IDLE or FAILED) before the run starts.
--           Returns BLOCKED_COUNT = 0 if safe to proceed.
--           Returns BLOCKED_COUNT > 0 if any entity is
--           RUNNING or DISABLED → taskflow routes to failure.
-- ============================================================

SELECT
    SUM(CASE WHEN c.STATUS IN ('RUNNING','DISABLED') THEN 1 ELSE 0 END) AS BLOCKED_COUNT,
    MAX(CASE WHEN e.ENTITY_NAME = 'STAFFING_SCHEDULE' THEN c.STATUS END) AS STAFFING_STATUS,
    MAX(CASE WHEN e.ENTITY_NAME = 'COST'              THEN c.STATUS END) AS COST_STATUS,
    TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS')                            AS CHECK_TIME
FROM
    GPC_DM.ETL_CONTROL c
    JOIN GPC_DM.ETL_ENTITY e ON e.ENTITY_ID = c.ENTITY_ID
WHERE
    e.ENTITY_NAME IN ('STAFFING_SCHEDULE', 'COST')
