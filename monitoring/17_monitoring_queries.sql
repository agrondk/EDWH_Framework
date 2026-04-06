-- ============================================================
-- FILE: 17_monitoring_queries.sql
-- DESC: Operational monitoring queries for the EDWH framework
-- SCHEMA: GPC_DM
-- ============================================================


-- ================================================================
-- 1. CURRENT ENTITY STATUS
--    Shows all entities, their watermark, and run state.
-- ================================================================
SELECT
    e.ENTITY_NAME,
    c.STATUS,
    c.LAST_WATERMARK,
    c.LAST_RUN_DATE,
    c.LAST_RUN_ID,
    c.NOTES
FROM   GPC_DM.ETL_CONTROL  c
JOIN   GPC_DM.ETL_ENTITY    e ON e.ENTITY_ID = c.ENTITY_ID
ORDER BY e.ENTITY_NAME;


-- ================================================================
-- 2. LAST 10 RUNS WITH ROW COUNTS
--    Entity-level and mapping-level entries combined.
-- ================================================================
SELECT *
FROM   GPC_DM.V_ETL_RUN_SUMMARY
WHERE  ROWNUM <= 10;


-- ================================================================
-- 3. ALL ERRORS IN THE LAST 24 HOURS
-- ================================================================
SELECT
    EL.RUN_ID,
    EL.ENTITY_NAME,
    EL.TARGET_TABLE,
    EL.ERROR_CODE,
    EL.ERROR_MESSAGE,
    EL.RECORD_KEY,
    EL.ERROR_TIME
FROM   GPC_DM.ETL_ERROR_LOG EL
WHERE  EL.ERROR_TIME >= SYSDATE - 1
ORDER BY EL.ERROR_TIME DESC;


-- ================================================================
-- 4. STEP-BY-STEP DETAIL FOR A SPECIFIC RUN
--    Replace :run_id with the RUN_ID from ETL_RUN_LOG.
-- ================================================================
SELECT
    SL.STEP_ID,
    SL.STEP_NAME,
    SL.STATUS,
    SL.ROWS_AFFECTED,
    ROUND((SL.END_TIME - SL.START_TIME) * 86400, 1) AS ELAPSED_SECS,
    SL.STEP_MESSAGE
FROM   GPC_DM.ETL_STEP_LOG SL
WHERE  SL.RUN_ID = :run_id
ORDER BY SL.STEP_ID;


-- ================================================================
-- 5. REJECTED RECORDS STILL IN STAGING
--    Rows that failed validation for investigation and reprocessing.
-- ================================================================
SELECT
    'STG_STAFFING_TIMELINE' AS STAGING_TABLE,
    STG_RUN_ID,
    SCHEDULE_ID             AS RECORD_KEY,
    DT_PERIOD,
    STG_REJECT_REASON,
    REPORTING_DATE
FROM   GPC_DM.STG_STAFFING_TIMELINE
WHERE  STG_STATUS = 'REJECTED'

UNION ALL

SELECT
    'STG_TIMELINE_COST',
    STG_RUN_ID,
    COST_ID,
    DT_PERIOD,
    STG_REJECT_REASON,
    REPORTING_DATE
FROM   GPC_DM.STG_TIMELINE_COST
WHERE  STG_STATUS = 'REJECTED'

ORDER BY STG_RUN_ID DESC, STAGING_TABLE;


-- ================================================================
-- 6. SCD2 VERSION HISTORY FOR A SPECIFIC SCHEDULE
--    Replace :schedule_id with the business key value.
-- ================================================================
SELECT
    DIM_SS_ID,
    SCHEDULE_ID,
    EMPLOYEE_ID,
    POSITION_ID,
    PROJECT_ID,
    SCHEDULE_TYPE,
    SCHEDULE_STATUS,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    IS_CURRENT,
    REPORTING_DATE,
    ETL_RUN_ID
FROM   GPC_DM.DIM_STAFFING_SCHEDULE
WHERE  SCHEDULE_ID = :schedule_id
ORDER BY EFFECTIVE_START_DATE;


-- ================================================================
-- 7. SCD2 VERSION HISTORY FOR A SPECIFIC COST RECORD
--    Replace :cost_id with the business key value.
-- ================================================================
SELECT
    DIM_COST_ID,
    COST_ID,
    PROJECT_ID,
    COST_CENTER,
    COST_TYPE,
    COST_CATEGORY,
    CURRENCY,
    BUDGET_VERSION,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    IS_CURRENT,
    REPORTING_DATE,
    ETL_RUN_ID
FROM   GPC_DM.DIM_COST
WHERE  COST_ID = :cost_id
ORDER BY EFFECTIVE_START_DATE;


-- ================================================================
-- 8. DAILY LOAD VOLUME TREND (LAST 30 DAYS)
-- ================================================================
SELECT
    TRUNC(START_TIME)   AS LOAD_DATE,
    ENTITY_NAME,
    SUM(ROWS_INSERTED)  AS TOTAL_INSERTED,
    SUM(ROWS_UPDATED)   AS TOTAL_UPDATED,
    SUM(ROWS_EXPIRED)   AS TOTAL_EXPIRED,
    SUM(ROWS_REJECTED)  AS TOTAL_REJECTED,
    COUNT(*)            AS RUN_COUNT,
    SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILURE_COUNT,
    ROUND(AVG(ELAPSED_SECS), 1) AS AVG_ELAPSED_SECS
FROM   GPC_DM.V_ETL_RUN_SUMMARY
WHERE  TRUNC(START_TIME) >= TRUNC(SYSDATE) - 30
GROUP BY TRUNC(START_TIME), ENTITY_NAME
ORDER BY LOAD_DATE DESC, ENTITY_NAME;


-- ================================================================
-- 9. STAGING TABLE SNAPSHOT — ROW COUNTS BY STATUS AND ACTION
-- ================================================================
SELECT *
FROM   GPC_DM.V_ETL_STAGING_SUMMARY
ORDER BY STG_RUN_ID DESC, STAGING_TABLE;


-- ================================================================
-- 10. DUPLICATE IS_CURRENT=Y CHECK (on-demand data quality audit)
--     Runs the same logic as PKG_ETL_VALIDATOR.check_scd2_duplicates
-- ================================================================
SELECT 'DIM_STAFFING_SCHEDULE' AS TARGET_TABLE,
       SCHEDULE_ID             AS BUSINESS_KEY,
       COUNT(*)                AS DUPLICATE_COUNT
FROM   GPC_DM.DIM_STAFFING_SCHEDULE
WHERE  IS_CURRENT = 'Y'
GROUP BY SCHEDULE_ID
HAVING COUNT(*) > 1

UNION ALL

SELECT 'DIM_COST',
       COST_ID,
       COUNT(*)
FROM   GPC_DM.DIM_COST
WHERE  IS_CURRENT = 'Y'
GROUP BY COST_ID
HAVING COUNT(*) > 1

ORDER BY 1, 2;


-- ================================================================
-- 11. COLUMN MAPPING REGISTRY SUMMARY
--     Useful for reviewing what is registered for each entity.
-- ================================================================
SELECT
    e.ENTITY_NAME,
    t.TARGET_TABLE,
    t.LOAD_TYPE,
    cm.TARGET_COLUMN,
    cm.IS_BUSINESS_KEY,
    cm.IS_TRACKED,
    cm.COLUMN_ORDER
FROM   GPC_DM.ETL_ENTITY          e
JOIN   GPC_DM.ETL_TARGET_MAPPING   t  ON t.ENTITY_ID  = e.ENTITY_ID
JOIN   GPC_DM.ETL_COLUMN_MAPPING   cm ON cm.MAPPING_ID = t.MAPPING_ID
WHERE  e.IS_ACTIVE  = 'Y'
AND    t.IS_ACTIVE  = 'Y'
AND    cm.IS_ACTIVE = 'Y'
ORDER BY e.ENTITY_NAME, t.LOAD_ORDER, cm.COLUMN_ORDER;


-- ================================================================
-- 12. WATERMARK HEALTH CHECK
--     Shows entities where watermark appears stale (>2 days old)
-- ================================================================
SELECT
    e.ENTITY_NAME,
    c.LAST_WATERMARK,
    c.LAST_RUN_DATE,
    TRUNC(SYSDATE) - TRUNC(c.LAST_WATERMARK)   AS WATERMARK_AGE_DAYS,
    c.STATUS
FROM   GPC_DM.ETL_CONTROL  c
JOIN   GPC_DM.ETL_ENTITY    e ON e.ENTITY_ID = c.ENTITY_ID
WHERE  e.IS_ACTIVE = 'Y'
AND    c.STATUS   != 'DISABLED'
AND    (
           c.LAST_WATERMARK IS NULL
           OR TRUNC(SYSDATE) - TRUNC(c.LAST_WATERMARK) > 2
       )
ORDER BY WATERMARK_AGE_DAYS DESC NULLS FIRST;
