-- ============================================================
-- FILE: 07_views.sql
-- DESC: Operational and convenience views for the EDWH framework
-- SCHEMA: GPC_DM
-- ============================================================

-- ----------------------------------------------------------------
-- V_ETL_ACTIVE_ENTITIES
-- Shows all active entities with their mapping details and
-- current watermark / control status. Used by run_all and ops.
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW GPC_DM.V_ETL_ACTIVE_ENTITIES AS
SELECT
    e.ENTITY_ID,
    e.ENTITY_NAME,
    e.SOURCE_TABLE,
    e.WATERMARK_COLUMN,
    t.MAPPING_ID,
    t.TARGET_TABLE,
    t.STAGING_TABLE,
    t.LOAD_TYPE,
    t.LOAD_ORDER,
    c.LAST_WATERMARK,
    c.STATUS         AS CTRL_STATUS,
    c.LAST_RUN_DATE,
    c.LAST_RUN_ID
FROM   GPC_DM.ETL_ENTITY         e
JOIN   GPC_DM.ETL_TARGET_MAPPING  t ON t.ENTITY_ID  = e.ENTITY_ID  AND t.IS_ACTIVE = 'Y'
JOIN   GPC_DM.ETL_CONTROL         c ON c.ENTITY_ID  = e.ENTITY_ID
WHERE  e.IS_ACTIVE = 'Y'
ORDER BY e.ENTITY_NAME, t.LOAD_ORDER;

COMMENT ON TABLE GPC_DM.V_ETL_ACTIVE_ENTITIES IS 'Active entities with mapping details and current watermark/control status.';


-- ----------------------------------------------------------------
-- V_ETL_RUN_SUMMARY
-- Last 7 days of run history with elapsed time and row counts.
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW GPC_DM.V_ETL_RUN_SUMMARY AS
SELECT
    r.RUN_ID,
    e.ENTITY_NAME,
    t.TARGET_TABLE,
    r.START_TIME,
    r.END_TIME,
    ROUND((r.END_TIME - r.START_TIME) * 86400, 1)  AS ELAPSED_SECS,
    r.STATUS,
    r.ROWS_READ,
    r.ROWS_INSERTED,
    r.ROWS_UPDATED,
    r.ROWS_EXPIRED,
    r.ROWS_SKIPPED,
    r.ROWS_REJECTED,
    r.ERROR_MESSAGE
FROM   GPC_DM.ETL_RUN_LOG         r
JOIN   GPC_DM.ETL_ENTITY           e ON e.ENTITY_ID  = r.ENTITY_ID
LEFT JOIN GPC_DM.ETL_TARGET_MAPPING t ON t.MAPPING_ID = r.MAPPING_ID
WHERE  r.START_TIME >= SYSDATE - 7
ORDER BY r.RUN_ID DESC;

COMMENT ON TABLE GPC_DM.V_ETL_RUN_SUMMARY IS 'Run history for the last 7 days with elapsed seconds and per-action row counts.';


-- ----------------------------------------------------------------
-- V_DIM_STAFFING_CURRENT
-- Current (IS_CURRENT=Y) staffing schedule records with ETL lineage.
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW GPC_DM.V_DIM_STAFFING_CURRENT AS
SELECT
    d.DIM_SS_ID,
    d.SCHEDULE_ID,
    d.EMPLOYEE_ID,
    d.POSITION_ID,
    d.PROJECT_ID,
    d.SCHEDULE_TYPE,
    d.SCHEDULE_STATUS,
    d.SCHEDULE_START_DATE,
    d.SCHEDULE_END_DATE,
    d.EFFECTIVE_START_DATE,
    d.REPORTING_DATE,
    d.ETL_RUN_ID,
    d.ETL_LOAD_DATE,
    r.START_TIME    AS ETL_RUN_START,
    r.STATUS        AS ETL_RUN_STATUS
FROM   GPC_DM.DIM_STAFFING_SCHEDULE d
LEFT JOIN GPC_DM.ETL_RUN_LOG        r ON r.RUN_ID = d.ETL_RUN_ID
WHERE  d.IS_CURRENT = 'Y';

COMMENT ON TABLE GPC_DM.V_DIM_STAFFING_CURRENT IS 'Current version only of DIM_STAFFING_SCHEDULE with ETL run lineage.';


-- ----------------------------------------------------------------
-- V_DIM_COST_CURRENT
-- Current (IS_CURRENT=Y) cost dimension records with ETL lineage.
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW GPC_DM.V_DIM_COST_CURRENT AS
SELECT
    d.DIM_COST_ID,
    d.COST_ID,
    d.PROJECT_ID,
    d.COST_CENTER,
    d.COST_TYPE,
    d.COST_CATEGORY,
    d.CURRENCY,
    d.BUDGET_VERSION,
    d.EFFECTIVE_START_DATE,
    d.REPORTING_DATE,
    d.ETL_RUN_ID,
    d.ETL_LOAD_DATE,
    r.START_TIME    AS ETL_RUN_START,
    r.STATUS        AS ETL_RUN_STATUS
FROM   GPC_DM.DIM_COST           d
LEFT JOIN GPC_DM.ETL_RUN_LOG     r ON r.RUN_ID = d.ETL_RUN_ID
WHERE  d.IS_CURRENT = 'Y';

COMMENT ON TABLE GPC_DM.V_DIM_COST_CURRENT IS 'Current version only of DIM_COST with ETL run lineage.';


-- ----------------------------------------------------------------
-- V_ETL_STAGING_SUMMARY
-- Snapshot of current staging table state across all entities.
-- Useful for monitoring what is pending, loaded, or rejected.
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW GPC_DM.V_ETL_STAGING_SUMMARY AS
SELECT 'STG_STAFFING_SCHEDULE' AS STAGING_TABLE,
       STG_RUN_ID,
       STG_STATUS,
       STG_ACTION,
       COUNT(*)     AS ROW_COUNT
FROM   GPC_DM.STG_STAFFING_SCHEDULE
GROUP BY STG_RUN_ID, STG_STATUS, STG_ACTION
UNION ALL
SELECT 'STG_STAFFING_TIMELINE',
       STG_RUN_ID, STG_STATUS, STG_ACTION, COUNT(*)
FROM   GPC_DM.STG_STAFFING_TIMELINE
GROUP BY STG_RUN_ID, STG_STATUS, STG_ACTION
UNION ALL
SELECT 'STG_COST',
       STG_RUN_ID, STG_STATUS, STG_ACTION, COUNT(*)
FROM   GPC_DM.STG_COST
GROUP BY STG_RUN_ID, STG_STATUS, STG_ACTION
UNION ALL
SELECT 'STG_TIMELINE_COST',
       STG_RUN_ID, STG_STATUS, STG_ACTION, COUNT(*)
FROM   GPC_DM.STG_TIMELINE_COST
GROUP BY STG_RUN_ID, STG_STATUS, STG_ACTION
ORDER BY STG_RUN_ID DESC, STAGING_TABLE, STG_STATUS;

COMMENT ON TABLE GPC_DM.V_ETL_STAGING_SUMMARY IS 'Aggregated staging row counts by run, status, and action across all staging tables.';
