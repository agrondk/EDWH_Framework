-- ============================================================
-- FILE: verify_install.sql
-- DESC: Post-install health check for the GPC_DM EDWH framework.
--       Run as GPC_DM or a DBA with SELECT on GPC_DM objects.
--
-- Checks:
--   1. Sequences       (12 expected)
--   2. Tables          (17 expected)
--   3. Views           ( 7 expected)
--   4. Packages        ( 8 expected, spec + body both VALID)
--   5. Metadata columns on DIM tables (migration check)
--   6. Metadata seed data (entity + mapping rows)
-- ============================================================

SET PAGESIZE 200
SET LINESIZE 130
SET FEEDBACK OFF
COLUMN OBJECT_TYPE  FORMAT A16
COLUMN OBJECT_NAME  FORMAT A40
COLUMN STATUS       FORMAT A8
COLUMN RESULT       FORMAT A8

-- ────────────────────────────────────────────────────────────────────
-- 1. SEQUENCES
-- ────────────────────────────────────────────────────────────────────
PROMPT
PROMPT ── 1. SEQUENCES ────────────────────────────────────────────────
SELECT
    s.SEQUENCE_NAME                                     AS OBJECT_NAME,
    CASE WHEN s.SEQUENCE_NAME IS NOT NULL THEN 'FOUND' ELSE 'MISSING' END AS RESULT
FROM (
    SELECT 'SEQ_DIM_COST'       AS SEQ_NAME FROM DUAL UNION ALL
    SELECT 'SEQ_DIM_SS'                     FROM DUAL UNION ALL
    SELECT 'SEQ_DIM_ST'                     FROM DUAL UNION ALL
    SELECT 'SEQ_DIM_TC'                     FROM DUAL UNION ALL
    SELECT 'SEQ_ETL_COL_MAP'               FROM DUAL UNION ALL
    SELECT 'SEQ_ETL_ENTITY'                FROM DUAL UNION ALL
    SELECT 'SEQ_ETL_ERROR_LOG'             FROM DUAL UNION ALL
    SELECT 'SEQ_ETL_MAPPING'               FROM DUAL UNION ALL
    SELECT 'SEQ_ETL_RUN_LOG'               FROM DUAL UNION ALL
    SELECT 'SEQ_ETL_SS'                    FROM DUAL UNION ALL
    SELECT 'SEQ_ETL_STEP_LOG'             FROM DUAL UNION ALL
    SELECT 'SEQ_ETL_VAL_RULE'             FROM DUAL
) expected
LEFT JOIN ALL_SEQUENCES s
       ON s.SEQUENCE_OWNER = 'GPC_DM'
      AND s.SEQUENCE_NAME  = expected.SEQ_NAME
ORDER BY SEQ_NAME;


-- ────────────────────────────────────────────────────────────────────
-- 2. TABLES
-- ────────────────────────────────────────────────────────────────────
PROMPT
PROMPT ── 2. TABLES ────────────────────────────────────────────────────
SELECT
    expected.TBL_NAME                                   AS OBJECT_NAME,
    CASE WHEN o.OBJECT_NAME IS NOT NULL THEN 'FOUND' ELSE 'MISSING' END AS RESULT
FROM (
    -- Metadata tables
    SELECT 'ETL_SOURCE_SYSTEM'          AS TBL_NAME FROM DUAL UNION ALL
    SELECT 'ETL_ENTITY'                              FROM DUAL UNION ALL
    SELECT 'ETL_TARGET_MAPPING'                      FROM DUAL UNION ALL
    SELECT 'ETL_COLUMN_MAPPING'                      FROM DUAL UNION ALL
    SELECT 'ETL_VALIDATION_RULE'                     FROM DUAL UNION ALL
    -- Control
    SELECT 'ETL_CONTROL'                             FROM DUAL UNION ALL
    -- Logging
    SELECT 'ETL_RUN_LOG'                             FROM DUAL UNION ALL
    SELECT 'ETL_ERROR_LOG'                           FROM DUAL UNION ALL
    SELECT 'ETL_STEP_LOG'                            FROM DUAL UNION ALL
    -- Target (DIM)
    SELECT 'DIM_STAFFING_SCHEDULE'                   FROM DUAL UNION ALL
    SELECT 'DIM_STAFFING_TIMELINE'                   FROM DUAL UNION ALL
    SELECT 'DIM_COST'                                FROM DUAL UNION ALL
    SELECT 'DIM_TIMELINE_COST'                       FROM DUAL UNION ALL
    -- Staging
    SELECT 'STG_STAFFING_SCHEDULE'                   FROM DUAL UNION ALL
    SELECT 'STG_STAFFING_TIMELINE'                   FROM DUAL UNION ALL
    SELECT 'STG_COST'                                FROM DUAL UNION ALL
    SELECT 'STG_TIMELINE_COST'                       FROM DUAL
) expected
LEFT JOIN ALL_OBJECTS o
       ON o.OWNER       = 'GPC_DM'
      AND o.OBJECT_TYPE = 'TABLE'
      AND o.OBJECT_NAME = expected.TBL_NAME
ORDER BY TBL_NAME;


-- ────────────────────────────────────────────────────────────────────
-- 3. VIEWS  (STATUS must be VALID)
-- ────────────────────────────────────────────────────────────────────
PROMPT
PROMPT ── 3. VIEWS ─────────────────────────────────────────────────────
SELECT
    expected.VW_NAME                                    AS OBJECT_NAME,
    CASE WHEN o.STATUS = 'VALID' THEN 'VALID'
         WHEN o.STATUS IS NOT NULL  THEN o.STATUS
         ELSE 'MISSING' END                             AS RESULT
FROM (
    SELECT 'V_ETL_ACTIVE_ENTITIES'    AS VW_NAME FROM DUAL UNION ALL
    SELECT 'V_ETL_RUN_SUMMARY'                   FROM DUAL UNION ALL
    SELECT 'V_DIM_STAFFING_CURRENT'              FROM DUAL UNION ALL
    SELECT 'V_DIM_COST_CURRENT'                  FROM DUAL UNION ALL
    SELECT 'V_ETL_STAGING_SUMMARY'               FROM DUAL UNION ALL
    SELECT 'V_IICS_RUN_SUMMARY'                  FROM DUAL UNION ALL
    SELECT 'V_IICS_ERROR_SUMMARY'                FROM DUAL
) expected
LEFT JOIN ALL_OBJECTS o
       ON o.OWNER       = 'GPC_DM'
      AND o.OBJECT_TYPE = 'VIEW'
      AND o.OBJECT_NAME = expected.VW_NAME
ORDER BY VW_NAME;


-- ────────────────────────────────────────────────────────────────────
-- 4. PACKAGES  (spec + body both VALID)
-- ────────────────────────────────────────────────────────────────────
PROMPT
PROMPT ── 4. PACKAGES ──────────────────────────────────────────────────
COLUMN SPEC   FORMAT A8
COLUMN BODY   FORMAT A8
SELECT
    expected.PKG_NAME                                   AS OBJECT_NAME,
    MAX(CASE WHEN o.OBJECT_TYPE = 'PACKAGE'      THEN NVL(o.STATUS,'MISSING') ELSE 'MISSING' END) AS SPEC,
    MAX(CASE WHEN o.OBJECT_TYPE = 'PACKAGE BODY' THEN NVL(o.STATUS,'MISSING') ELSE 'MISSING' END) AS BODY
FROM (
    SELECT 'PKG_ETL_LOGGER'            AS PKG_NAME FROM DUAL UNION ALL
    SELECT 'PKG_ETL_METADATA'                      FROM DUAL UNION ALL
    SELECT 'PKG_ETL_CONTROL'                       FROM DUAL UNION ALL
    SELECT 'PKG_ETL_VALIDATOR'                     FROM DUAL UNION ALL
    SELECT 'PKG_ETL_SCD2_LOADER'                   FROM DUAL UNION ALL
    SELECT 'PKG_ETL_TRANSFORM_STAFFING'            FROM DUAL UNION ALL
    SELECT 'PKG_ETL_TRANSFORM_COST'                FROM DUAL UNION ALL
    SELECT 'PKG_ETL_ORCHESTRATOR'                  FROM DUAL
) expected
LEFT JOIN ALL_OBJECTS o
       ON o.OWNER       = 'GPC_DM'
      AND o.OBJECT_TYPE IN ('PACKAGE','PACKAGE BODY')
      AND o.OBJECT_NAME = expected.PKG_NAME
GROUP BY expected.PKG_NAME
ORDER BY PKG_NAME;


-- ────────────────────────────────────────────────────────────────────
-- 5. METADATA COLUMNS on DIM tables (migration check)
-- ────────────────────────────────────────────────────────────────────
PROMPT
PROMPT ── 5. METADATA COLUMNS ON DIM TABLES ───────────────────────────
COLUMN TABLE_NAME   FORMAT A28
COLUMN COLUMN_NAME  FORMAT A24
SELECT
    expected.TBL    AS TABLE_NAME,
    expected.COL    AS COLUMN_NAME,
    CASE WHEN c.COLUMN_NAME IS NOT NULL THEN 'FOUND' ELSE 'MISSING' END AS RESULT
FROM (
    -- All 4 DIM tables — common audit columns
    SELECT 'DIM_STAFFING_SCHEDULE' AS TBL, 'SOURCE_CODE'          AS COL FROM DUAL UNION ALL
    SELECT 'DIM_STAFFING_SCHEDULE',         'RECORD_HASH'                 FROM DUAL UNION ALL
    SELECT 'DIM_STAFFING_SCHEDULE',         'REPORTING_DATE'              FROM DUAL UNION ALL
    SELECT 'DIM_STAFFING_SCHEDULE',         'ETL_RUN_ID'                  FROM DUAL UNION ALL
    SELECT 'DIM_STAFFING_SCHEDULE',         'ETL_LOAD_DATE'               FROM DUAL UNION ALL
    -- SCD2 control columns
    SELECT 'DIM_STAFFING_SCHEDULE',         'EFFECTIVE_START_DATE'        FROM DUAL UNION ALL
    SELECT 'DIM_STAFFING_SCHEDULE',         'EFFECTIVE_END_DATE'          FROM DUAL UNION ALL
    SELECT 'DIM_STAFFING_SCHEDULE',         'IS_CURRENT'                  FROM DUAL UNION ALL

    SELECT 'DIM_STAFFING_TIMELINE',         'SOURCE_CODE'                 FROM DUAL UNION ALL
    SELECT 'DIM_STAFFING_TIMELINE',         'RECORD_HASH'                 FROM DUAL UNION ALL
    SELECT 'DIM_STAFFING_TIMELINE',         'REPORTING_DATE'              FROM DUAL UNION ALL
    SELECT 'DIM_STAFFING_TIMELINE',         'ETL_RUN_ID'                  FROM DUAL UNION ALL
    SELECT 'DIM_STAFFING_TIMELINE',         'ETL_LOAD_DATE'               FROM DUAL UNION ALL

    SELECT 'DIM_COST',                      'SOURCE_CODE'                 FROM DUAL UNION ALL
    SELECT 'DIM_COST',                      'RECORD_HASH'                 FROM DUAL UNION ALL
    SELECT 'DIM_COST',                      'REPORTING_DATE'              FROM DUAL UNION ALL
    SELECT 'DIM_COST',                      'ETL_RUN_ID'                  FROM DUAL UNION ALL
    SELECT 'DIM_COST',                      'ETL_LOAD_DATE'               FROM DUAL UNION ALL
    SELECT 'DIM_COST',                      'EFFECTIVE_START_DATE'        FROM DUAL UNION ALL
    SELECT 'DIM_COST',                      'EFFECTIVE_END_DATE'          FROM DUAL UNION ALL
    SELECT 'DIM_COST',                      'IS_CURRENT'                  FROM DUAL UNION ALL

    SELECT 'DIM_TIMELINE_COST',             'SOURCE_CODE'                 FROM DUAL UNION ALL
    SELECT 'DIM_TIMELINE_COST',             'RECORD_HASH'                 FROM DUAL UNION ALL
    SELECT 'DIM_TIMELINE_COST',             'REPORTING_DATE'              FROM DUAL UNION ALL
    SELECT 'DIM_TIMELINE_COST',             'ETL_RUN_ID'                  FROM DUAL UNION ALL
    SELECT 'DIM_TIMELINE_COST',             'ETL_LOAD_DATE'               FROM DUAL
) expected
LEFT JOIN ALL_TAB_COLUMNS c
       ON c.OWNER       = 'GPC_DM'
      AND c.TABLE_NAME  = expected.TBL
      AND c.COLUMN_NAME = expected.COL
ORDER BY TBL, COL;


-- ────────────────────────────────────────────────────────────────────
-- 6. METADATA SEED DATA
-- ────────────────────────────────────────────────────────────────────
PROMPT
PROMPT ── 6. METADATA SEED DATA ────────────────────────────────────────
SELECT 'ETL_ENTITY rows'        AS CHECK_NAME, COUNT(*) AS ROW_COUNT FROM GPC_DM.ETL_ENTITY
UNION ALL
SELECT 'ETL_TARGET_MAPPING rows',              COUNT(*) FROM GPC_DM.ETL_TARGET_MAPPING
UNION ALL
SELECT 'ETL_COLUMN_MAPPING rows',             COUNT(*) FROM GPC_DM.ETL_COLUMN_MAPPING
UNION ALL
SELECT 'ETL_VALIDATION_RULE rows',            COUNT(*) FROM GPC_DM.ETL_VALIDATION_RULE
UNION ALL
SELECT 'ETL_CONTROL rows',                    COUNT(*) FROM GPC_DM.ETL_CONTROL
UNION ALL
SELECT 'ETL_SOURCE_SYSTEM rows',              COUNT(*) FROM GPC_DM.ETL_SOURCE_SYSTEM;


-- ────────────────────────────────────────────────────────────────────
-- 7. INVALID OBJECTS SUMMARY (anything INVALID in GPC_DM)
-- ────────────────────────────────────────────────────────────────────
PROMPT
PROMPT ── 7. INVALID OBJECTS IN GPC_DM ────────────────────────────────
SELECT OBJECT_TYPE, OBJECT_NAME, STATUS
FROM   ALL_OBJECTS
WHERE  OWNER  = 'GPC_DM'
AND    STATUS != 'VALID'
ORDER BY OBJECT_TYPE, OBJECT_NAME;

PROMPT
PROMPT ════════════════════════════════════════════════════════════════
PROMPT  Any row showing MISSING or INVALID above needs attention.
PROMPT  All RESULT values should be FOUND/VALID and ROW_COUNT > 0.
PROMPT ════════════════════════════════════════════════════════════════
SET FEEDBACK ON
