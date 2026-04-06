-- ============================================================
-- FILE: 00_migrate_target_tables.sql
-- DESC: Safe, idempotent migration of metadata columns onto
--       existing GPC_DM target (DIM) tables.
--
--       Checks ALL_TAB_COLUMNS before every ALTER TABLE so it
--       is safe to re-run any number of times.  Never drops or
--       recreates a table.
--
-- Columns added to ALL four DIM tables:
--   SOURCE_CODE          VARCHAR2(100)         -- source system identifier
--   RECORD_HASH          VARCHAR2(64)          -- change-detection hash
--   REPORTING_DATE       DATE                  -- business reporting date
--   ETL_RUN_ID           NUMBER                -- FK to ETL_RUN_LOG
--   ETL_LOAD_DATE        DATE  DEFAULT SYSDATE -- physical load timestamp
--
-- Additional columns for SCD2 tables only
--   (DIM_STAFFING_SCHEDULE, DIM_COST):
--   EFFECTIVE_START_DATE DATE  DEFAULT TRUNC(SYSDATE) -- version start
--   EFFECTIVE_END_DATE   DATE  DEFAULT DATE '9999-12-31' -- version end
--   IS_CURRENT           VARCHAR2(1) DEFAULT 'Y'         -- current flag
--
-- NOTE: EFFECTIVE_START_DATE is added with DEFAULT TRUNC(SYSDATE)
--       so that existing rows receive a valid, non-NULL value.
--       After running this script you may UPDATE those rows to a
--       more meaningful date if required.
--
-- Usage (SQL*Plus / SQLcl):
--   @00_migrate_target_tables.sql
--
-- Usage (SQL Developer VS Code Extension):
--   @<full_path>/EDWH_Framework/ddl/00_migrate_target_tables.sql
-- ============================================================

SET SERVEROUTPUT ON SIZE UNLIMITED;

DECLARE

    -- ----------------------------------------------------------------
    -- Helper: add a column only if it does not already exist.
    -- p_owner   : schema owner (GPC_DM)
    -- p_table   : table name in ALL_TAB_COLUMNS
    -- p_col     : column name to add
    -- p_ddl_col : full column definition string for ALTER TABLE
    -- ----------------------------------------------------------------
    PROCEDURE add_col_if_missing (
        p_owner   IN VARCHAR2,
        p_table   IN VARCHAR2,
        p_col     IN VARCHAR2,
        p_ddl_col IN VARCHAR2
    ) IS
        v_count   PLS_INTEGER;
        v_sql     VARCHAR2(500);
    BEGIN
        SELECT COUNT(*)
        INTO   v_count
        FROM   ALL_TAB_COLUMNS
        WHERE  OWNER       = UPPER(p_owner)
        AND    TABLE_NAME  = UPPER(p_table)
        AND    COLUMN_NAME = UPPER(p_col);

        IF v_count = 0 THEN
            v_sql := 'ALTER TABLE ' || p_owner || '.' || p_table
                     || ' ADD (' || p_ddl_col || ')';
            EXECUTE IMMEDIATE v_sql;
            DBMS_OUTPUT.PUT_LINE('  [ADDED]  ' || p_table || '.' || p_col);
        ELSE
            DBMS_OUTPUT.PUT_LINE('  [EXISTS] ' || p_table || '.' || p_col);
        END IF;
    END add_col_if_missing;

BEGIN

    -- ================================================================
    -- SECTION 1 — Columns common to ALL four DIM tables
    -- ================================================================

    -- ────────────────────────────────────────────────────────────────
    --  DIM_STAFFING_SCHEDULE
    -- ────────────────────────────────────────────────────────────────
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '>>> DIM_STAFFING_SCHEDULE');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_SCHEDULE', 'SOURCE_CODE',
        'SOURCE_CODE   VARCHAR2(100)');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_SCHEDULE', 'RECORD_HASH',
        'RECORD_HASH   VARCHAR2(64)');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_SCHEDULE', 'REPORTING_DATE',
        'REPORTING_DATE DATE');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_SCHEDULE', 'ETL_RUN_ID',
        'ETL_RUN_ID    NUMBER');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_SCHEDULE', 'ETL_LOAD_DATE',
        'ETL_LOAD_DATE DATE DEFAULT SYSDATE');

    -- SCD2-specific
    add_col_if_missing('GPC_DM', 'DIM_STAFFING_SCHEDULE', 'EFFECTIVE_START_DATE',
        'EFFECTIVE_START_DATE DATE DEFAULT TRUNC(SYSDATE)');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_SCHEDULE', 'EFFECTIVE_END_DATE',
        q'[EFFECTIVE_END_DATE DATE DEFAULT DATE '9999-12-31']');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_SCHEDULE', 'IS_CURRENT',
        q'[IS_CURRENT VARCHAR2(1) DEFAULT 'Y'
           CONSTRAINT CHK_DIM_SS_CURRENT CHECK (IS_CURRENT IN ('Y','N'))]');


    -- ────────────────────────────────────────────────────────────────
    --  DIM_STAFFING_TIMELINE
    -- ────────────────────────────────────────────────────────────────
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '>>> DIM_STAFFING_TIMELINE');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_TIMELINE', 'SOURCE_CODE',
        'SOURCE_CODE   VARCHAR2(100)');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_TIMELINE', 'RECORD_HASH',
        'RECORD_HASH   VARCHAR2(64)');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_TIMELINE', 'REPORTING_DATE',
        'REPORTING_DATE DATE');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_TIMELINE', 'ETL_RUN_ID',
        'ETL_RUN_ID    NUMBER');

    add_col_if_missing('GPC_DM', 'DIM_STAFFING_TIMELINE', 'ETL_LOAD_DATE',
        'ETL_LOAD_DATE DATE DEFAULT SYSDATE');


    -- ────────────────────────────────────────────────────────────────
    --  DIM_COST
    -- ────────────────────────────────────────────────────────────────
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '>>> DIM_COST');

    add_col_if_missing('GPC_DM', 'DIM_COST', 'SOURCE_CODE',
        'SOURCE_CODE   VARCHAR2(100)');

    add_col_if_missing('GPC_DM', 'DIM_COST', 'RECORD_HASH',
        'RECORD_HASH   VARCHAR2(64)');

    add_col_if_missing('GPC_DM', 'DIM_COST', 'REPORTING_DATE',
        'REPORTING_DATE DATE');

    add_col_if_missing('GPC_DM', 'DIM_COST', 'ETL_RUN_ID',
        'ETL_RUN_ID    NUMBER');

    add_col_if_missing('GPC_DM', 'DIM_COST', 'ETL_LOAD_DATE',
        'ETL_LOAD_DATE DATE DEFAULT SYSDATE');

    -- SCD2-specific
    add_col_if_missing('GPC_DM', 'DIM_COST', 'EFFECTIVE_START_DATE',
        'EFFECTIVE_START_DATE DATE DEFAULT TRUNC(SYSDATE)');

    add_col_if_missing('GPC_DM', 'DIM_COST', 'EFFECTIVE_END_DATE',
        q'[EFFECTIVE_END_DATE DATE DEFAULT DATE '9999-12-31']');

    add_col_if_missing('GPC_DM', 'DIM_COST', 'IS_CURRENT',
        q'[IS_CURRENT VARCHAR2(1) DEFAULT 'Y'
           CONSTRAINT CHK_DIM_COST_CURRENT CHECK (IS_CURRENT IN ('Y','N'))]');


    -- ────────────────────────────────────────────────────────────────
    --  DIM_TIMELINE_COST
    -- ────────────────────────────────────────────────────────────────
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '>>> DIM_TIMELINE_COST');

    add_col_if_missing('GPC_DM', 'DIM_TIMELINE_COST', 'SOURCE_CODE',
        'SOURCE_CODE   VARCHAR2(100)');

    add_col_if_missing('GPC_DM', 'DIM_TIMELINE_COST', 'RECORD_HASH',
        'RECORD_HASH   VARCHAR2(64)');

    add_col_if_missing('GPC_DM', 'DIM_TIMELINE_COST', 'REPORTING_DATE',
        'REPORTING_DATE DATE');

    add_col_if_missing('GPC_DM', 'DIM_TIMELINE_COST', 'ETL_RUN_ID',
        'ETL_RUN_ID    NUMBER');

    add_col_if_missing('GPC_DM', 'DIM_TIMELINE_COST', 'ETL_LOAD_DATE',
        'ETL_LOAD_DATE DATE DEFAULT SYSDATE');


    -- ================================================================
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '=== Migration complete. ===');
    DBMS_OUTPUT.PUT_LINE(
        'Verify with:' || CHR(10) ||
        '  SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_DEFAULT' || CHR(10) ||
        '  FROM   ALL_TAB_COLUMNS' || CHR(10) ||
        '  WHERE  OWNER = ''GPC_DM''' || CHR(10) ||
        '  AND    TABLE_NAME IN (''DIM_STAFFING_SCHEDULE'',''DIM_STAFFING_TIMELINE'',' || CHR(10) ||
        '                        ''DIM_COST'',''DIM_TIMELINE_COST'')' || CHR(10) ||
        '  AND    COLUMN_NAME IN (''SOURCE_CODE'',''RECORD_HASH'',''REPORTING_DATE'',' || CHR(10) ||
        '                         ''ETL_RUN_ID'',''ETL_LOAD_DATE'',' || CHR(10) ||
        '                         ''EFFECTIVE_START_DATE'',''EFFECTIVE_END_DATE'',''IS_CURRENT'')' || CHR(10) ||
        '  ORDER BY TABLE_NAME, COLUMN_NAME;'
    );

END;
/
