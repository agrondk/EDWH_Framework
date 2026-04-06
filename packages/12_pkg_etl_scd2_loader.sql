-- ============================================================
-- FILE: 12_pkg_etl_scd2_loader.sql
-- DESC: PKG_ETL_SCD2_LOADER — generic SCD2 and incremental loader
--
-- load_scd2()
--   Phase 1 CLASSIFY  : marks staging rows as INSERT / UPDATE / UNCHANGED
--   Phase 2 EXPIRE    : sets IS_CURRENT='N', EFFECTIVE_END_DATE on changed rows
--   Phase 3 INSERT    : inserts new current versions for INSERT + UPDATE rows
--   Phase 4 LOADED    : marks staging rows as LOADED
--
-- load_incremental()
--   Single MERGE statement on business key(s).
--   Skips rows where RECORD_HASH has not changed.
--
-- All SQL is built dynamically from ETL_COLUMN_MAPPING metadata.
-- SCHEMA: GPC_DM
-- ============================================================

CREATE OR REPLACE PACKAGE GPC_DM.PKG_ETL_SCD2_LOADER AS

    PROCEDURE load_scd2(
        p_run_id         IN NUMBER,
        p_mapping_id     IN NUMBER,
        p_reporting_date IN DATE
    );

    PROCEDURE load_incremental(
        p_run_id         IN NUMBER,
        p_mapping_id     IN NUMBER,
        p_reporting_date IN DATE
    );

END PKG_ETL_SCD2_LOADER;
/


CREATE OR REPLACE PACKAGE BODY GPC_DM.PKG_ETL_SCD2_LOADER AS

    ---------------------------------------------------------------------------
    -- Build "t.BK_COL = s.SRC_COL AND ..." join condition from BK columns
    ---------------------------------------------------------------------------
    FUNCTION build_bk_join(
        p_mapping_id IN NUMBER,
        p_t_alias    IN VARCHAR2 DEFAULT 't',
        p_s_alias    IN VARCHAR2 DEFAULT 's'
    ) RETURN VARCHAR2 IS
        v_clause  VARCHAR2(2000) := '';
    BEGIN
        FOR rec IN (
            SELECT TARGET_COLUMN, SOURCE_COLUMN
            FROM   GPC_DM.ETL_COLUMN_MAPPING
            WHERE  MAPPING_ID      = p_mapping_id
            AND    IS_BUSINESS_KEY = 'Y'
            AND    IS_ACTIVE       = 'Y'
            ORDER BY COLUMN_ORDER
        ) LOOP
            IF v_clause IS NOT NULL THEN
                v_clause := v_clause || ' AND ';
            END IF;
            v_clause := v_clause
                        || p_t_alias || '.' || rec.TARGET_COLUMN
                        || ' = '
                        || p_s_alias || '.' || rec.SOURCE_COLUMN;
        END LOOP;
        RETURN v_clause;
    END build_bk_join;


    ---------------------------------------------------------------------------
    -- Build "t.COL1 = s.COL1, t.COL2 = s.COL2, ..." SET clause (non-BK cols)
    ---------------------------------------------------------------------------
    FUNCTION build_update_set(
        p_mapping_id IN NUMBER,
        p_t_alias    IN VARCHAR2 DEFAULT 't',
        p_s_alias    IN VARCHAR2 DEFAULT 's'
    ) RETURN VARCHAR2 IS
        v_clause  VARCHAR2(4000) := '';
    BEGIN
        FOR rec IN (
            SELECT TARGET_COLUMN, SOURCE_COLUMN
            FROM   GPC_DM.ETL_COLUMN_MAPPING
            WHERE  MAPPING_ID      = p_mapping_id
            AND    IS_BUSINESS_KEY = 'N'
            AND    IS_ACTIVE       = 'Y'
            ORDER BY COLUMN_ORDER
        ) LOOP
            IF v_clause IS NOT NULL THEN
                v_clause := v_clause || ', ';
            END IF;
            v_clause := v_clause
                        || p_t_alias || '.' || rec.TARGET_COLUMN
                        || ' = '
                        || p_s_alias || '.' || rec.SOURCE_COLUMN;
        END LOOP;
        RETURN v_clause;
    END build_update_set;


    ---------------------------------------------------------------------------
    -- Build comma-separated TARGET_COLUMN list (for INSERT column list)
    ---------------------------------------------------------------------------
    FUNCTION build_insert_col_list(p_mapping_id IN NUMBER) RETURN VARCHAR2 IS
        v_list  VARCHAR2(4000) := '';
    BEGIN
        FOR rec IN (
            SELECT TARGET_COLUMN
            FROM   GPC_DM.ETL_COLUMN_MAPPING
            WHERE  MAPPING_ID = p_mapping_id
            AND    IS_ACTIVE  = 'Y'
            ORDER BY COLUMN_ORDER
        ) LOOP
            IF v_list IS NOT NULL THEN v_list := v_list || ', '; END IF;
            v_list := v_list || rec.TARGET_COLUMN;
        END LOOP;
        RETURN v_list;
    END build_insert_col_list;


    ---------------------------------------------------------------------------
    -- Build comma-separated s.SOURCE_COLUMN list (for INSERT VALUES)
    ---------------------------------------------------------------------------
    FUNCTION build_insert_val_list(
        p_mapping_id IN NUMBER,
        p_alias      IN VARCHAR2 DEFAULT 's'
    ) RETURN VARCHAR2 IS
        v_list  VARCHAR2(4000) := '';
    BEGIN
        FOR rec IN (
            SELECT SOURCE_COLUMN
            FROM   GPC_DM.ETL_COLUMN_MAPPING
            WHERE  MAPPING_ID = p_mapping_id
            AND    IS_ACTIVE  = 'Y'
            ORDER BY COLUMN_ORDER
        ) LOOP
            IF v_list IS NOT NULL THEN v_list := v_list || ', '; END IF;
            v_list := v_list || p_alias || '.' || rec.SOURCE_COLUMN;
        END LOOP;
        RETURN v_list;
    END build_insert_val_list;


    ---------------------------------------------------------------------------
    -- SCD2 LOADER
    ---------------------------------------------------------------------------
    PROCEDURE load_scd2(
        p_run_id         IN NUMBER,
        p_mapping_id     IN NUMBER,
        p_reporting_date IN DATE
    ) IS
        v_stg       VARCHAR2(200);
        v_tgt       VARCHAR2(200);
        v_seq       VARCHAR2(200);
        v_sk_col    VARCHAR2(100);
        v_hash_col  VARCHAR2(100);
        v_bk_join   VARCHAR2(2000);
        v_ins_cols  VARCHAR2(4000);
        v_ins_vals  VARCHAR2(4000);
        v_sql       VARCHAR2(32767);
        v_step_id   NUMBER;
        v_ins_cnt   NUMBER := 0;
        v_upd_cnt   NUMBER := 0;
        v_exp_cnt   NUMBER := 0;
        v_unc_cnt   NUMBER := 0;
    BEGIN
        -- Fetch mapping attributes
        SELECT STAGING_TABLE, TARGET_TABLE,
               SURROGATE_SEQ_NAME, SURROGATE_KEY_COL, HASH_COL
        INTO   v_stg, v_tgt, v_seq, v_sk_col, v_hash_col
        FROM   GPC_DM.ETL_TARGET_MAPPING
        WHERE  MAPPING_ID = p_mapping_id;

        -- Pre-build reusable SQL fragments
        v_bk_join  := build_bk_join(p_mapping_id, 't', 's');
        v_ins_cols := build_insert_col_list(p_mapping_id);
        v_ins_vals := build_insert_val_list(p_mapping_id, 's');

        -- --------------------------------------------------------
        -- PHASE 1 — CLASSIFY staging rows
        -- --------------------------------------------------------
        v_step_id := GPC_DM.PKG_ETL_LOGGER.log_step(
            p_run_id, 'SCD2_CLASSIFY:' || v_tgt, 'RUNNING');

        -- Mark UNCHANGED: BK exists in target, hash matches
        v_sql :=
            'UPDATE ' || v_stg || ' s'
            ||' SET s.STG_ACTION = ''UNCHANGED'''
            ||' WHERE s.STG_RUN_ID = :1'
            ||' AND   s.STG_STATUS = ''PENDING'''
            ||' AND EXISTS ('
            ||'   SELECT 1 FROM ' || v_tgt || ' t'
            ||'   WHERE ' || v_bk_join
            ||'   AND   t.IS_CURRENT = ''Y'''
            ||'   AND   t.' || v_hash_col || ' = s.STG_RECORD_HASH'
            ||')';
        EXECUTE IMMEDIATE v_sql USING p_run_id;
        v_unc_cnt := SQL%ROWCOUNT;

        -- Mark UPDATE: BK exists in target, hash changed
        v_sql :=
            'UPDATE ' || v_stg || ' s'
            ||' SET s.STG_ACTION = ''UPDATE'''
            ||' WHERE s.STG_RUN_ID = :1'
            ||' AND   s.STG_STATUS = ''PENDING'''
            ||' AND   s.STG_ACTION IS NULL'
            ||' AND EXISTS ('
            ||'   SELECT 1 FROM ' || v_tgt || ' t'
            ||'   WHERE ' || v_bk_join
            ||'   AND   t.IS_CURRENT = ''Y'''
            ||')';
        EXECUTE IMMEDIATE v_sql USING p_run_id;
        v_upd_cnt := SQL%ROWCOUNT;

        -- Mark INSERT: BK not found in target at all
        v_sql :=
            'UPDATE ' || v_stg || ' s'
            ||' SET s.STG_ACTION = ''INSERT'''
            ||' WHERE s.STG_RUN_ID = :1'
            ||' AND   s.STG_STATUS = ''PENDING'''
            ||' AND   s.STG_ACTION IS NULL';
        EXECUTE IMMEDIATE v_sql USING p_run_id;
        v_ins_cnt := SQL%ROWCOUNT;

        GPC_DM.PKG_ETL_LOGGER.end_step(
            v_step_id, 'SUCCESS',
            v_ins_cnt + v_upd_cnt + v_unc_cnt,
            'INSERT=' || v_ins_cnt
            || ' UPDATE=' || v_upd_cnt
            || ' UNCHANGED=' || v_unc_cnt
        );

        -- --------------------------------------------------------
        -- PHASE 2 — EXPIRE changed records in target
        -- --------------------------------------------------------
        v_step_id := GPC_DM.PKG_ETL_LOGGER.log_step(
            p_run_id, 'SCD2_EXPIRE:' || v_tgt, 'RUNNING');

        v_sql :=
            'UPDATE ' || v_tgt || ' t'
            ||' SET t.IS_CURRENT         = ''N'','
            ||'     t.EFFECTIVE_END_DATE  = TRUNC(SYSDATE) - 1'
            ||' WHERE t.IS_CURRENT = ''Y'''
            ||' AND EXISTS ('
            ||'   SELECT 1 FROM ' || v_stg || ' s'
            ||'   WHERE s.STG_RUN_ID = :1'
            ||'   AND   s.STG_ACTION = ''UPDATE'''
            ||'   AND   ' || v_bk_join
            ||')';
        EXECUTE IMMEDIATE v_sql USING p_run_id;
        v_exp_cnt := SQL%ROWCOUNT;

        GPC_DM.PKG_ETL_LOGGER.end_step(
            v_step_id, 'SUCCESS', v_exp_cnt,
            v_exp_cnt || ' row(s) expired in ' || v_tgt
        );

        -- --------------------------------------------------------
        -- PHASE 3 — INSERT new current versions
        -- --------------------------------------------------------
        v_step_id := GPC_DM.PKG_ETL_LOGGER.log_step(
            p_run_id, 'SCD2_INSERT:' || v_tgt, 'RUNNING');

        v_sql :=
            'INSERT INTO ' || v_tgt
            ||' (' || v_sk_col
            ||', ' || v_ins_cols
            ||', EFFECTIVE_START_DATE'
            ||', EFFECTIVE_END_DATE'
            ||', IS_CURRENT'
            ||', ' || v_hash_col
            ||', REPORTING_DATE'
            ||', ETL_RUN_ID'
            ||', ETL_LOAD_DATE'
            ||')'
            ||' SELECT ' || v_seq || '.NEXTVAL'
            ||',        ' || v_ins_vals
            ||',        TRUNC(SYSDATE)'
            ||',        DATE ''9999-12-31'''
            ||',        ''Y'''
            ||',        s.STG_RECORD_HASH'
            ||',        :1'   -- p_reporting_date
            ||',        :2'   -- p_run_id
            ||',        SYSDATE'
            ||' FROM ' || v_stg || ' s'
            ||' WHERE s.STG_RUN_ID = :3'
            ||' AND   s.STG_ACTION IN (''INSERT'',''UPDATE'')'
            ||' AND   s.STG_STATUS = ''PENDING''';

        EXECUTE IMMEDIATE v_sql
            USING p_reporting_date, p_run_id, p_run_id;

        GPC_DM.PKG_ETL_LOGGER.end_step(
            v_step_id, 'SUCCESS', SQL%ROWCOUNT,
            SQL%ROWCOUNT || ' new version(s) inserted into ' || v_tgt
        );

        -- --------------------------------------------------------
        -- PHASE 4 — Mark staging rows as LOADED
        -- --------------------------------------------------------
        v_sql :=
            'UPDATE ' || v_stg
            ||' SET STG_STATUS = ''LOADED'''
            ||' WHERE STG_RUN_ID = :1'
            ||' AND   STG_ACTION IN (''INSERT'',''UPDATE'',''UNCHANGED'')';
        EXECUTE IMMEDIATE v_sql USING p_run_id;

        -- Accumulate counts into the mapping-level run log entry
        UPDATE GPC_DM.ETL_RUN_LOG
        SET    ROWS_INSERTED = ROWS_INSERTED + v_ins_cnt,
               ROWS_UPDATED  = ROWS_UPDATED  + v_upd_cnt,
               ROWS_EXPIRED  = ROWS_EXPIRED  + v_exp_cnt,
               ROWS_SKIPPED  = ROWS_SKIPPED  + v_unc_cnt
        WHERE  RUN_ID = p_run_id;

    EXCEPTION
        WHEN OTHERS THEN
            GPC_DM.PKG_ETL_LOGGER.end_step(v_step_id, 'FAILED', 0, SQLERRM);
            RAISE;
    END load_scd2;


    ---------------------------------------------------------------------------
    -- INCREMENTAL LOADER  (MERGE on business key)
    ---------------------------------------------------------------------------
    PROCEDURE load_incremental(
        p_run_id         IN NUMBER,
        p_mapping_id     IN NUMBER,
        p_reporting_date IN DATE
    ) IS
        v_stg       VARCHAR2(200);
        v_tgt       VARCHAR2(200);
        v_seq       VARCHAR2(200);
        v_sk_col    VARCHAR2(100);
        v_hash_col  VARCHAR2(100);
        v_bk_join   VARCHAR2(2000);
        v_upd_set   VARCHAR2(4000);
        v_ins_cols  VARCHAR2(4000);
        v_ins_vals  VARCHAR2(4000);
        v_sql       VARCHAR2(32767);
        v_step_id   NUMBER;
    BEGIN
        SELECT STAGING_TABLE, TARGET_TABLE,
               SURROGATE_SEQ_NAME, SURROGATE_KEY_COL, HASH_COL
        INTO   v_stg, v_tgt, v_seq, v_sk_col, v_hash_col
        FROM   GPC_DM.ETL_TARGET_MAPPING
        WHERE  MAPPING_ID = p_mapping_id;

        v_bk_join  := build_bk_join(p_mapping_id, 't', 's');
        v_upd_set  := build_update_set(p_mapping_id, 't', 's');
        v_ins_cols := build_insert_col_list(p_mapping_id);
        v_ins_vals := build_insert_val_list(p_mapping_id, 's');

        v_step_id := GPC_DM.PKG_ETL_LOGGER.log_step(
            p_run_id, 'INCREMENTAL_MERGE:' || v_tgt, 'RUNNING');

        v_sql :=
            'MERGE INTO ' || v_tgt || ' t'
            ||' USING ('
            ||'   SELECT * FROM ' || v_stg
            ||'   WHERE STG_RUN_ID = :1'
            ||'   AND   STG_STATUS = ''PENDING'''
            ||' ) s'
            ||' ON (' || v_bk_join || ')'
            ||' WHEN MATCHED THEN'
            ||'   UPDATE SET'
            ||'     ' || v_upd_set
            ||'   , t.' || v_hash_col || '  = s.STG_RECORD_HASH'
            ||'   , t.REPORTING_DATE        = :2'
            ||'   , t.ETL_RUN_ID            = :3'
            ||'   , t.ETL_LOAD_DATE         = SYSDATE'
            ||'   WHERE t.' || v_hash_col || ' != s.STG_RECORD_HASH'  -- skip unchanged
            ||' WHEN NOT MATCHED THEN'
            ||'   INSERT ('
            ||'     ' || v_sk_col
            ||'   , ' || v_ins_cols
            ||'   , ' || v_hash_col
            ||'   , REPORTING_DATE'
            ||'   , ETL_RUN_ID'
            ||'   , ETL_LOAD_DATE'
            ||'   ) VALUES ('
            ||'     ' || v_seq || '.NEXTVAL'
            ||'   , ' || v_ins_vals
            ||'   , s.STG_RECORD_HASH'
            ||'   , :4'   -- p_reporting_date
            ||'   , :5'   -- p_run_id
            ||'   , SYSDATE'
            ||'   )';

        EXECUTE IMMEDIATE v_sql
            USING p_run_id, p_reporting_date, p_run_id,
                  p_reporting_date, p_run_id;

        -- Mark all PENDING rows as LOADED
        v_sql :=
            'UPDATE ' || v_stg
            ||' SET STG_STATUS = ''LOADED'', STG_ACTION = ''MERGE'''
            ||' WHERE STG_RUN_ID = :1 AND STG_STATUS = ''PENDING''';
        EXECUTE IMMEDIATE v_sql USING p_run_id;

        GPC_DM.PKG_ETL_LOGGER.end_step(
            v_step_id, 'SUCCESS', SQL%ROWCOUNT,
            'MERGE into ' || v_tgt || ' complete.'
        );

    EXCEPTION
        WHEN OTHERS THEN
            GPC_DM.PKG_ETL_LOGGER.end_step(v_step_id, 'FAILED', 0, SQLERRM);
            RAISE;
    END load_incremental;

END PKG_ETL_SCD2_LOADER;
/
