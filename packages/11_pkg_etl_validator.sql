-- ============================================================
-- FILE: 11_pkg_etl_validator.sql
-- DESC: PKG_ETL_VALIDATOR — staging data quality validation
--       Applies ETL_VALIDATION_RULE definitions to staging rows:
--         NOT_NULL  → mark REJECTED if column is NULL
--         DERIVED   → attempt derivation via DERIVED_SQL, then
--                     reject if still NULL
--         CHECK     → mark REJECTED if DERIVED_SQL predicate is
--                     FALSE or NULL (enum checks, date order, etc.)
--       ERROR_ACTION = FAIL  raises exception and aborts the run.
--       ERROR_ACTION = REJECT marks rows and continues.
--
--       CHECK rules mirror the enum / date-order validations applied
--       in the C# FluentValidation backend (TimelineExelValidator,
--       PaffExelValidator, CostExcelValidator).
--
--       Also validates SCD2 target integrity (duplicate active rows).
-- SCHEMA: GPC_DM
-- ============================================================

CREATE OR REPLACE PACKAGE GPC_DM.PKG_ETL_VALIDATOR AS

    -- --------------------------------------------------------
    -- Apply all active validation rules for a mapping against
    -- its staging table rows with STG_STATUS = 'PENDING'.
    -- Returns total number of rejected rows across all rules.
    -- --------------------------------------------------------
    FUNCTION validate_staging(
        p_run_id      IN NUMBER,
        p_mapping_id  IN NUMBER,
        p_entity_name IN VARCHAR2
    ) RETURN NUMBER;

    -- --------------------------------------------------------
    -- Detect duplicate IS_CURRENT = 'Y' rows for the same
    -- business key in an SCD2 target table.
    -- Logs an error if duplicates are found.
    -- Returns count of duplicate business keys found.
    -- --------------------------------------------------------
    FUNCTION check_scd2_duplicates(
        p_run_id     IN NUMBER,
        p_mapping_id IN NUMBER
    ) RETURN NUMBER;

END PKG_ETL_VALIDATOR;
/


CREATE OR REPLACE PACKAGE BODY GPC_DM.PKG_ETL_VALIDATOR AS

    FUNCTION validate_staging(
        p_run_id      IN NUMBER,
        p_mapping_id  IN NUMBER,
        p_entity_name IN VARCHAR2
    ) RETURN NUMBER IS
        v_stg_table    VARCHAR2(200);
        v_tgt_table    VARCHAR2(200);
        v_sql          VARCHAR2(4000);
        v_reject_cnt   NUMBER := 0;
        v_row_cnt      NUMBER;
        v_step_id      NUMBER;
    BEGIN
        SELECT STAGING_TABLE, TARGET_TABLE
        INTO   v_stg_table, v_tgt_table
        FROM   GPC_DM.ETL_TARGET_MAPPING
        WHERE  MAPPING_ID = p_mapping_id;

        v_step_id := GPC_DM.PKG_ETL_LOGGER.log_step(
            p_run_id,
            'VALIDATE_STAGING:' || v_tgt_table,
            'RUNNING'
        );

        FOR vr IN (
            SELECT RULE_ID, RULE_NAME, RULE_TYPE,
                   COLUMN_NAME, DERIVED_SQL, ERROR_ACTION
            FROM   GPC_DM.ETL_VALIDATION_RULE
            WHERE  MAPPING_ID = p_mapping_id
            AND    IS_ACTIVE  = 'Y'
            ORDER BY RULE_ID
        ) LOOP

            IF vr.RULE_TYPE = 'NOT_NULL' THEN
                -- Reject any PENDING rows where the column is NULL
                v_sql :=
                    'UPDATE ' || v_stg_table ||
                    ' SET STG_STATUS = ''REJECTED'','
                    ||'     STG_REJECT_REASON = SUBSTR('
                    ||'         NVL(STG_REJECT_REASON,'''')'
                    ||'         || '' ['' || :rule_name || '': '
                    ||             vr.COLUMN_NAME || ' IS NULL]'', 1, 500)'
                    ||' WHERE STG_RUN_ID = :run_id'
                    ||' AND   STG_STATUS = ''PENDING'''
                    ||' AND   ' || vr.COLUMN_NAME || ' IS NULL';

                EXECUTE IMMEDIATE v_sql USING vr.RULE_NAME, p_run_id;
                v_row_cnt := SQL%ROWCOUNT;

                IF v_row_cnt > 0 THEN
                    v_reject_cnt := v_reject_cnt + v_row_cnt;
                    GPC_DM.PKG_ETL_LOGGER.log_error(
                        p_run_id       => p_run_id,
                        p_entity_name  => p_entity_name,
                        p_target_table => v_tgt_table,
                        p_error_code   => 'VAL_NOT_NULL',
                        p_error_msg    => vr.RULE_NAME || ' — ' || vr.COLUMN_NAME
                                          || ' is NULL for ' || v_row_cnt || ' staging row(s).'
                    );

                    IF vr.ERROR_ACTION = 'FAIL' THEN
                        GPC_DM.PKG_ETL_LOGGER.end_step(
                            v_step_id, 'FAILED', v_reject_cnt,
                            'FAIL rule triggered: ' || vr.RULE_NAME);
                        RAISE_APPLICATION_ERROR(-20030,
                            'FAIL rule triggered: ' || vr.RULE_NAME
                            || ' — ' || v_row_cnt || ' row(s) with NULL '
                            || vr.COLUMN_NAME || ' in ' || v_stg_table);
                    END IF;
                END IF;

            ELSIF vr.RULE_TYPE = 'DERIVED' THEN
                -- Step 1: Attempt to populate the column using DERIVED_SQL
                -- for any PENDING rows where it is currently NULL.
                IF vr.DERIVED_SQL IS NOT NULL THEN
                    v_sql :=
                        'UPDATE ' || v_stg_table ||
                        ' SET ' || vr.COLUMN_NAME || ' = (' || vr.DERIVED_SQL || ')'
                        ||' WHERE STG_RUN_ID = :run_id'
                        ||' AND   STG_STATUS = ''PENDING'''
                        ||' AND   ' || vr.COLUMN_NAME || ' IS NULL';
                    EXECUTE IMMEDIATE v_sql USING p_run_id;
                END IF;

                -- Step 2: Reject any rows where the column is still NULL
                v_sql :=
                    'UPDATE ' || v_stg_table ||
                    ' SET STG_STATUS = ''REJECTED'','
                    ||'     STG_REJECT_REASON = SUBSTR('
                    ||'         NVL(STG_REJECT_REASON,'''')'
                    ||'         || '' [DERIVED:' || vr.COLUMN_NAME
                    ||             ' could not be derived]'', 1, 500)'
                    ||' WHERE STG_RUN_ID = :run_id'
                    ||' AND   STG_STATUS = ''PENDING'''
                    ||' AND   ' || vr.COLUMN_NAME || ' IS NULL';

                EXECUTE IMMEDIATE v_sql USING p_run_id;
                v_row_cnt := SQL%ROWCOUNT;

                IF v_row_cnt > 0 THEN
                    v_reject_cnt := v_reject_cnt + v_row_cnt;
                    GPC_DM.PKG_ETL_LOGGER.log_error(
                        p_run_id       => p_run_id,
                        p_entity_name  => p_entity_name,
                        p_target_table => v_tgt_table,
                        p_error_code   => 'VAL_DERIVED_NULL',
                        p_error_msg    => vr.RULE_NAME || ' — ' || vr.COLUMN_NAME
                                          || ' could not be derived for '
                                          || v_row_cnt || ' row(s). Check DERIVED_SQL.'
                    );

                    IF vr.ERROR_ACTION = 'FAIL' THEN
                        GPC_DM.PKG_ETL_LOGGER.end_step(
                            v_step_id, 'FAILED', v_reject_cnt,
                            'FAIL rule triggered: ' || vr.RULE_NAME);
                        RAISE_APPLICATION_ERROR(-20031,
                            'FAIL rule triggered: derived column ' || vr.COLUMN_NAME
                            || ' is NULL for ' || v_row_cnt || ' row(s) in '
                            || v_stg_table);
                    END IF;
                END IF;

            ELSIF vr.RULE_TYPE = 'CHECK' THEN
                -- Reject any PENDING rows where the SQL predicate (DERIVED_SQL)
                -- evaluates to FALSE or NULL.  DERIVED_SQL must be a valid WHERE
                -- clause expression over the staging table columns.
                -- Examples:
                --   SCHEDULE_TYPE IN ('Exempt','Non-Exempt',...)
                --   SCHEDULE_START_DATE IS NULL OR SCHEDULE_START_DATE <= SCHEDULE_END_DATE
                v_sql :=
                    'UPDATE ' || v_stg_table ||
                    ' SET STG_STATUS = ''REJECTED'','
                    ||'     STG_REJECT_REASON = SUBSTR('
                    ||'         NVL(STG_REJECT_REASON,'''')'
                    ||'         || '' ['' || :rule_name || '': '
                    ||             vr.COLUMN_NAME || ' check failed]'', 1, 500)'
                    ||' WHERE STG_RUN_ID = :run_id'
                    ||' AND   STG_STATUS = ''PENDING'''
                    ||' AND   NOT (' || vr.DERIVED_SQL || ')';

                EXECUTE IMMEDIATE v_sql USING vr.RULE_NAME, p_run_id;
                v_row_cnt := SQL%ROWCOUNT;

                IF v_row_cnt > 0 THEN
                    v_reject_cnt := v_reject_cnt + v_row_cnt;
                    GPC_DM.PKG_ETL_LOGGER.log_error(
                        p_run_id       => p_run_id,
                        p_entity_name  => p_entity_name,
                        p_target_table => v_tgt_table,
                        p_error_code   => 'VAL_CHECK_FAILED',
                        p_error_msg    => vr.RULE_NAME || ' — ' || vr.COLUMN_NAME
                                          || ' check condition failed for '
                                          || v_row_cnt || ' staging row(s). '
                                          || 'Condition: ' || SUBSTR(vr.DERIVED_SQL, 1, 200)
                    );

                    IF vr.ERROR_ACTION = 'FAIL' THEN
                        GPC_DM.PKG_ETL_LOGGER.end_step(
                            v_step_id, 'FAILED', v_reject_cnt,
                            'FAIL rule triggered: ' || vr.RULE_NAME);
                        RAISE_APPLICATION_ERROR(-20032,
                            'FAIL rule triggered: ' || vr.RULE_NAME
                            || ' — ' || v_row_cnt || ' row(s) failed CHECK in '
                            || v_stg_table);
                    END IF;
                END IF;

            END IF;  -- RULE_TYPE

        END LOOP;

        GPC_DM.PKG_ETL_LOGGER.end_step(
            v_step_id, 'SUCCESS', v_reject_cnt,
            v_reject_cnt || ' row(s) rejected by validation rules.'
        );

        RETURN v_reject_cnt;

    EXCEPTION
        WHEN OTHERS THEN
            GPC_DM.PKG_ETL_LOGGER.end_step(
                v_step_id, 'FAILED', 0,
                'Validation aborted: ' || SQLERRM);
            RAISE;
    END validate_staging;


    FUNCTION check_scd2_duplicates(
        p_run_id     IN NUMBER,
        p_mapping_id IN NUMBER
    ) RETURN NUMBER IS
        v_tgt_table   VARCHAR2(200);
        v_bk_cols     VARCHAR2(2000);
        v_sql         VARCHAR2(4000);
        v_dup_cnt     NUMBER;
    BEGIN
        SELECT TARGET_TABLE
        INTO   v_tgt_table
        FROM   GPC_DM.ETL_TARGET_MAPPING
        WHERE  MAPPING_ID = p_mapping_id;

        v_bk_cols := GPC_DM.PKG_ETL_METADATA.get_bk_cols(p_mapping_id);

        v_sql :=
            'SELECT COUNT(*) FROM ('
            ||' SELECT ' || v_bk_cols || ', COUNT(*) AS cnt'
            ||' FROM '   || v_tgt_table
            ||' WHERE IS_CURRENT = ''Y'''
            ||' GROUP BY ' || v_bk_cols
            ||' HAVING COUNT(*) > 1'
            ||')';

        EXECUTE IMMEDIATE v_sql INTO v_dup_cnt;

        IF v_dup_cnt > 0 THEN
            GPC_DM.PKG_ETL_LOGGER.log_error(
                p_run_id       => p_run_id,
                p_entity_name  => NULL,
                p_target_table => v_tgt_table,
                p_error_code   => 'SCD2_POST_LOAD_DUP',
                p_error_msg    => v_dup_cnt || ' duplicate IS_CURRENT=Y business key(s) '
                                  || 'detected in ' || v_tgt_table
                                  || ' after load. Investigate immediately.'
            );
        END IF;

        RETURN v_dup_cnt;
    END check_scd2_duplicates;

END PKG_ETL_VALIDATOR;
/
