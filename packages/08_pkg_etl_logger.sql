-- ============================================================
-- FILE: 08_pkg_etl_logger.sql
-- DESC: PKG_ETL_LOGGER — centralised logging package
--       All procedures use PRAGMA AUTONOMOUS_TRANSACTION so
--       log entries persist even when the calling transaction
--       rolls back on error.
-- SCHEMA: GPC_DM
-- ============================================================

CREATE OR REPLACE PACKAGE GPC_DM.PKG_ETL_LOGGER AS

    -- --------------------------------------------------------
    -- Open a new run log entry.
    -- p_mapping_id = NULL for an entity-level parent run entry.
    -- Returns the new RUN_ID.
    -- --------------------------------------------------------
    FUNCTION start_run(
        p_entity_id   IN NUMBER,
        p_mapping_id  IN NUMBER DEFAULT NULL
    ) RETURN NUMBER;

    -- --------------------------------------------------------
    -- Close a run entry with final status and cumulative counts.
    -- --------------------------------------------------------
    PROCEDURE end_run(
        p_run_id        IN NUMBER,
        p_status        IN VARCHAR2,
        p_rows_read     IN NUMBER   DEFAULT 0,
        p_rows_inserted IN NUMBER   DEFAULT 0,
        p_rows_updated  IN NUMBER   DEFAULT 0,
        p_rows_expired  IN NUMBER   DEFAULT 0,
        p_rows_skipped  IN NUMBER   DEFAULT 0,
        p_rows_rejected IN NUMBER   DEFAULT 0,
        p_error_message IN VARCHAR2 DEFAULT NULL
    );

    -- --------------------------------------------------------
    -- Open a step log entry within a run.
    -- Returns the new STEP_ID.
    -- --------------------------------------------------------
    FUNCTION log_step(
        p_run_id    IN NUMBER,
        p_step_name IN VARCHAR2,
        p_status    IN VARCHAR2 DEFAULT 'RUNNING',
        p_rows      IN NUMBER   DEFAULT 0,
        p_message   IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER;

    -- --------------------------------------------------------
    -- Close an existing step log entry.
    -- p_rows and p_message are optional; existing values kept if NULL.
    -- --------------------------------------------------------
    PROCEDURE end_step(
        p_step_id   IN NUMBER,
        p_status    IN VARCHAR2,
        p_rows      IN NUMBER   DEFAULT NULL,
        p_message   IN VARCHAR2 DEFAULT NULL
    );

    -- --------------------------------------------------------
    -- Log an error record.
    -- p_record_key : serialised business key(s), e.g. 'SCHEDULE_ID=SCH001'
    -- p_record_data: full CLOB for investigation (optional)
    -- --------------------------------------------------------
    PROCEDURE log_error(
        p_run_id       IN NUMBER,
        p_entity_name  IN VARCHAR2,
        p_target_table IN VARCHAR2,
        p_error_code   IN VARCHAR2,
        p_error_msg    IN VARCHAR2,
        p_record_key   IN VARCHAR2 DEFAULT NULL,
        p_record_data  IN CLOB     DEFAULT NULL
    );

END PKG_ETL_LOGGER;
/


CREATE OR REPLACE PACKAGE BODY GPC_DM.PKG_ETL_LOGGER AS

    FUNCTION start_run(
        p_entity_id   IN NUMBER,
        p_mapping_id  IN NUMBER DEFAULT NULL
    ) RETURN NUMBER IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_run_id  NUMBER;
    BEGIN
        v_run_id := GPC_DM.SEQ_ETL_RUN_LOG.NEXTVAL;
        INSERT INTO GPC_DM.ETL_RUN_LOG (
            RUN_ID, ENTITY_ID, MAPPING_ID, START_TIME, STATUS
        ) VALUES (
            v_run_id, p_entity_id, p_mapping_id, SYSDATE, 'RUNNING'
        );
        COMMIT;
        RETURN v_run_id;
    END start_run;


    PROCEDURE end_run(
        p_run_id        IN NUMBER,
        p_status        IN VARCHAR2,
        p_rows_read     IN NUMBER   DEFAULT 0,
        p_rows_inserted IN NUMBER   DEFAULT 0,
        p_rows_updated  IN NUMBER   DEFAULT 0,
        p_rows_expired  IN NUMBER   DEFAULT 0,
        p_rows_skipped  IN NUMBER   DEFAULT 0,
        p_rows_rejected IN NUMBER   DEFAULT 0,
        p_error_message IN VARCHAR2 DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE GPC_DM.ETL_RUN_LOG
        SET    END_TIME      = SYSDATE,
               STATUS        = p_status,
               ROWS_READ     = NVL(p_rows_read,     0),
               ROWS_INSERTED = NVL(p_rows_inserted, 0),
               ROWS_UPDATED  = NVL(p_rows_updated,  0),
               ROWS_EXPIRED  = NVL(p_rows_expired,  0),
               ROWS_SKIPPED  = NVL(p_rows_skipped,  0),
               ROWS_REJECTED = NVL(p_rows_rejected, 0),
               ERROR_MESSAGE = SUBSTR(p_error_message, 1, 4000)
        WHERE  RUN_ID = p_run_id;
        COMMIT;
    END end_run;


    FUNCTION log_step(
        p_run_id    IN NUMBER,
        p_step_name IN VARCHAR2,
        p_status    IN VARCHAR2 DEFAULT 'RUNNING',
        p_rows      IN NUMBER   DEFAULT 0,
        p_message   IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_step_id  NUMBER;
    BEGIN
        v_step_id := GPC_DM.SEQ_ETL_STEP_LOG.NEXTVAL;
        INSERT INTO GPC_DM.ETL_STEP_LOG (
            STEP_ID, RUN_ID, STEP_NAME, START_TIME, STATUS,
            ROWS_AFFECTED, STEP_MESSAGE
        ) VALUES (
            v_step_id, p_run_id, SUBSTR(p_step_name, 1, 200),
            SYSDATE, p_status,
            NVL(p_rows, 0), SUBSTR(p_message, 1, 4000)
        );
        COMMIT;
        RETURN v_step_id;
    END log_step;


    PROCEDURE end_step(
        p_step_id   IN NUMBER,
        p_status    IN VARCHAR2,
        p_rows      IN NUMBER   DEFAULT NULL,
        p_message   IN VARCHAR2 DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE GPC_DM.ETL_STEP_LOG
        SET    END_TIME      = SYSDATE,
               STATUS        = p_status,
               ROWS_AFFECTED = CASE WHEN p_rows IS NOT NULL THEN p_rows
                                    ELSE ROWS_AFFECTED END,
               STEP_MESSAGE  = CASE WHEN p_message IS NOT NULL
                                    THEN SUBSTR(p_message, 1, 4000)
                                    ELSE STEP_MESSAGE END
        WHERE  STEP_ID = p_step_id;
        COMMIT;
    END end_step;


    PROCEDURE log_error(
        p_run_id       IN NUMBER,
        p_entity_name  IN VARCHAR2,
        p_target_table IN VARCHAR2,
        p_error_code   IN VARCHAR2,
        p_error_msg    IN VARCHAR2,
        p_record_key   IN VARCHAR2 DEFAULT NULL,
        p_record_data  IN CLOB     DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO GPC_DM.ETL_ERROR_LOG (
            ERR_ID, RUN_ID, ENTITY_NAME, TARGET_TABLE,
            ERROR_CODE, ERROR_MESSAGE, ERROR_TIME,
            RECORD_KEY, RECORD_DATA
        ) VALUES (
            GPC_DM.SEQ_ETL_ERROR_LOG.NEXTVAL,
            p_run_id,
            SUBSTR(p_entity_name,  1, 100),
            SUBSTR(p_target_table, 1, 200),
            SUBSTR(p_error_code,   1, 50),
            SUBSTR(p_error_msg,    1, 4000),
            SYSDATE,
            SUBSTR(p_record_key,   1, 500),
            p_record_data
        );
        COMMIT;
    END log_error;

END PKG_ETL_LOGGER;
/
