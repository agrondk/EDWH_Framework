-- ============================================================
-- FILE: 10_pkg_etl_control.sql
-- DESC: PKG_ETL_CONTROL — watermark management and run locking
--       Controls which data window is processed each run and
--       prevents concurrent execution of the same entity.
-- SCHEMA: GPC_DM
-- ============================================================

CREATE OR REPLACE PACKAGE GPC_DM.PKG_ETL_CONTROL AS

    -- --------------------------------------------------------
    -- Returns the current watermark for an entity.
    -- Returns DATE '1900-01-01' on first run (NULL watermark),
    -- so the entire source table is processed initially.
    -- --------------------------------------------------------
    FUNCTION get_watermark(p_entity_id IN NUMBER) RETURN DATE;

    -- --------------------------------------------------------
    -- Queries the source table to find the maximum watermark
    -- value available. Used to set the upper bound of each run.
    -- --------------------------------------------------------
    FUNCTION get_max_source_wm(p_entity_id IN NUMBER) RETURN DATE;

    -- --------------------------------------------------------
    -- Acquires an exclusive run lock via SELECT FOR UPDATE NOWAIT.
    -- Raises -20020 if another session holds the lock.
    -- Raises -20021 if entity STATUS = RUNNING.
    -- Raises -20022 if entity STATUS = DISABLED.
    -- Sets STATUS = RUNNING and records the run id.
    -- --------------------------------------------------------
    PROCEDURE lock_entity(p_entity_id IN NUMBER, p_run_id IN NUMBER);

    -- --------------------------------------------------------
    -- Releases the run lock. Uses AUTONOMOUS_TRANSACTION so
    -- the status update commits regardless of main transaction.
    -- p_status = 'SUCCESS' → STATUS := IDLE
    -- p_status = anything else → STATUS := FAILED
    -- --------------------------------------------------------
    PROCEDURE release_entity(
        p_entity_id IN NUMBER,
        p_run_id    IN NUMBER,
        p_status    IN VARCHAR2
    );

    -- --------------------------------------------------------
    -- Advances the watermark to p_watermark on successful run.
    -- --------------------------------------------------------
    PROCEDURE advance_watermark(
        p_entity_id IN NUMBER,
        p_watermark IN DATE,
        p_run_id    IN NUMBER
    );

    -- --------------------------------------------------------
    -- Returns TRUE if entity is eligible to run (IDLE or FAILED).
    -- --------------------------------------------------------
    FUNCTION is_runnable(p_entity_id IN NUMBER) RETURN BOOLEAN;

    -- --------------------------------------------------------
    -- Manually reset a FAILED entity to IDLE to allow re-run.
    -- Logs the reset in the NOTES column.
    -- --------------------------------------------------------
    PROCEDURE reset_failed_entity(
        p_entity_id IN NUMBER,
        p_reason    IN VARCHAR2 DEFAULT 'Manual reset'
    );

END PKG_ETL_CONTROL;
/


CREATE OR REPLACE PACKAGE BODY GPC_DM.PKG_ETL_CONTROL AS

    FUNCTION get_watermark(p_entity_id IN NUMBER) RETURN DATE IS
        v_wm  DATE;
    BEGIN
        SELECT LAST_WATERMARK
        INTO   v_wm
        FROM   GPC_DM.ETL_CONTROL
        WHERE  ENTITY_ID = p_entity_id;
        -- NULL means first run; return epoch start so all source data is included
        RETURN NVL(v_wm, DATE '1900-01-01');
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20025,
                'No control record found for entity_id: ' || p_entity_id
                || '. Ensure ETL_CONTROL row was inserted during onboarding.');
    END get_watermark;


    FUNCTION get_max_source_wm(p_entity_id IN NUMBER) RETURN DATE IS
        v_sql        VARCHAR2(1000);
        v_src_table  VARCHAR2(200);
        v_wm_col     VARCHAR2(100);
        v_wm         DATE;
    BEGIN
        SELECT e.SOURCE_TABLE, e.WATERMARK_COLUMN
        INTO   v_src_table, v_wm_col
        FROM   GPC_DM.ETL_ENTITY e
        WHERE  e.ENTITY_ID = p_entity_id;

        v_sql := 'SELECT MAX(' || v_wm_col || ') FROM ' || v_src_table;
        EXECUTE IMMEDIATE v_sql INTO v_wm;
        RETURN v_wm;
    END get_max_source_wm;


    PROCEDURE lock_entity(p_entity_id IN NUMBER, p_run_id IN NUMBER) IS
        v_status  VARCHAR2(20);
    BEGIN
        -- Attempt a NOWAIT lock to detect concurrent sessions
        BEGIN
            SELECT STATUS
            INTO   v_status
            FROM   GPC_DM.ETL_CONTROL
            WHERE  ENTITY_ID = p_entity_id
            FOR UPDATE NOWAIT;
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -54 THEN  -- ORA-00054: resource busy
                    RAISE_APPLICATION_ERROR(-20020,
                        'Cannot acquire lock on entity_id ' || p_entity_id
                        || '. Another session is currently running it.');
                ELSE
                    RAISE;
                END IF;
        END;

        -- Guard against double-start or disabled entity
        IF v_status = 'RUNNING' THEN
            RAISE_APPLICATION_ERROR(-20021,
                'Entity ' || p_entity_id || ' status is already RUNNING. '
                || 'If this is stale, use reset_failed_entity after investigation.');
        END IF;

        IF v_status = 'DISABLED' THEN
            RAISE_APPLICATION_ERROR(-20022,
                'Entity ' || p_entity_id || ' is DISABLED. '
                || 'Update ETL_CONTROL.STATUS to IDLE to re-enable.');
        END IF;

        UPDATE GPC_DM.ETL_CONTROL
        SET    STATUS        = 'RUNNING',
               LAST_RUN_ID   = p_run_id,
               LAST_RUN_DATE = SYSDATE
        WHERE  ENTITY_ID    = p_entity_id;
        -- Caller commits this update alongside any other pre-work
    END lock_entity;


    PROCEDURE release_entity(
        p_entity_id IN NUMBER,
        p_run_id    IN NUMBER,
        p_status    IN VARCHAR2
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE GPC_DM.ETL_CONTROL
        SET    STATUS      = CASE
                                WHEN p_status = 'SUCCESS' THEN 'IDLE'
                                ELSE 'FAILED'
                             END,
               LAST_RUN_ID = p_run_id
        WHERE  ENTITY_ID = p_entity_id;
        COMMIT;
    END release_entity;


    PROCEDURE advance_watermark(
        p_entity_id IN NUMBER,
        p_watermark IN DATE,
        p_run_id    IN NUMBER
    ) IS
    BEGIN
        UPDATE GPC_DM.ETL_CONTROL
        SET    LAST_WATERMARK = p_watermark,
               LAST_RUN_DATE  = SYSDATE,
               LAST_RUN_ID    = p_run_id
        WHERE  ENTITY_ID = p_entity_id;
    END advance_watermark;


    FUNCTION is_runnable(p_entity_id IN NUMBER) RETURN BOOLEAN IS
        v_status  VARCHAR2(20);
    BEGIN
        SELECT STATUS
        INTO   v_status
        FROM   GPC_DM.ETL_CONTROL
        WHERE  ENTITY_ID = p_entity_id;
        RETURN v_status IN ('IDLE','FAILED');
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN FALSE;
    END is_runnable;


    PROCEDURE reset_failed_entity(
        p_entity_id IN NUMBER,
        p_reason    IN VARCHAR2 DEFAULT 'Manual reset'
    ) IS
        v_current_status  VARCHAR2(20);
    BEGIN
        SELECT STATUS INTO v_current_status
        FROM   GPC_DM.ETL_CONTROL
        WHERE  ENTITY_ID = p_entity_id
        FOR UPDATE NOWAIT;

        IF v_current_status NOT IN ('FAILED','RUNNING') THEN
            RAISE_APPLICATION_ERROR(-20026,
                'Entity ' || p_entity_id || ' is in status ' || v_current_status
                || '. Only FAILED or RUNNING entities can be manually reset.');
        END IF;

        UPDATE GPC_DM.ETL_CONTROL
        SET    STATUS = 'IDLE',
               NOTES  = SUBSTR(
                    'Reset at ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS')
                    || ' — ' || p_reason, 1, 500)
        WHERE  ENTITY_ID = p_entity_id;
        COMMIT;
    END reset_failed_entity;

END PKG_ETL_CONTROL;
/
