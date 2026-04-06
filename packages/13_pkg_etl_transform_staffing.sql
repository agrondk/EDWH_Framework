-- ============================================================
-- FILE: 13_pkg_etl_transform_staffing.sql
-- DESC: PKG_ETL_TRANSFORM_STAFFING
--       Entity-specific transform for STAFFING_SCHEDULE.
--       Reads from KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB
--       and populates two staging tables:
--         STG_STAFFING_SCHEDULE  → feeds DIM_STAFFING_SCHEDULE (SCD2)
--         STG_STAFFING_TIMELINE  → feeds DIM_STAFFING_TIMELINE  (INCREMENTAL)
--
-- Design notes:
--   * Source rows are deduplicated by taking the latest REPORTING_DATE
--     per SCHEDULE_ID (for DIM) and per SCHEDULE_ID+period (for timeline).
--   * STANDARD_HASH is computed over tracked attributes (non-BK cols).
--   * DT_PERIOD is derived from PERIOD_START_DATE as YYYYMM.
--     NULL PERIOD_START_DATE → DT_PERIOD = NULL → caught by validator.
--   * Any PENDING staging rows from prior failed runs are cleared
--     before inserting fresh data (idempotent behaviour).
-- SCHEMA: GPC_DM
-- SOURCE: KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB
-- ============================================================

CREATE OR REPLACE PACKAGE GPC_DM.PKG_ETL_TRANSFORM_STAFFING AS

    PROCEDURE transform(
        p_run_id         IN NUMBER,
        p_entity_id      IN NUMBER,
        p_from_watermark IN DATE,
        p_to_watermark   IN DATE
    );

END PKG_ETL_TRANSFORM_STAFFING;
/


CREATE OR REPLACE PACKAGE BODY GPC_DM.PKG_ETL_TRANSFORM_STAFFING AS

    PROCEDURE transform(
        p_run_id         IN NUMBER,
        p_entity_id      IN NUMBER,
        p_from_watermark IN DATE,
        p_to_watermark   IN DATE
    ) IS
        v_step_id    NUMBER;
        v_rows_ss    NUMBER;
        v_rows_st    NUMBER;
    BEGIN
        -- --------------------------------------------------------
        -- Clear any PENDING rows from prior failed runs.
        -- LOADED/REJECTED rows are retained for audit.
        -- --------------------------------------------------------
        DELETE FROM GPC_DM.STG_STAFFING_SCHEDULE WHERE STG_STATUS = 'PENDING';
        DELETE FROM GPC_DM.STG_STAFFING_TIMELINE  WHERE STG_STATUS = 'PENDING';

        -- --------------------------------------------------------
        -- STAGE 1: Populate STG_STAFFING_SCHEDULE
        -- Deduplication: latest REPORTING_DATE per SCHEDULE_ID.
        -- Hash covers: EMPLOYEE_ID, POSITION_ID, PROJECT_ID,
        --              SCHEDULE_TYPE, SCHEDULE_STATUS,
        --              SCHEDULE_START_DATE, SCHEDULE_END_DATE
        -- --------------------------------------------------------
        v_step_id := GPC_DM.PKG_ETL_LOGGER.log_step(
            p_run_id, 'TRANSFORM:STG_STAFFING_SCHEDULE', 'RUNNING');

        INSERT INTO GPC_DM.STG_STAFFING_SCHEDULE (
            STG_RUN_ID,
            STG_STATUS,
            STG_RECORD_HASH,
            SCHEDULE_ID,
            EMPLOYEE_ID,
            POSITION_ID,
            PROJECT_ID,
            SCHEDULE_TYPE,
            SCHEDULE_STATUS,
            SCHEDULE_START_DATE,
            SCHEDULE_END_DATE,
            REPORTING_DATE
        )
        SELECT
            p_run_id,
            'PENDING',
            STANDARD_HASH(
                COALESCE(EMPLOYEE_ID,   'NULL') || CHR(1) ||
                COALESCE(POSITION_ID,   'NULL') || CHR(1) ||
                COALESCE(PROJECT_ID,    'NULL') || CHR(1) ||
                COALESCE(SCHEDULE_TYPE, 'NULL') || CHR(1) ||
                COALESCE(SCHEDULE_STATUS,'NULL') || CHR(1) ||
                COALESCE(TO_CHAR(SCHEDULE_START_DATE, 'YYYY-MM-DD'), 'NULL') || CHR(1) ||
                COALESCE(TO_CHAR(SCHEDULE_END_DATE,   'YYYY-MM-DD'), 'NULL'),
                'SHA256'
            ),
            SCHEDULE_ID,
            EMPLOYEE_ID,
            POSITION_ID,
            PROJECT_ID,
            SCHEDULE_TYPE,
            SCHEDULE_STATUS,
            SCHEDULE_START_DATE,
            SCHEDULE_END_DATE,
            REPORTING_DATE
        FROM (
            SELECT
                SCHEDULE_ID,
                EMPLOYEE_ID,
                POSITION_ID,
                PROJECT_ID,
                SCHEDULE_TYPE,
                SCHEDULE_STATUS,
                SCHEDULE_START_DATE,
                SCHEDULE_END_DATE,
                REPORTING_DATE,
                ROW_NUMBER() OVER (
                    PARTITION BY SCHEDULE_ID
                    ORDER BY REPORTING_DATE DESC, ROWID DESC
                ) AS rn
            FROM   KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB
            WHERE  REPORTING_DATE >  p_from_watermark
            AND    REPORTING_DATE <= p_to_watermark
            AND    SCHEDULE_ID   IS NOT NULL  -- basic source guard
        )
        WHERE rn = 1;

        v_rows_ss := SQL%ROWCOUNT;

        GPC_DM.PKG_ETL_LOGGER.end_step(
            v_step_id, 'SUCCESS', v_rows_ss,
            v_rows_ss || ' row(s) staged for DIM_STAFFING_SCHEDULE.'
        );

        -- --------------------------------------------------------
        -- STAGE 2: Populate STG_STAFFING_TIMELINE
        -- Business key: SCHEDULE_ID + DT_PERIOD (YYYYMM)
        -- Deduplication: latest REPORTING_DATE per SCHEDULE_ID + period month.
        -- DT_PERIOD derived from PERIOD_START_DATE;
        --   NULL → DT_PERIOD = NULL → validator rejects the row.
        -- Hash covers: PERIOD_START_DATE, PERIOD_END_DATE, ALLOCATED_HOURS
        -- --------------------------------------------------------
        v_step_id := GPC_DM.PKG_ETL_LOGGER.log_step(
            p_run_id, 'TRANSFORM:STG_STAFFING_TIMELINE', 'RUNNING');

        INSERT INTO GPC_DM.STG_STAFFING_TIMELINE (
            STG_RUN_ID,
            STG_STATUS,
            STG_RECORD_HASH,
            SCHEDULE_ID,
            DT_PERIOD,
            PERIOD_START_DATE,
            PERIOD_END_DATE,
            ALLOCATED_HOURS,
            REPORTING_DATE
        )
        SELECT
            p_run_id,
            'PENDING',
            STANDARD_HASH(
                COALESCE(TO_CHAR(PERIOD_START_DATE, 'YYYY-MM-DD'), 'NULL') || CHR(1) ||
                COALESCE(TO_CHAR(PERIOD_END_DATE,   'YYYY-MM-DD'), 'NULL') || CHR(1) ||
                COALESCE(TO_CHAR(ALLOCATED_HOURS),                 'NULL'),
                'SHA256'
            ),
            SCHEDULE_ID,
            TO_CHAR(PERIOD_START_DATE, 'YYYYMM'),  -- NULL if source date missing
            PERIOD_START_DATE,
            PERIOD_END_DATE,
            ALLOCATED_HOURS,
            REPORTING_DATE
        FROM (
            SELECT
                SCHEDULE_ID,
                PERIOD_START_DATE,
                PERIOD_END_DATE,
                ALLOCATED_HOURS,
                REPORTING_DATE,
                ROW_NUMBER() OVER (
                    PARTITION BY SCHEDULE_ID, TRUNC(PERIOD_START_DATE, 'MM')
                    ORDER BY REPORTING_DATE DESC, ROWID DESC
                ) AS rn
            FROM   KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB
            WHERE  REPORTING_DATE >  p_from_watermark
            AND    REPORTING_DATE <= p_to_watermark
            AND    SCHEDULE_ID   IS NOT NULL
            -- Note: rows with NULL PERIOD_START_DATE are included here
            -- so the validator can count and log them as rejected.
        )
        WHERE rn = 1;

        v_rows_st := SQL%ROWCOUNT;

        GPC_DM.PKG_ETL_LOGGER.end_step(
            v_step_id, 'SUCCESS', v_rows_st,
            v_rows_st || ' row(s) staged for DIM_STAFFING_TIMELINE.'
        );

    EXCEPTION
        WHEN OTHERS THEN
            GPC_DM.PKG_ETL_LOGGER.end_step(v_step_id, 'FAILED', 0, SQLERRM);
            RAISE;
    END transform;

END PKG_ETL_TRANSFORM_STAFFING;
/
