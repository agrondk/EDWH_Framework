-- ============================================================
-- FILE: 14_pkg_etl_transform_cost.sql
-- DESC: PKG_ETL_TRANSFORM_COST
--       Entity-specific transform for COST.
--       Reads from KBR_IHUB.APAC_PCDM_COST_IHUB
--       and populates two staging tables:
--         STG_COST           → feeds DIM_COST          (SCD2)
--         STG_TIMELINE_COST  → feeds DIM_TIMELINE_COST (INCREMENTAL)
--
-- Design notes:
--   * Source rows are deduplicated by latest REPORTING_DATE
--     per COST_ID (for DIM) and per COST_ID + period month (for timeline).
--   * CURVE_TYPE derivation logic (applied at transform time;
--     validator re-applies as fallback and rejects if still NULL):
--       FORECAST_TYPE IN ('ACTUALS','ACTUAL')      → 'ACTUAL'
--       BUDGET_VERSION LIKE 'BUD%'                 → 'BUDGET'
--       FORECAST_TYPE IN ('FC','FORECAST')         → 'FORECAST'
--       CURVE_TYPE column IS NOT NULL in source    → UPPER(CURVE_TYPE)
--       Otherwise                                  → NULL (will be rejected)
--   * DT_PERIOD derived from PERIOD_DATE as YYYYMM.
--   * Any PENDING staging rows from prior failed runs are cleared first.
-- SCHEMA: GPC_DM
-- SOURCE: KBR_IHUB.APAC_PCDM_COST_IHUB
-- ============================================================

CREATE OR REPLACE PACKAGE GPC_DM.PKG_ETL_TRANSFORM_COST AS

    PROCEDURE transform(
        p_run_id         IN NUMBER,
        p_entity_id      IN NUMBER,
        p_from_watermark IN DATE,
        p_to_watermark   IN DATE
    );

END PKG_ETL_TRANSFORM_COST;
/


CREATE OR REPLACE PACKAGE BODY GPC_DM.PKG_ETL_TRANSFORM_COST AS

    PROCEDURE transform(
        p_run_id         IN NUMBER,
        p_entity_id      IN NUMBER,
        p_from_watermark IN DATE,
        p_to_watermark   IN DATE
    ) IS
        v_step_id    NUMBER;
        v_rows_c     NUMBER;
        v_rows_tc    NUMBER;
    BEGIN
        -- --------------------------------------------------------
        -- Clear any PENDING rows from prior failed runs.
        -- --------------------------------------------------------
        DELETE FROM GPC_DM.STG_COST          WHERE STG_STATUS = 'PENDING';
        DELETE FROM GPC_DM.STG_TIMELINE_COST WHERE STG_STATUS = 'PENDING';

        -- --------------------------------------------------------
        -- STAGE 1: Populate STG_COST
        -- Deduplication: latest REPORTING_DATE per COST_ID.
        -- Hash covers: PROJECT_ID, COST_CENTER, COST_TYPE,
        --              COST_CATEGORY, CURRENCY, BUDGET_VERSION
        -- --------------------------------------------------------
        v_step_id := GPC_DM.PKG_ETL_LOGGER.log_step(
            p_run_id, 'TRANSFORM:STG_COST', 'RUNNING');

        INSERT INTO GPC_DM.STG_COST (
            STG_RUN_ID,
            STG_STATUS,
            STG_RECORD_HASH,
            COST_ID,
            PROJECT_ID,
            COST_CENTER,
            COST_TYPE,
            COST_CATEGORY,
            CURRENCY,
            BUDGET_VERSION,
            REPORTING_DATE
        )
        SELECT
            p_run_id,
            'PENDING',
            STANDARD_HASH(
                COALESCE(PROJECT_ID,    'NULL') || CHR(1) ||
                COALESCE(COST_CENTER,   'NULL') || CHR(1) ||
                COALESCE(COST_TYPE,     'NULL') || CHR(1) ||
                COALESCE(COST_CATEGORY, 'NULL') || CHR(1) ||
                COALESCE(CURRENCY,      'NULL') || CHR(1) ||
                COALESCE(BUDGET_VERSION,'NULL'),
                'SHA256'
            ),
            COST_ID,
            PROJECT_ID,
            COST_CENTER,
            COST_TYPE,
            COST_CATEGORY,
            CURRENCY,
            BUDGET_VERSION,
            REPORTING_DATE
        FROM (
            SELECT
                COST_ID,
                PROJECT_ID,
                COST_CENTER,
                COST_TYPE,
                COST_CATEGORY,
                CURRENCY,
                BUDGET_VERSION,
                REPORTING_DATE,
                ROW_NUMBER() OVER (
                    PARTITION BY COST_ID
                    ORDER BY REPORTING_DATE DESC, ROWID DESC
                ) AS rn
            FROM   KBR_IHUB.APAC_PCDM_COST_IHUB
            WHERE  REPORTING_DATE >  p_from_watermark
            AND    REPORTING_DATE <= p_to_watermark
            AND    COST_ID        IS NOT NULL
        )
        WHERE rn = 1;

        v_rows_c := SQL%ROWCOUNT;

        GPC_DM.PKG_ETL_LOGGER.end_step(
            v_step_id, 'SUCCESS', v_rows_c,
            v_rows_c || ' row(s) staged for DIM_COST.'
        );

        -- --------------------------------------------------------
        -- STAGE 2: Populate STG_TIMELINE_COST
        -- Business key: COST_ID + DT_PERIOD (YYYYMM from PERIOD_DATE)
        -- Deduplication: latest REPORTING_DATE per COST_ID + period month.
        -- CURVE_TYPE derivation rule:
        --   1. FORECAST_TYPE = 'ACTUALS' or 'ACTUAL' → 'ACTUAL'
        --   2. BUDGET_VERSION starts with 'BUD'       → 'BUDGET'
        --   3. FORECAST_TYPE = 'FC' or 'FORECAST'     → 'FORECAST'
        --   4. Source CURVE_TYPE column not null       → UPPER(CURVE_TYPE)
        --   5. Otherwise                               → NULL (rejected)
        -- Hash covers: PERIOD_DATE, AMOUNT, FORECAST_TYPE
        -- --------------------------------------------------------
        v_step_id := GPC_DM.PKG_ETL_LOGGER.log_step(
            p_run_id, 'TRANSFORM:STG_TIMELINE_COST', 'RUNNING');

        INSERT INTO GPC_DM.STG_TIMELINE_COST (
            STG_RUN_ID,
            STG_STATUS,
            STG_RECORD_HASH,
            COST_ID,
            DT_PERIOD,
            PERIOD_DATE,
            AMOUNT,
            CURVE_TYPE,
            FORECAST_TYPE,
            REPORTING_DATE
        )
        SELECT
            p_run_id,
            'PENDING',
            STANDARD_HASH(
                COALESCE(TO_CHAR(PERIOD_DATE, 'YYYY-MM-DD'), 'NULL') || CHR(1) ||
                COALESCE(TO_CHAR(AMOUNT),                    'NULL') || CHR(1) ||
                COALESCE(FORECAST_TYPE,                      'NULL'),
                'SHA256'
            ),
            COST_ID,
            TO_CHAR(PERIOD_DATE, 'YYYYMM'),    -- DT_PERIOD; NULL if PERIOD_DATE is NULL
            PERIOD_DATE,
            AMOUNT,
            -- CURVE_TYPE derivation (ordered by specificity)
            CASE
                WHEN UPPER(FORECAST_TYPE) IN ('ACTUALS', 'ACTUAL')      THEN 'ACTUAL'
                WHEN BUDGET_VERSION       LIKE 'BUD%'                   THEN 'BUDGET'
                WHEN UPPER(FORECAST_TYPE) IN ('FC', 'FORECAST')         THEN 'FORECAST'
                WHEN CURVE_TYPE           IS NOT NULL                   THEN UPPER(CURVE_TYPE)
                ELSE NULL  -- validator will reject this row
            END                               AS CURVE_TYPE,
            FORECAST_TYPE,
            REPORTING_DATE
        FROM (
            SELECT
                COST_ID,
                PERIOD_DATE,
                AMOUNT,
                CURVE_TYPE,
                FORECAST_TYPE,
                BUDGET_VERSION,
                REPORTING_DATE,
                ROW_NUMBER() OVER (
                    PARTITION BY COST_ID, TRUNC(PERIOD_DATE, 'MM')
                    ORDER BY REPORTING_DATE DESC, ROWID DESC
                ) AS rn
            FROM   KBR_IHUB.APAC_PCDM_COST_IHUB
            WHERE  REPORTING_DATE >  p_from_watermark
            AND    REPORTING_DATE <= p_to_watermark
            AND    COST_ID        IS NOT NULL
            AND    PERIOD_DATE    IS NOT NULL
        )
        WHERE rn = 1;

        v_rows_tc := SQL%ROWCOUNT;

        GPC_DM.PKG_ETL_LOGGER.end_step(
            v_step_id, 'SUCCESS', v_rows_tc,
            v_rows_tc || ' row(s) staged for DIM_TIMELINE_COST.'
        );

    EXCEPTION
        WHEN OTHERS THEN
            GPC_DM.PKG_ETL_LOGGER.end_step(v_step_id, 'FAILED', 0, SQLERRM);
            RAISE;
    END transform;

END PKG_ETL_TRANSFORM_COST;
/
