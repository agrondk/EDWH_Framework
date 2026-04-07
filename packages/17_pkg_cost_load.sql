-- ============================================================
-- FILE: 17_pkg_cost_load.sql
-- DESC: PKG_COST_LOAD
--       Dedicated load package for the COST entity.
--
-- Source : KBR_IHUB.APAC_PCDM_COST_IHUB
-- Target : GPC_DM.DIM_COST
--
-- GCS pre-process:
--   1. Insert distinct coding combinations into STG_GLOBAL_CODING_STRUCTURE
--   2. Call GPC_DM.PRC_MERGE_GLOBAL_CODING_STRUCTURE
--   3. Join REF_GLOBAL_CODING_STRUCTURE to get GCS_KEY per row
--
-- Load logic (Full Load per Portfolio + Reporting Date):
--   - Invalidate existing DIM_COST rows for the portfolio + reporting
--     date by setting IS_VALID = 0 and stamping DT_MODIFIED.
--   - Insert new rows with IS_VALID = 1.
--
-- Pivot logic (one source row → N target rows):
--   Each source row is expanded into one row per CLASS value.
--
--   COST_BASIS = 'COMPANY' → 5 rows:
--     OB COMPANY   : HOURS = ORIGINAL_BUDGET_HOURS,  COST = ORIGINAL_BUDGET_COST
--     BUDGET       : HOURS = CURRENT_BUDGET_HOURS,   COST = CURRENT_BUDGET_COST
--     EAC COMPANY  : HOURS = FORECAST_BUDGET_HOURS,  COST = FORECAST_BUDGET_COST
--     FORECAST     : HOURS = ETC_BUDGET_HOURS,       COST = ETC_BUDGET_COST
--     EARNED       : HOURS = EARNED_BUDGET_HOURS,    COST = EARNED_BUDGET_COST
--
--   COST_BASIS = 'CLIENT' → 6 rows:
--     OB CLIENT    : HOURS = ORIGINAL_BUDGET_HOURS,  COST = ORIGINAL_BUDGET_COST
--     BUDGET CLIENT: HOURS = CURRENT_BUDGET_HOURS,   COST = CURRENT_BUDGET_COST
--     EAC CLIENT   : HOURS = FORECAST_BUDGET_HOURS,  COST = FORECAST_BUDGET_COST
--     FORECAST CLIENT: HOURS = ETC_BUDGET_HOURS,     COST = ETC_BUDGET_COST
--     EARNED CLIENT: HOURS = EARNED_BUDGET_HOURS,    COST = EARNED_BUDGET_COST
--     ACTUAL CLIENT: HOURS = CLIENT_HOURS,           COST = VALUE_OF_WORK_DONE
--
-- SCHEMA: GPC_DM
-- ============================================================

CREATE OR REPLACE PACKAGE GPC_DM.PKG_COST_LOAD AS

    PROCEDURE load(
        p_portfolio_id   IN VARCHAR2,
        p_reporting_date IN DATE,
        p_user           IN VARCHAR2 DEFAULT 'ETL'
    );

END PKG_COST_LOAD;
/


CREATE OR REPLACE PACKAGE BODY GPC_DM.PKG_COST_LOAD AS

    PROCEDURE load(
        p_portfolio_id   IN VARCHAR2,
        p_reporting_date IN DATE,
        p_user           IN VARCHAR2 DEFAULT 'ETL'
    ) IS
    BEGIN

        -- ============================================================
        -- STEP 1 — GCS Pre-process
        -- ============================================================

        -- Clear stale staging rows for this portfolio from today
        DELETE FROM GPC_DM.STG_GLOBAL_CODING_STRUCTURE
        WHERE  PORTFOLIO_ID = p_portfolio_id
        AND    TRUNC(DT_CREATED) = TRUNC(SYSDATE);

        -- Insert distinct coding combinations from source (COST_FLAG = 1)
        INSERT INTO GPC_DM.STG_GLOBAL_CODING_STRUCTURE (
            PORTFOLIO_ID,
            PROJECT_ID,
            BUSINESS_UNIT_ID,
            OPERATING_CENTER_ID,
            BILL_TYPE_ID,
            WBS1_ID,
            WBS2_ID,
            COST_TYPE_ID,
            CBS_ID,
            COST_FLAG,
            IS_VALID,
            DT_CREATED,
            CREATED_BY
        )
        SELECT DISTINCT
            p_portfolio_id,
            s.PROJECT_ID,
            s.BUSINESS_UNIT,
            SUBSTR(s.EXECUTION_CENTER, 1, 2),
            SUBSTR(s.BILL_TYPE,        1, 2),
            s.WBS_1_CODE,
            s.WBS_2_CODE,
            SUBSTR(s.COST_TYPE, 1, 5),
            SUBSTR(s.CBS,       1, 7),
            1,     -- COST_FLAG
            1,     -- IS_VALID
            SYSTIMESTAMP,
            p_user
        FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
        WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
        AND   s.PORTFOLIO_ID          = p_portfolio_id;

        -- Merge staging into reference table
        GPC_DM.PRC_MERGE_GLOBAL_CODING_STRUCTURE;


        -- ============================================================
        -- STEP 2 — Invalidate existing DIM_COST rows
        --   Full load per Portfolio ID + Reporting Date:
        --   set IS_VALID = 0 for all existing rows in scope.
        -- ============================================================

        UPDATE GPC_DM.DIM_COST
        SET    IS_VALID    = 0,
               DT_MODIFIED = SYSDATE,
               MODIFIED_BY = p_user
        WHERE  PROJECT_ID          = p_portfolio_id
        AND    TRUNC(DT_REPORTING) = TRUNC(p_reporting_date)
        AND    IS_VALID            = 1;


        -- ============================================================
        -- STEP 3 — Insert pivoted DIM_COST rows (IS_VALID = 1)
        --
        -- Each source row is expanded into one row per CLASS via
        -- UNION ALL.  The COST_BASIS column determines which CLASS
        -- labels and which column pairs (HOURS, COST) to use.
        -- ============================================================

        INSERT INTO GPC_DM.DIM_COST (
            COST_KEY,
            GCS_KEY,
            DT_REPORTING,
            PROJECT_ID,
            BUSINESS_UNIT_ID,
            OPERATING_CENTER_ID,
            BILL_TYPE_ID,
            WBS1_ID,
            WBS2_ID,
            COST_TYPE_ID,
            CBS_ID,
            STS_GLOBAL_STRUCTURE,
            COST_BASIS,
            CURRENCY_CODE,
            CLASS,
            HOURS,
            COST,
            ACTUAL_PERCENT_COMPLETE,
            DT_BASELINE_START,
            DT_BASELINE_END,
            DT_FORECAST_START,
            DT_FORECAST_END,
            DATA_SOURCE,
            IS_VALID,
            DT_CREATED,
            CREATED_BY,
            DT_MODIFIED,
            MODIFIED_BY
        )
        SELECT
            GPC_DM.SEQ_DIM_COST.NEXTVAL,
            pvt.GCS_KEY,
            p_reporting_date,
            p_portfolio_id,                              -- PROJECT_ID = PORTFOLIO_ID
            pvt.BUSINESS_UNIT,
            SUBSTR(pvt.EXECUTION_CENTER, 1, 2),
            SUBSTR(pvt.BILL_TYPE,        1, 2),
            pvt.WBS_1_CODE,
            pvt.WBS_2_CODE,
            SUBSTR(pvt.COST_TYPE, 1, 5),
            SUBSTR(pvt.CBS,       1, 7),
            -- STS_GLOBAL_STRUCTURE composite key
            p_portfolio_id          || '.' ||
            pvt.PROJECT_ID          || '.' ||
            pvt.BUSINESS_UNIT       || '.' ||
            pvt.EXECUTION_CENTER    || '.' ||
            pvt.BILL_TYPE           || '.' ||
            pvt.WBS_1_CODE          || '.' ||
            pvt.WBS_2_CODE          || '.' ||
            pvt.COST_TYPE           || '.' ||
            pvt.CBS,
            pvt.COST_BASIS,
            pvt.CURRENCY_CODE,
            pvt.CLASS,
            pvt.HOURS,
            pvt.COST,
            pvt.ACTUAL_PERCENT_COMPLETE,
            pvt.BASELINE_START,
            pvt.BASELINE_END,
            pvt.FORECAST_START_DATE,
            pvt.FORECAST_END_DATE,
            'GENERIC LOAD',
            1,           -- IS_VALID
            SYSDATE,
            p_user,
            SYSDATE,
            p_user
        FROM (
            -- ── COMPANY rows (5 CLASS values) ─────────────────────
            SELECT
                s.PROJECT_ID, s.BUSINESS_UNIT, s.EXECUTION_CENTER,
                s.BILL_TYPE, s.WBS_1_CODE, s.WBS_2_CODE, s.COST_TYPE, s.CBS,
                s.COST_BASIS, s.CURRENCY_CODE,
                s.ACTUAL_PERCENT_COMPLETE, s.BASELINE_START, s.BASELINE_END,
                s.FORECAST_START_DATE, s.FORECAST_END_DATE,
                r.GCS_KEY,
                'OB COMPANY'             AS CLASS,
                s.ORIGINAL_BUDGET_HOURS  AS HOURS,
                s.ORIGINAL_BUDGET_COST   AS COST
            FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r ON
                r.PORTFOLIO_ID        = p_portfolio_id
                AND r.PROJECT_ID          = s.PROJECT_ID
                AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                AND r.WBS1_ID             = s.WBS_1_CODE
                AND r.WBS2_ID             = s.WBS_2_CODE
                AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                AND r.IS_VALID            = 1
            WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            AND   s.PORTFOLIO_ID          = p_portfolio_id
            AND   UPPER(s.COST_BASIS)     = 'COMPANY'

            UNION ALL

            SELECT s.PROJECT_ID, s.BUSINESS_UNIT, s.EXECUTION_CENTER,
                s.BILL_TYPE, s.WBS_1_CODE, s.WBS_2_CODE, s.COST_TYPE, s.CBS,
                s.COST_BASIS, s.CURRENCY_CODE,
                s.ACTUAL_PERCENT_COMPLETE, s.BASELINE_START, s.BASELINE_END,
                s.FORECAST_START_DATE, s.FORECAST_END_DATE, r.GCS_KEY,
                'BUDGET',
                s.CURRENT_BUDGET_HOURS,
                s.CURRENT_BUDGET_COST
            FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r ON
                r.PORTFOLIO_ID        = p_portfolio_id
                AND r.PROJECT_ID          = s.PROJECT_ID
                AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                AND r.WBS1_ID             = s.WBS_1_CODE
                AND r.WBS2_ID             = s.WBS_2_CODE
                AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                AND r.IS_VALID            = 1
            WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            AND   s.PORTFOLIO_ID          = p_portfolio_id
            AND   UPPER(s.COST_BASIS)     = 'COMPANY'

            UNION ALL

            SELECT s.PROJECT_ID, s.BUSINESS_UNIT, s.EXECUTION_CENTER,
                s.BILL_TYPE, s.WBS_1_CODE, s.WBS_2_CODE, s.COST_TYPE, s.CBS,
                s.COST_BASIS, s.CURRENCY_CODE,
                s.ACTUAL_PERCENT_COMPLETE, s.BASELINE_START, s.BASELINE_END,
                s.FORECAST_START_DATE, s.FORECAST_END_DATE, r.GCS_KEY,
                'EAC COMPANY',
                s.FORECAST_BUDGET_HOURS,
                s.FORECAST_BUDGET_COST
            FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r ON
                r.PORTFOLIO_ID        = p_portfolio_id
                AND r.PROJECT_ID          = s.PROJECT_ID
                AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                AND r.WBS1_ID             = s.WBS_1_CODE
                AND r.WBS2_ID             = s.WBS_2_CODE
                AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                AND r.IS_VALID            = 1
            WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            AND   s.PORTFOLIO_ID          = p_portfolio_id
            AND   UPPER(s.COST_BASIS)     = 'COMPANY'

            UNION ALL

            SELECT s.PROJECT_ID, s.BUSINESS_UNIT, s.EXECUTION_CENTER,
                s.BILL_TYPE, s.WBS_1_CODE, s.WBS_2_CODE, s.COST_TYPE, s.CBS,
                s.COST_BASIS, s.CURRENCY_CODE,
                s.ACTUAL_PERCENT_COMPLETE, s.BASELINE_START, s.BASELINE_END,
                s.FORECAST_START_DATE, s.FORECAST_END_DATE, r.GCS_KEY,
                'FORECAST',
                s.ETC_BUDGET_HOURS,
                s.ETC_BUDGET_COST
            FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r ON
                r.PORTFOLIO_ID        = p_portfolio_id
                AND r.PROJECT_ID          = s.PROJECT_ID
                AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                AND r.WBS1_ID             = s.WBS_1_CODE
                AND r.WBS2_ID             = s.WBS_2_CODE
                AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                AND r.IS_VALID            = 1
            WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            AND   s.PORTFOLIO_ID          = p_portfolio_id
            AND   UPPER(s.COST_BASIS)     = 'COMPANY'

            UNION ALL

            SELECT s.PROJECT_ID, s.BUSINESS_UNIT, s.EXECUTION_CENTER,
                s.BILL_TYPE, s.WBS_1_CODE, s.WBS_2_CODE, s.COST_TYPE, s.CBS,
                s.COST_BASIS, s.CURRENCY_CODE,
                s.ACTUAL_PERCENT_COMPLETE, s.BASELINE_START, s.BASELINE_END,
                s.FORECAST_START_DATE, s.FORECAST_END_DATE, r.GCS_KEY,
                'EARNED',
                s.EARNED_BUDGET_HOURS,
                s.EARNED_BUDGET_COST
            FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r ON
                r.PORTFOLIO_ID        = p_portfolio_id
                AND r.PROJECT_ID          = s.PROJECT_ID
                AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                AND r.WBS1_ID             = s.WBS_1_CODE
                AND r.WBS2_ID             = s.WBS_2_CODE
                AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                AND r.IS_VALID            = 1
            WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            AND   s.PORTFOLIO_ID          = p_portfolio_id
            AND   UPPER(s.COST_BASIS)     = 'COMPANY'

            -- ── CLIENT rows (6 CLASS values) ──────────────────────

            UNION ALL

            SELECT s.PROJECT_ID, s.BUSINESS_UNIT, s.EXECUTION_CENTER,
                s.BILL_TYPE, s.WBS_1_CODE, s.WBS_2_CODE, s.COST_TYPE, s.CBS,
                s.COST_BASIS, s.CURRENCY_CODE,
                s.ACTUAL_PERCENT_COMPLETE, s.BASELINE_START, s.BASELINE_END,
                s.FORECAST_START_DATE, s.FORECAST_END_DATE, r.GCS_KEY,
                'OB CLIENT',
                s.ORIGINAL_BUDGET_HOURS,
                s.ORIGINAL_BUDGET_COST
            FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r ON
                r.PORTFOLIO_ID        = p_portfolio_id
                AND r.PROJECT_ID          = s.PROJECT_ID
                AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                AND r.WBS1_ID             = s.WBS_1_CODE
                AND r.WBS2_ID             = s.WBS_2_CODE
                AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                AND r.IS_VALID            = 1
            WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            AND   s.PORTFOLIO_ID          = p_portfolio_id
            AND   UPPER(s.COST_BASIS)     = 'CLIENT'

            UNION ALL

            SELECT s.PROJECT_ID, s.BUSINESS_UNIT, s.EXECUTION_CENTER,
                s.BILL_TYPE, s.WBS_1_CODE, s.WBS_2_CODE, s.COST_TYPE, s.CBS,
                s.COST_BASIS, s.CURRENCY_CODE,
                s.ACTUAL_PERCENT_COMPLETE, s.BASELINE_START, s.BASELINE_END,
                s.FORECAST_START_DATE, s.FORECAST_END_DATE, r.GCS_KEY,
                'BUDGET CLIENT',
                s.CURRENT_BUDGET_HOURS,
                s.CURRENT_BUDGET_COST
            FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r ON
                r.PORTFOLIO_ID        = p_portfolio_id
                AND r.PROJECT_ID          = s.PROJECT_ID
                AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                AND r.WBS1_ID             = s.WBS_1_CODE
                AND r.WBS2_ID             = s.WBS_2_CODE
                AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                AND r.IS_VALID            = 1
            WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            AND   s.PORTFOLIO_ID          = p_portfolio_id
            AND   UPPER(s.COST_BASIS)     = 'CLIENT'

            UNION ALL

            SELECT s.PROJECT_ID, s.BUSINESS_UNIT, s.EXECUTION_CENTER,
                s.BILL_TYPE, s.WBS_1_CODE, s.WBS_2_CODE, s.COST_TYPE, s.CBS,
                s.COST_BASIS, s.CURRENCY_CODE,
                s.ACTUAL_PERCENT_COMPLETE, s.BASELINE_START, s.BASELINE_END,
                s.FORECAST_START_DATE, s.FORECAST_END_DATE, r.GCS_KEY,
                'EAC CLIENT',
                s.FORECAST_BUDGET_HOURS,
                s.FORECAST_BUDGET_COST
            FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r ON
                r.PORTFOLIO_ID        = p_portfolio_id
                AND r.PROJECT_ID          = s.PROJECT_ID
                AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                AND r.WBS1_ID             = s.WBS_1_CODE
                AND r.WBS2_ID             = s.WBS_2_CODE
                AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                AND r.IS_VALID            = 1
            WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            AND   s.PORTFOLIO_ID          = p_portfolio_id
            AND   UPPER(s.COST_BASIS)     = 'CLIENT'

            UNION ALL

            SELECT s.PROJECT_ID, s.BUSINESS_UNIT, s.EXECUTION_CENTER,
                s.BILL_TYPE, s.WBS_1_CODE, s.WBS_2_CODE, s.COST_TYPE, s.CBS,
                s.COST_BASIS, s.CURRENCY_CODE,
                s.ACTUAL_PERCENT_COMPLETE, s.BASELINE_START, s.BASELINE_END,
                s.FORECAST_START_DATE, s.FORECAST_END_DATE, r.GCS_KEY,
                'FORECAST CLIENT',
                s.ETC_BUDGET_HOURS,
                s.ETC_BUDGET_COST
            FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r ON
                r.PORTFOLIO_ID        = p_portfolio_id
                AND r.PROJECT_ID          = s.PROJECT_ID
                AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                AND r.WBS1_ID             = s.WBS_1_CODE
                AND r.WBS2_ID             = s.WBS_2_CODE
                AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                AND r.IS_VALID            = 1
            WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            AND   s.PORTFOLIO_ID          = p_portfolio_id
            AND   UPPER(s.COST_BASIS)     = 'CLIENT'

            UNION ALL

            SELECT s.PROJECT_ID, s.BUSINESS_UNIT, s.EXECUTION_CENTER,
                s.BILL_TYPE, s.WBS_1_CODE, s.WBS_2_CODE, s.COST_TYPE, s.CBS,
                s.COST_BASIS, s.CURRENCY_CODE,
                s.ACTUAL_PERCENT_COMPLETE, s.BASELINE_START, s.BASELINE_END,
                s.FORECAST_START_DATE, s.FORECAST_END_DATE, r.GCS_KEY,
                'EARNED CLIENT',
                s.EARNED_BUDGET_HOURS,
                s.EARNED_BUDGET_COST
            FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r ON
                r.PORTFOLIO_ID        = p_portfolio_id
                AND r.PROJECT_ID          = s.PROJECT_ID
                AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                AND r.WBS1_ID             = s.WBS_1_CODE
                AND r.WBS2_ID             = s.WBS_2_CODE
                AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                AND r.IS_VALID            = 1
            WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            AND   s.PORTFOLIO_ID          = p_portfolio_id
            AND   UPPER(s.COST_BASIS)     = 'CLIENT'

            UNION ALL

            -- ACTUAL CLIENT: CLIENT_HOURS + VALUE_OF_WORK_DONE
            SELECT s.PROJECT_ID, s.BUSINESS_UNIT, s.EXECUTION_CENTER,
                s.BILL_TYPE, s.WBS_1_CODE, s.WBS_2_CODE, s.COST_TYPE, s.CBS,
                s.COST_BASIS, s.CURRENCY_CODE,
                s.ACTUAL_PERCENT_COMPLETE, s.BASELINE_START, s.BASELINE_END,
                s.FORECAST_START_DATE, s.FORECAST_END_DATE, r.GCS_KEY,
                'ACTUAL CLIENT',
                s.CLIENT_HOURS,
                s.VALUE_OF_WORK_DONE
            FROM KBR_IHUB.APAC_PCDM_COST_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r ON
                r.PORTFOLIO_ID        = p_portfolio_id
                AND r.PROJECT_ID          = s.PROJECT_ID
                AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                AND r.WBS1_ID             = s.WBS_1_CODE
                AND r.WBS2_ID             = s.WBS_2_CODE
                AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                AND r.IS_VALID            = 1
            WHERE TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            AND   s.PORTFOLIO_ID          = p_portfolio_id
            AND   UPPER(s.COST_BASIS)     = 'CLIENT'

        ) pvt;


        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load;

END PKG_COST_LOAD;
/
