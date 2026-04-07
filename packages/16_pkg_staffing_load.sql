-- ============================================================
-- FILE: 16_pkg_staffing_load.sql
-- DESC: PKG_STAFFING_LOAD
--       Dedicated load package for STAFFING_SCHEDULE and
--       STAFFING_TIMELINE entities.
--
-- Source : KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB
-- Targets: GPC_DM.DIM_STAFFING_SCHEDULE  (is_valid flag management)
--          GPC_DM.DIM_STAFFING_TIMELINE   (is_valid + period unpivot)
--
-- GCS pre-process:
--   1. Insert distinct coding combinations into STG_GLOBAL_CODING_STRUCTURE
--   2. Call GPC_DM.PRC_MERGE_GLOBAL_CODING_STRUCTURE
--   3. Join REF_GLOBAL_CODING_STRUCTURE to get GCS_KEY per row
--
-- Import modes:
--   REPLACE_ALL : Invalidate ALL existing rows for
--                 Portfolio + Execution Center + Reporting Date,
--                 then insert all incoming rows as IS_VALID = 1.
--   ADD_UPDATE  : Invalidate only rows whose GCS + employee + position
--                 combination matches an incoming row, then insert
--                 all incoming rows as IS_VALID = 1.
--                 Rows NOT present in the file are left untouched.
--
-- Timeline expansion:
--   For each source row, one DIM_STAFFING_TIMELINE row is generated
--   per calendar month covering the union of PLAN and FORECAST date
--   ranges.  PLAN_HOURS = HOURS_PER_WEEK for periods within the plan
--   date range; FORECAST_HOURS = HOURS_PER_WEEK for periods within
--   the forecast date range.
--
-- SCHEMA: GPC_DM
-- ============================================================

CREATE OR REPLACE PACKAGE GPC_DM.PKG_STAFFING_LOAD AS

    PROCEDURE load(
        p_portfolio_id     IN VARCHAR2,
        p_reporting_date   IN DATE,
        p_execution_center IN VARCHAR2,
        p_import_mode      IN VARCHAR2 DEFAULT 'REPLACE_ALL',
        p_user             IN VARCHAR2 DEFAULT 'ETL'
    );

END PKG_STAFFING_LOAD;
/


CREATE OR REPLACE PACKAGE BODY GPC_DM.PKG_STAFFING_LOAD AS

    PROCEDURE load(
        p_portfolio_id     IN VARCHAR2,
        p_reporting_date   IN DATE,
        p_execution_center IN VARCHAR2,
        p_import_mode      IN VARCHAR2 DEFAULT 'REPLACE_ALL',
        p_user             IN VARCHAR2 DEFAULT 'ETL'
    ) IS
        v_now         TIMESTAMP := SYSTIMESTAMP;
        v_exec_center VARCHAR2(2) := SUBSTR(p_execution_center, 1, 2);
    BEGIN

        -- ============================================================
        -- STEP 1 — GCS Pre-process
        --   Insert distinct coding structure combinations from source
        --   into STG_GLOBAL_CODING_STRUCTURE (STAFFING_FLAG = 1),
        --   then call the merge procedure so REF_GLOBAL_CODING_STRUCTURE
        --   is populated/updated before we look up GCS_KEY.
        -- ============================================================

        -- Clear any stale staging rows for this portfolio from today
        DELETE FROM GPC_DM.STG_GLOBAL_CODING_STRUCTURE
        WHERE  PORTFOLIO_ID = p_portfolio_id
        AND    TRUNC(DT_CREATED) = TRUNC(SYSDATE);

        -- Insert distinct combinations from source
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
            STAFFING_FLAG,
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
            1,    -- STAFFING_FLAG
            1,    -- IS_VALID
            SYSTIMESTAMP,
            p_user
        FROM KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB s
        WHERE s.PORTFOLIO_ID        = p_portfolio_id
        AND   TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date);

        -- Merge staging into reference table
        GPC_DM.PRC_MERGE_GLOBAL_CODING_STRUCTURE;


        -- ============================================================
        -- STEP 2 — Invalidate existing DIM_STAFFING_SCHEDULE rows
        -- ============================================================

        IF p_import_mode = 'REPLACE_ALL' THEN
            -- Invalidate all rows for this portfolio + execution center
            -- + reporting date scope
            UPDATE GPC_DM.DIM_STAFFING_SCHEDULE
            SET    IS_VALID    = 0,
                   DT_MODIFIED = SYSTIMESTAMP,
                   MODIFIED_BY = p_user
            WHERE  PROJECT_ID          = p_portfolio_id
            AND    TRUNC(DT_REPORTING) = TRUNC(p_reporting_date)
            AND    OPERATING_CENTER_ID = v_exec_center
            AND    IS_VALID            = 1;

        ELSE  -- ADD_UPDATE
            -- Invalidate only rows whose GCS_KEY + EMPLOYEE_ID + POSITION_NUMBER
            -- matches an incoming row
            UPDATE GPC_DM.DIM_STAFFING_SCHEDULE d
            SET    IS_VALID    = 0,
                   DT_MODIFIED = SYSTIMESTAMP,
                   MODIFIED_BY = p_user
            WHERE  d.PROJECT_ID          = p_portfolio_id
            AND    TRUNC(d.DT_REPORTING) = TRUNC(p_reporting_date)
            AND    d.IS_VALID            = 1
            AND EXISTS (
                SELECT 1
                FROM   KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB s
                JOIN   GPC_DM.REF_GLOBAL_CODING_STRUCTURE r
                    ON r.PORTFOLIO_ID        = p_portfolio_id
                   AND r.PROJECT_ID          = s.PROJECT_ID
                   AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                   AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                   AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                   AND r.WBS1_ID             = s.WBS_1_CODE
                   AND r.WBS2_ID             = s.WBS_2_CODE
                   AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                   AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                   AND r.IS_VALID            = 1
                WHERE  s.PORTFOLIO_ID        = p_portfolio_id
                AND    TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
                AND    r.GCS_KEY          = d.GCS_KEY
                AND    s.EMPLOYEE_ID      = d.EMPLOYEE_ID
                AND    s.POSITION_NUMBER  = d.POSITION_NUMBER
            );
        END IF;


        -- ============================================================
        -- STEP 3 — Insert new DIM_STAFFING_SCHEDULE rows (IS_VALID = 1)
        -- ============================================================

        INSERT INTO GPC_DM.DIM_STAFFING_SCHEDULE (
            STAFFING_SCHEDULE_KEY,
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
            POSITION_NUMBER,
            POSITION_TITLE,
            FULLTIME_PARTIME,
            EMPLOYEE_ID,
            EMPLOYEE_NAME,
            CONTRACT_TYPE,
            JOB_TITLE,
            PRIORITY,
            DT_PLAN_START,
            DT_PLAN_END,
            DT_FORECAST_START,
            DT_FORECAST_END,
            DT_ACTUAL_START,
            DT_ACTUAL_END,
            HOURS_PER_WEEK,
            STATUS,
            NEW_HIRE,
            IS_VALID,
            IS_CURRENT,
            DT_CREATED,
            CREATED_BY,
            DT_MODIFIED,
            MODIFIED_BY
        )
        SELECT
            GPC_DM.SEQ_DIM_SS.NEXTVAL,
            r.GCS_KEY,
            p_reporting_date,
            p_portfolio_id,                              -- PROJECT_ID = PORTFOLIO_ID
            s.BUSINESS_UNIT,
            SUBSTR(s.EXECUTION_CENTER, 1, 2),
            SUBSTR(s.BILL_TYPE,        1, 2),
            s.WBS_1_CODE,
            s.WBS_2_CODE,
            SUBSTR(s.COST_TYPE, 1, 5),
            SUBSTR(s.CBS,       1, 7),
            -- STS_GLOBAL_STRUCTURE composite key
            p_portfolio_id      || '.' ||
            s.PROJECT_ID        || '.' ||
            s.BUSINESS_UNIT     || '.' ||
            s.EXECUTION_CENTER  || '.' ||
            s.BILL_TYPE         || '.' ||
            s.WBS_1_CODE        || '.' ||
            s.WBS_2_CODE        || '.' ||
            s.COST_TYPE         || '.' ||
            s.CBS,
            s.POSITION_NUMBER,
            s.POSITION_TITLE,
            s.FULL_PART_TIME,
            s.EMPLOYEE_ID,
            s.EMPLOYEE_NAME,
            s.CONTRACT_TYPE,
            s.JOB_TITLE,
            s.PRIORITY,
            s.PLAN_START_DATE,
            s.PLAN_END_DATE,
            s.FORECAST_START_DATE,
            s.FORECAST_END_DATE,
            s.ACTUAL_START_DATE,
            s.ACTUAL_END_DATE,
            s.HOURS_PER_WEEK,
            s.STATUS,
            s.NEW_HIRE,
            1,               -- IS_VALID
            1,               -- IS_CURRENT
            SYSTIMESTAMP,
            p_user,
            SYSTIMESTAMP,
            p_user
        FROM KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB s
        JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r
          ON r.PORTFOLIO_ID        = p_portfolio_id
         AND r.PROJECT_ID          = s.PROJECT_ID
         AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
         AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
         AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
         AND r.WBS1_ID             = s.WBS_1_CODE
         AND r.WBS2_ID             = s.WBS_2_CODE
         AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
         AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
         AND r.IS_VALID            = 1
        WHERE s.PORTFOLIO_ID        = p_portfolio_id
        AND   TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date);


        -- ============================================================
        -- STEP 4 — Invalidate existing DIM_STAFFING_TIMELINE rows
        -- ============================================================

        IF p_import_mode = 'REPLACE_ALL' THEN
            UPDATE GPC_DM.DIM_STAFFING_TIMELINE
            SET    IS_VALID    = 0,
                   DT_MODIFIED = SYSTIMESTAMP,
                   MODIFIED_BY = p_user
            WHERE  PROJECT_ID          = p_portfolio_id
            AND    TRUNC(DT_REPORTING) = TRUNC(p_reporting_date)
            AND    IS_VALID            = 1;

        ELSE  -- ADD_UPDATE
            UPDATE GPC_DM.DIM_STAFFING_TIMELINE t
            SET    IS_VALID    = 0,
                   DT_MODIFIED = SYSTIMESTAMP,
                   MODIFIED_BY = p_user
            WHERE  t.PROJECT_ID          = p_portfolio_id
            AND    TRUNC(t.DT_REPORTING) = TRUNC(p_reporting_date)
            AND    t.IS_VALID            = 1
            AND EXISTS (
                SELECT 1
                FROM   KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB s
                JOIN   GPC_DM.REF_GLOBAL_CODING_STRUCTURE r
                    ON r.PORTFOLIO_ID        = p_portfolio_id
                   AND r.PROJECT_ID          = s.PROJECT_ID
                   AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
                   AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
                   AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
                   AND r.WBS1_ID             = s.WBS_1_CODE
                   AND r.WBS2_ID             = s.WBS_2_CODE
                   AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
                   AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
                   AND r.IS_VALID            = 1
                WHERE  s.PORTFOLIO_ID        = p_portfolio_id
                AND    TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
                AND    r.GCS_KEY         = t.GCS_KEY
                AND    s.EMPLOYEE_ID     = t.EMPLOYEE_ID
                AND    s.POSITION_NUMBER = t.POSITION_NUMBER
            );
        END IF;


        -- ============================================================
        -- STEP 5 — Insert DIM_STAFFING_TIMELINE rows
        --
        -- Period expansion: one row per calendar month covering the
        -- union of [PLAN_START_DATE..PLAN_END_DATE] and
        --          [FORECAST_START_DATE..FORECAST_END_DATE].
        --
        -- A numbers generator (DUAL CONNECT BY LEVEL <= 360) is
        -- cross-joined to each source row and filtered to only months
        -- within that row's date range.  360 months = 30 years max.
        --
        -- PLAN_HOURS     = HOURS_PER_WEEK if period falls within plan range
        -- FORECAST_HOURS = HOURS_PER_WEEK if period falls within forecast range
        -- ============================================================

        INSERT INTO GPC_DM.DIM_STAFFING_TIMELINE (
            STAFFING_TIMELINE_KEY,
            GCS_KEY,
            DT_REPORTING,
            DT_PERIOD,
            PROJECT_ID,
            POSITION_NUMBER,
            POSITION_TITLE,
            EMPLOYEE_ID,
            EMPLOYEE_NAME,
            PLAN_HOURS,
            FORECAST_HOURS,
            IS_VALID,
            IS_CURRENT,
            DT_CREATED,
            CREATED_BY,
            DT_MODIFIED,
            MODIFIED_BY
        )
        SELECT
            GPC_DM.SEQ_DIM_ST.NEXTVAL,
            src.GCS_KEY,
            p_reporting_date,
            ADD_MONTHS(src.range_start, nums.n),   -- DT_PERIOD (first day of month)
            p_portfolio_id,
            src.POSITION_NUMBER,
            src.POSITION_TITLE,
            src.EMPLOYEE_ID,
            src.EMPLOYEE_NAME,
            -- PLAN_HOURS: apply HOURS_PER_WEEK only if period is within plan range
            CASE
                WHEN src.plan_start IS NOT NULL
                 AND ADD_MONTHS(src.range_start, nums.n)
                     BETWEEN src.plan_start AND src.plan_end
                THEN src.HOURS_PER_WEEK
                ELSE NULL
            END,
            -- FORECAST_HOURS: apply HOURS_PER_WEEK only if period is within forecast range
            CASE
                WHEN src.forecast_start IS NOT NULL
                 AND ADD_MONTHS(src.range_start, nums.n)
                     BETWEEN src.forecast_start AND src.forecast_end
                THEN src.HOURS_PER_WEEK
                ELSE NULL
            END,
            1,              -- IS_VALID
            1,              -- IS_CURRENT
            SYSTIMESTAMP,
            p_user,
            SYSTIMESTAMP,
            p_user
        FROM (
            -- Compute per-row date range bounds and attach GCS_KEY
            SELECT
                s.POSITION_NUMBER,
                s.POSITION_TITLE,
                s.EMPLOYEE_ID,
                s.EMPLOYEE_NAME,
                s.HOURS_PER_WEEK,
                r.GCS_KEY,
                -- Rounded plan range
                TRUNC(s.PLAN_START_DATE, 'MM')     AS plan_start,
                TRUNC(s.PLAN_END_DATE,   'MM')     AS plan_end,
                -- Rounded forecast range
                TRUNC(s.FORECAST_START_DATE, 'MM') AS forecast_start,
                TRUNC(s.FORECAST_END_DATE,   'MM') AS forecast_end,
                -- Overall range start: earliest of plan/forecast starts
                TRUNC(
                    CASE
                        WHEN s.PLAN_START_DATE IS NOT NULL
                         AND s.FORECAST_START_DATE IS NOT NULL
                            THEN LEAST(s.PLAN_START_DATE, s.FORECAST_START_DATE)
                        WHEN s.PLAN_START_DATE IS NOT NULL
                            THEN s.PLAN_START_DATE
                        ELSE s.FORECAST_START_DATE
                    END, 'MM'
                ) AS range_start,
                -- Overall range end: latest of plan/forecast ends
                TRUNC(
                    CASE
                        WHEN s.PLAN_END_DATE IS NOT NULL
                         AND s.FORECAST_END_DATE IS NOT NULL
                            THEN GREATEST(s.PLAN_END_DATE, s.FORECAST_END_DATE)
                        WHEN s.PLAN_END_DATE IS NOT NULL
                            THEN s.PLAN_END_DATE
                        ELSE s.FORECAST_END_DATE
                    END, 'MM'
                ) AS range_end,
                -- Month count for this row
                MONTHS_BETWEEN(
                    TRUNC(
                        CASE
                            WHEN s.PLAN_END_DATE IS NOT NULL
                             AND s.FORECAST_END_DATE IS NOT NULL
                                THEN GREATEST(s.PLAN_END_DATE, s.FORECAST_END_DATE)
                            WHEN s.PLAN_END_DATE IS NOT NULL THEN s.PLAN_END_DATE
                            ELSE s.FORECAST_END_DATE
                        END, 'MM'
                    ),
                    TRUNC(
                        CASE
                            WHEN s.PLAN_START_DATE IS NOT NULL
                             AND s.FORECAST_START_DATE IS NOT NULL
                                THEN LEAST(s.PLAN_START_DATE, s.FORECAST_START_DATE)
                            WHEN s.PLAN_START_DATE IS NOT NULL THEN s.PLAN_START_DATE
                            ELSE s.FORECAST_START_DATE
                        END, 'MM'
                    )
                ) AS month_count
            FROM KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB s
            JOIN GPC_DM.REF_GLOBAL_CODING_STRUCTURE r
              ON r.PORTFOLIO_ID        = p_portfolio_id
             AND r.PROJECT_ID          = s.PROJECT_ID
             AND r.BUSINESS_UNIT_ID    = s.BUSINESS_UNIT
             AND r.OPERATING_CENTER_ID = SUBSTR(s.EXECUTION_CENTER, 1, 2)
             AND r.BILL_TYPE_ID        = SUBSTR(s.BILL_TYPE,        1, 2)
             AND r.WBS1_ID             = s.WBS_1_CODE
             AND r.WBS2_ID             = s.WBS_2_CODE
             AND r.COST_TYPE_ID        = SUBSTR(s.COST_TYPE, 1, 5)
             AND r.CBS_ID              = SUBSTR(s.CBS,       1, 7)
             AND r.IS_VALID            = 1
            WHERE s.PORTFOLIO_ID        = p_portfolio_id
            AND   TRUNC(s.REPORTING_DATE) = TRUNC(p_reporting_date)
            -- Only include rows that have at least one date range to expand
            AND (s.PLAN_START_DATE IS NOT NULL OR s.FORECAST_START_DATE IS NOT NULL)
        ) src
        -- Numbers generator: 0 to 359 (up to 30 years of monthly periods)
        CROSS JOIN (
            SELECT LEVEL - 1 AS n
            FROM   DUAL
            CONNECT BY LEVEL <= 360
        ) nums
        -- Filter: only generate periods within this row's range
        WHERE nums.n <= src.month_count;


        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END load;

END PKG_STAFFING_LOAD;
/
