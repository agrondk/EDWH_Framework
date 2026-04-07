-- ============================================================
-- IICS SQL Task: T3_RUN_COST
-- Purpose : Execute the full ETL load for the COST entity
--           for the given portfolio and reporting date.
--
-- Oracle handles internally:
--   1. GCS pre-process (stage → merge → GCS_KEY lookup)
--   2. Invalidate all DIM_COST rows for this
--      portfolio + reporting_date (full load per scope)
--   3. Insert pivoted COST CLASS rows into DIM_COST —
--      11 UNION ALL blocks producing one row per COST_CLASS:
--        COMPANY (5): OB COMPANY, BUDGET, EAC COMPANY,
--                     FORECAST, EARNED
--        CLIENT  (6): OB CLIENT, BUDGET CLIENT, EAC CLIENT,
--                     FORECAST CLIENT, EARNED CLIENT,
--                     ACTUAL CLIENT
--
-- On failure: Oracle issues ROLLBACK and raises an exception.
--             IICS detects the ORA- error, marks this task
--             FAILED, and routes to T5_CAPTURE_ERRORS.
-- ============================================================

BEGIN
    GPC_DM.PKG_COST_LOAD.load(
        p_portfolio_id   => '$$v_portfolio_id',
        p_reporting_date => TO_DATE('$$v_reporting_date', 'YYYY-MM-DD'),
        p_user           => '$$v_user'
    );
END;
