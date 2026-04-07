-- ============================================================
-- IICS SQL Task: T2_RUN_STAFFING
-- Purpose : Execute the full ETL load for the STAFFING_SCHEDULE
--           entity for the given portfolio and reporting date.
--
-- Oracle handles internally:
--   1. GCS pre-process (stage → merge → GCS_KEY lookup)
--   2. Invalidate DIM_STAFFING_SCHEDULE rows in scope
--   3. Insert current DIM_STAFFING_SCHEDULE rows (IS_VALID=1)
--   4. Invalidate DIM_STAFFING_TIMELINE rows in scope
--   5. Insert period rows into DIM_STAFFING_TIMELINE
--      (one row per calendar month via CONNECT BY unpivot)
--
-- Import modes:
--   REPLACE_ALL — full reload: invalidate all rows for this
--                 portfolio + reporting_date before inserting
--   ADD_UPDATE  — incremental: invalidate only matching
--                 GCS_KEY + EMPLOYEE_ID + POSITION_NUMBER
--
-- On failure: Oracle issues ROLLBACK and raises an exception.
--             IICS detects the ORA- error, marks this task
--             FAILED, and routes to T5_CAPTURE_ERRORS.
-- ============================================================

BEGIN
    GPC_DM.PKG_STAFFING_LOAD.load(
        p_portfolio_id     => '$$v_portfolio_id',
        p_reporting_date   => TO_DATE('$$v_reporting_date', 'YYYY-MM-DD'),
        p_execution_center => '$$v_execution_center',
        p_import_mode      => '$$v_import_mode',
        p_user             => '$$v_user'
    );
END;
