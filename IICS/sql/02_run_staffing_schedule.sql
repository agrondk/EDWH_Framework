-- ============================================================
-- IICS SQL Task: T2_RUN_STAFFING
-- Purpose : Execute the full ETL pipeline for the
--           STAFFING_SCHEDULE entity.
--
-- Oracle handles internally:
--   1. Incremental extract from KBR_IHUB source table
--   2. Transform and hash into STG_STAFFING_SCHEDULE
--      and STG_STAFFING_TIMELINE
--   3. Validate staging rows (NOT_NULL, DERIVED, CHECK rules)
--   4. SCD2 load into DIM_STAFFING_SCHEDULE
--   5. Incremental MERGE into DIM_STAFFING_TIMELINE
--   6. Advance watermark on success
--
-- On failure: Oracle raises RAISE_APPLICATION_ERROR.
--             IICS detects the ORA- exception and marks
--             this task FAILED, routing to T5_CAPTURE_ERRORS.
-- ============================================================

BEGIN
    GPC_DM.PKG_ETL_ORCHESTRATOR.run_entity('STAFFING_SCHEDULE');
END;
