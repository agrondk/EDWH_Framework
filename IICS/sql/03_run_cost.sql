-- ============================================================
-- IICS SQL Task: T3_RUN_COST
-- Purpose : Execute the full ETL pipeline for the
--           COST entity.
--
-- Oracle handles internally:
--   1. Incremental extract from KBR_IHUB.APAC_PCDM_COST_IHUB
--   2. Transform and hash into STG_COST and STG_TIMELINE_COST
--   3. Validate staging rows (NOT_NULL, DERIVED, CHECK rules)
--      incl. COST_CATEGORY IN (COMPANY, CLIENT)
--      and   CURVE_TYPE IN (ACTUAL, BUDGET, FORECAST)
--   4. SCD2 load into DIM_COST
--   5. Incremental MERGE into DIM_TIMELINE_COST
--   6. Advance watermark on success
--
-- On failure: Oracle raises RAISE_APPLICATION_ERROR.
--             IICS detects the ORA- exception and marks
--             this task FAILED, routing to T5_CAPTURE_ERRORS.
-- ============================================================

BEGIN
    GPC_DM.PKG_ETL_ORCHESTRATOR.run_entity('COST');
END;
