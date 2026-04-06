-- ============================================================
-- FILE: 00_install_all.sql
-- DESC: Master install script — executes all framework files
--       in the correct dependency order.
--
-- Run this script as the GPC_DM schema owner (or a DBA granting
-- CREATE TABLE, CREATE SEQUENCE, CREATE VIEW, CREATE PROCEDURE
-- privileges to GPC_DM).
--
-- Prerequisites:
--   * GPC_DM schema must exist
--   * KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB must be accessible
--   * KBR_IHUB.APAC_PCDM_COST_IHUB must be accessible
--   * SELECT privilege on KBR_IHUB source tables granted to GPC_DM
--
-- Usage (SQL*Plus / SQLcl):
--   @00_install_all.sql
--
-- Usage (SQLcl with a specific path):
--   @/path/to/EDWH_Framework/00_install_all.sql
-- ============================================================

WHENEVER SQLERROR EXIT FAILURE ROLLBACK;
SET ECHO ON
SET FEEDBACK ON

PROMPT =============================================================
PROMPT  STEP 1 — Sequences
PROMPT =============================================================
@@ddl/01_sequences.sql

PROMPT =============================================================
PROMPT  STEP 2 — Metadata Tables
PROMPT =============================================================
@@ddl/02_metadata_tables.sql

PROMPT =============================================================
PROMPT  STEP 3 — Control Tables
PROMPT =============================================================
@@ddl/03_control_tables.sql

PROMPT =============================================================
PROMPT  STEP 4 — Logging Tables
PROMPT =============================================================
@@ddl/04_logging_tables.sql

PROMPT =============================================================
PROMPT  STEP 5 — Target Dimension Tables
PROMPT =============================================================
@@ddl/05_target_tables.sql

PROMPT =============================================================
PROMPT  STEP 6 — Staging Tables
PROMPT =============================================================
@@ddl/06_staging_tables.sql

PROMPT =============================================================
PROMPT  STEP 7 — Views
PROMPT =============================================================
@@ddl/07_views.sql

PROMPT =============================================================
PROMPT  STEP 8 — PKG_ETL_LOGGER
PROMPT =============================================================
@@packages/08_pkg_etl_logger.sql

PROMPT =============================================================
PROMPT  STEP 9 — PKG_ETL_METADATA
PROMPT =============================================================
@@packages/09_pkg_etl_metadata.sql

PROMPT =============================================================
PROMPT  STEP 10 — PKG_ETL_CONTROL
PROMPT =============================================================
@@packages/10_pkg_etl_control.sql

PROMPT =============================================================
PROMPT  STEP 11 — PKG_ETL_VALIDATOR
PROMPT =============================================================
@@packages/11_pkg_etl_validator.sql

PROMPT =============================================================
PROMPT  STEP 12 — PKG_ETL_SCD2_LOADER
PROMPT =============================================================
@@packages/12_pkg_etl_scd2_loader.sql

PROMPT =============================================================
PROMPT  STEP 13 — PKG_ETL_TRANSFORM_STAFFING
PROMPT =============================================================
@@packages/13_pkg_etl_transform_staffing.sql

PROMPT =============================================================
PROMPT  STEP 14 — PKG_ETL_TRANSFORM_COST
PROMPT =============================================================
@@packages/14_pkg_etl_transform_cost.sql

PROMPT =============================================================
PROMPT  STEP 15 — PKG_ETL_ORCHESTRATOR
PROMPT =============================================================
@@packages/15_pkg_etl_orchestrator.sql

PROMPT =============================================================
PROMPT  STEP 16 — Seed Metadata
PROMPT =============================================================
@@data/16_metadata_inserts.sql

PROMPT =============================================================
PROMPT  Installation complete.
PROMPT  Verify with:
PROMPT    SELECT * FROM GPC_DM.V_ETL_ACTIVE_ENTITIES;
PROMPT  Run entities with:
PROMPT    EXEC GPC_DM.PKG_ETL_ORCHESTRATOR.run_entity('STAFFING_SCHEDULE');
PROMPT    EXEC GPC_DM.PKG_ETL_ORCHESTRATOR.run_entity('COST');
PROMPT    EXEC GPC_DM.PKG_ETL_ORCHESTRATOR.run_all;
PROMPT =============================================================
