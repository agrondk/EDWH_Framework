-- ============================================================
-- FILE: 03_control_tables.sql
-- DESC: ETL control/watermark table
-- SCHEMA: GPC_DM
-- ============================================================

-- ----------------------------------------------------------------
-- ETL_CONTROL
-- One row per entity. Tracks watermark, run state, and last run id.
-- STATUS transitions:
--   IDLE     → locked to RUNNING at run start
--   RUNNING  → IDLE (success) or FAILED (error) at run end
--   FAILED   → manually set to IDLE to allow re-run
--   DISABLED → skipped by run_all; requires manual re-enable
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.ETL_CONTROL (
    CTRL_ID          NUMBER         GENERATED ALWAYS AS IDENTITY NOT NULL,
    ENTITY_ID        NUMBER         NOT NULL,
    LAST_WATERMARK   DATE,
    LAST_RUN_DATE    DATE,
    LAST_RUN_ID      NUMBER,
    STATUS           VARCHAR2(20)   DEFAULT 'IDLE' NOT NULL
                     CONSTRAINT CHK_ETL_CTRL_STATUS
                         CHECK (STATUS IN ('IDLE','RUNNING','FAILED','DISABLED')),
    NOTES            VARCHAR2(500),
    CONSTRAINT PK_ETL_CONTROL       PRIMARY KEY (CTRL_ID),
    CONSTRAINT UQ_ETL_CONTROL_ENT   UNIQUE (ENTITY_ID),
    CONSTRAINT FK_ETL_CTRL_ENTITY   FOREIGN KEY (ENTITY_ID)
                                    REFERENCES GPC_DM.ETL_ENTITY(ENTITY_ID)
);

COMMENT ON TABLE  GPC_DM.ETL_CONTROL                    IS 'Watermark and run-state control record, one per entity.';
COMMENT ON COLUMN GPC_DM.ETL_CONTROL.LAST_WATERMARK     IS 'MAX(REPORTING_DATE) of the last successfully completed run. NULL = never run (full load).';
COMMENT ON COLUMN GPC_DM.ETL_CONTROL.STATUS             IS 'IDLE=ready to run, RUNNING=in progress (locked), FAILED=last run failed, DISABLED=excluded from run_all.';
COMMENT ON COLUMN GPC_DM.ETL_CONTROL.NOTES              IS 'Free-text for operator comments, e.g. reason for disabling or manual watermark override notes.';
