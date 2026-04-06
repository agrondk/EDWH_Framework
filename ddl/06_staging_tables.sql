-- ============================================================
-- FILE: 06_staging_tables.sql
-- DESC: Staging tables — one per target table.
--       Each staging table holds extracted/transformed rows
--       tagged by STG_RUN_ID before loading to target.
--
-- STG_STATUS values:
--   PENDING   = loaded by transform, awaiting validation + load
--   LOADED    = successfully written to target
--   REJECTED  = failed validation; not loaded
--
-- STG_ACTION values (set by loader):
--   INSERT    = new record, not found in target
--   UPDATE    = existing record with changed tracked columns (SCD2)
--   UNCHANGED = existing record, no change detected
--   MERGE     = written via MERGE statement (INCREMENTAL load type)
-- ============================================================

-- ----------------------------------------------------------------
-- STG_STAFFING_SCHEDULE
-- Staging for DIM_STAFFING_SCHEDULE (SCD2)
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.STG_STAFFING_SCHEDULE (
    STG_ID              NUMBER         GENERATED ALWAYS AS IDENTITY NOT NULL,
    STG_RUN_ID          NUMBER         NOT NULL,
    STG_STATUS          VARCHAR2(20)   DEFAULT 'PENDING' NOT NULL
                        CONSTRAINT CHK_STG_SS_STATUS
                            CHECK (STG_STATUS IN ('PENDING','LOADED','REJECTED')),
    STG_ACTION          VARCHAR2(20)
                        CONSTRAINT CHK_STG_SS_ACTION
                            CHECK (STG_ACTION IN ('INSERT','UPDATE','UNCHANGED')),
    STG_REJECT_REASON   VARCHAR2(500),
    STG_RECORD_HASH     VARCHAR2(64),
    -- Payload columns (mirrors DIM_STAFFING_SCHEDULE non-SCD2 columns)
    SCHEDULE_ID         VARCHAR2(50),
    EMPLOYEE_ID         VARCHAR2(50),
    POSITION_ID         VARCHAR2(50),
    PROJECT_ID          VARCHAR2(50),
    SCHEDULE_TYPE       VARCHAR2(50),
    SCHEDULE_STATUS     VARCHAR2(30),
    SCHEDULE_START_DATE DATE,
    SCHEDULE_END_DATE   DATE,
    REPORTING_DATE      DATE,
    CONSTRAINT PK_STG_SS PRIMARY KEY (STG_ID)
);

CREATE INDEX GPC_DM.IDX_STG_SS_RUN ON GPC_DM.STG_STAFFING_SCHEDULE(STG_RUN_ID, STG_STATUS);
CREATE INDEX GPC_DM.IDX_STG_SS_BK  ON GPC_DM.STG_STAFFING_SCHEDULE(SCHEDULE_ID, STG_RUN_ID);

COMMENT ON TABLE  GPC_DM.STG_STAFFING_SCHEDULE               IS 'Staging table for DIM_STAFFING_SCHEDULE. Populated by PKG_ETL_TRANSFORM_STAFFING.';
COMMENT ON COLUMN GPC_DM.STG_STAFFING_SCHEDULE.STG_RECORD_HASH IS 'SHA-256 hash over tracked attributes; computed during transform via STANDARD_HASH.';


-- ----------------------------------------------------------------
-- STG_STAFFING_TIMELINE
-- Staging for DIM_STAFFING_TIMELINE (INCREMENTAL)
-- DT_PERIOD derived from PERIOD_START_DATE; validation may reject NULL.
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.STG_STAFFING_TIMELINE (
    STG_ID              NUMBER         GENERATED ALWAYS AS IDENTITY NOT NULL,
    STG_RUN_ID          NUMBER         NOT NULL,
    STG_STATUS          VARCHAR2(20)   DEFAULT 'PENDING' NOT NULL
                        CONSTRAINT CHK_STG_ST_STATUS
                            CHECK (STG_STATUS IN ('PENDING','LOADED','REJECTED')),
    STG_ACTION          VARCHAR2(20)
                        CONSTRAINT CHK_STG_ST_ACTION
                            CHECK (STG_ACTION IN ('INSERT','UPDATE','UNCHANGED','MERGE')),
    STG_REJECT_REASON   VARCHAR2(500),
    STG_RECORD_HASH     VARCHAR2(64),
    -- Payload
    SCHEDULE_ID         VARCHAR2(50),
    DT_PERIOD           VARCHAR2(6),
    PERIOD_START_DATE   DATE,
    PERIOD_END_DATE     DATE,
    ALLOCATED_HOURS     NUMBER(18,4),
    REPORTING_DATE      DATE,
    CONSTRAINT PK_STG_ST PRIMARY KEY (STG_ID)
);

CREATE INDEX GPC_DM.IDX_STG_ST_RUN ON GPC_DM.STG_STAFFING_TIMELINE(STG_RUN_ID, STG_STATUS);
CREATE INDEX GPC_DM.IDX_STG_ST_BK  ON GPC_DM.STG_STAFFING_TIMELINE(SCHEDULE_ID, DT_PERIOD, STG_RUN_ID);

COMMENT ON TABLE  GPC_DM.STG_STAFFING_TIMELINE           IS 'Staging table for DIM_STAFFING_TIMELINE. DT_PERIOD is mandatory; NULL rows are rejected.';


-- ----------------------------------------------------------------
-- STG_COST
-- Staging for DIM_COST (SCD2)
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.STG_COST (
    STG_ID              NUMBER         GENERATED ALWAYS AS IDENTITY NOT NULL,
    STG_RUN_ID          NUMBER         NOT NULL,
    STG_STATUS          VARCHAR2(20)   DEFAULT 'PENDING' NOT NULL
                        CONSTRAINT CHK_STG_COST_STATUS
                            CHECK (STG_STATUS IN ('PENDING','LOADED','REJECTED')),
    STG_ACTION          VARCHAR2(20)
                        CONSTRAINT CHK_STG_COST_ACTION
                            CHECK (STG_ACTION IN ('INSERT','UPDATE','UNCHANGED')),
    STG_REJECT_REASON   VARCHAR2(500),
    STG_RECORD_HASH     VARCHAR2(64),
    -- Payload
    COST_ID             VARCHAR2(50),
    PROJECT_ID          VARCHAR2(50),
    COST_CENTER         VARCHAR2(50),
    COST_TYPE           VARCHAR2(50),
    COST_CATEGORY       VARCHAR2(50),
    CURRENCY            VARCHAR2(10),
    REPORTING_DATE      DATE,
    CONSTRAINT PK_STG_COST PRIMARY KEY (STG_ID)
);

CREATE INDEX GPC_DM.IDX_STG_COST_RUN ON GPC_DM.STG_COST(STG_RUN_ID, STG_STATUS);
CREATE INDEX GPC_DM.IDX_STG_COST_BK  ON GPC_DM.STG_COST(COST_ID, STG_RUN_ID);

COMMENT ON TABLE  GPC_DM.STG_COST IS 'Staging table for DIM_COST. Populated by PKG_ETL_TRANSFORM_COST.';


-- ----------------------------------------------------------------
-- STG_TIMELINE_COST
-- Staging for DIM_TIMELINE_COST (INCREMENTAL)
-- CURVE_TYPE and DT_PERIOD are mandatory derived fields;
-- rows without derivable values are rejected by the validator.
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.STG_TIMELINE_COST (
    STG_ID              NUMBER         GENERATED ALWAYS AS IDENTITY NOT NULL,
    STG_RUN_ID          NUMBER         NOT NULL,
    STG_STATUS          VARCHAR2(20)   DEFAULT 'PENDING' NOT NULL
                        CONSTRAINT CHK_STG_TC_STATUS
                            CHECK (STG_STATUS IN ('PENDING','LOADED','REJECTED')),
    STG_ACTION          VARCHAR2(20)
                        CONSTRAINT CHK_STG_TC_ACTION
                            CHECK (STG_ACTION IN ('INSERT','UPDATE','UNCHANGED','MERGE')),
    STG_REJECT_REASON   VARCHAR2(500),
    STG_RECORD_HASH     VARCHAR2(64),
    -- Payload
    COST_ID             VARCHAR2(50),
    DT_PERIOD           VARCHAR2(6),
    PERIOD_DATE         DATE,
    AMOUNT              NUMBER(22,6),
    CURVE_TYPE          VARCHAR2(50),
    FORECAST_TYPE       VARCHAR2(50),
    REPORTING_DATE      DATE,
    CONSTRAINT PK_STG_TC PRIMARY KEY (STG_ID)
);

CREATE INDEX GPC_DM.IDX_STG_TC_RUN ON GPC_DM.STG_TIMELINE_COST(STG_RUN_ID, STG_STATUS);
CREATE INDEX GPC_DM.IDX_STG_TC_BK  ON GPC_DM.STG_TIMELINE_COST(COST_ID, DT_PERIOD, STG_RUN_ID);

COMMENT ON TABLE  GPC_DM.STG_TIMELINE_COST               IS 'Staging table for DIM_TIMELINE_COST. CURVE_TYPE and DT_PERIOD are mandatory derived fields.';
COMMENT ON COLUMN GPC_DM.STG_TIMELINE_COST.CURVE_TYPE    IS 'Derived in transform from FORECAST_TYPE/BUDGET_VERSION. Validator re-derives and rejects if still NULL.';
