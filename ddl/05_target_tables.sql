-- ============================================================
-- FILE: 05_target_tables.sql
-- DESC: Target dimension tables in GPC_DM schema
--       DIM_STAFFING_SCHEDULE  (SCD2)
--       DIM_STAFFING_TIMELINE  (INCREMENTAL/MERGE)
--       DIM_COST               (SCD2)
--       DIM_TIMELINE_COST      (INCREMENTAL/MERGE)
-- ============================================================

-- ----------------------------------------------------------------
-- DIM_STAFFING_SCHEDULE  (SCD2)
-- Business key : SCHEDULE_ID
-- Tracked attrs: EMPLOYEE_ID, POSITION_ID, PROJECT_ID,
--                SCHEDULE_TYPE, SCHEDULE_STATUS,
--                SCHEDULE_START_DATE, SCHEDULE_END_DATE
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.DIM_STAFFING_SCHEDULE (
    DIM_SS_ID             NUMBER         NOT NULL,
    -- Business key
    SCHEDULE_ID           VARCHAR2(50)   NOT NULL,
    -- Tracked descriptive attributes
    EMPLOYEE_ID           VARCHAR2(50),
    POSITION_ID           VARCHAR2(50),
    PROJECT_ID            VARCHAR2(50),
    SCHEDULE_TYPE         VARCHAR2(50),
    SCHEDULE_STATUS       VARCHAR2(30),
    SCHEDULE_START_DATE   DATE,
    SCHEDULE_END_DATE     DATE,
    -- SCD2 control columns
    EFFECTIVE_START_DATE  DATE           NOT NULL,
    EFFECTIVE_END_DATE    DATE           DEFAULT DATE '9999-12-31' NOT NULL,
    IS_CURRENT            VARCHAR2(1)    DEFAULT 'Y' NOT NULL
                          CONSTRAINT CHK_DIM_SS_CURRENT CHECK (IS_CURRENT IN ('Y','N')),
    RECORD_HASH           VARCHAR2(64),
    -- Audit columns
    REPORTING_DATE        DATE,
    ETL_RUN_ID            NUMBER,
    ETL_LOAD_DATE         DATE           DEFAULT SYSDATE,
    CONSTRAINT PK_DIM_SS          PRIMARY KEY (DIM_SS_ID),
    CONSTRAINT UQ_DIM_SS_EFFSTART UNIQUE (SCHEDULE_ID, EFFECTIVE_START_DATE)
);

CREATE INDEX GPC_DM.IDX_DIM_SS_BK      ON GPC_DM.DIM_STAFFING_SCHEDULE(SCHEDULE_ID, IS_CURRENT);
CREATE INDEX GPC_DM.IDX_DIM_SS_CURRENT ON GPC_DM.DIM_STAFFING_SCHEDULE(IS_CURRENT);

COMMENT ON TABLE  GPC_DM.DIM_STAFFING_SCHEDULE                      IS 'SCD2 dimension for staffing schedule definitions. One row per schedule version.';
COMMENT ON COLUMN GPC_DM.DIM_STAFFING_SCHEDULE.DIM_SS_ID            IS 'Surrogate key; populated from SEQ_DIM_SS.';
COMMENT ON COLUMN GPC_DM.DIM_STAFFING_SCHEDULE.SCHEDULE_ID          IS 'Natural/business key from source system.';
COMMENT ON COLUMN GPC_DM.DIM_STAFFING_SCHEDULE.EFFECTIVE_START_DATE IS 'Date this version became active (set to TRUNC(SYSDATE) on load).';
COMMENT ON COLUMN GPC_DM.DIM_STAFFING_SCHEDULE.EFFECTIVE_END_DATE   IS 'Date this version was superseded. 9999-12-31 = currently active.';
COMMENT ON COLUMN GPC_DM.DIM_STAFFING_SCHEDULE.IS_CURRENT           IS 'Y = active/current version; N = historical.';
COMMENT ON COLUMN GPC_DM.DIM_STAFFING_SCHEDULE.RECORD_HASH          IS 'SHA-256 hash over tracked attribute columns; used for efficient change detection.';


-- ----------------------------------------------------------------
-- DIM_STAFFING_TIMELINE  (INCREMENTAL / MERGE)
-- Business key : SCHEDULE_ID + DT_PERIOD
-- DT_PERIOD    : Derived from PERIOD_START_DATE as YYYYMM (mandatory)
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.DIM_STAFFING_TIMELINE (
    DIM_ST_ID             NUMBER         NOT NULL,
    -- Business key (composite)
    SCHEDULE_ID           VARCHAR2(50)   NOT NULL,
    DT_PERIOD             VARCHAR2(6)    NOT NULL,
    -- Attributes
    PERIOD_START_DATE     DATE,
    PERIOD_END_DATE       DATE,
    ALLOCATED_HOURS       NUMBER(18,4),
    RECORD_HASH           VARCHAR2(64),
    -- Audit
    REPORTING_DATE        DATE,
    ETL_RUN_ID            NUMBER,
    ETL_LOAD_DATE         DATE           DEFAULT SYSDATE,
    CONSTRAINT PK_DIM_ST     PRIMARY KEY (DIM_ST_ID),
    CONSTRAINT UQ_DIM_ST_BK  UNIQUE (SCHEDULE_ID, DT_PERIOD)
);

CREATE INDEX GPC_DM.IDX_DIM_ST_BK ON GPC_DM.DIM_STAFFING_TIMELINE(SCHEDULE_ID, DT_PERIOD);

COMMENT ON TABLE  GPC_DM.DIM_STAFFING_TIMELINE               IS 'Incremental timeline table for staffing schedule periods. Merged on SCHEDULE_ID + DT_PERIOD.';
COMMENT ON COLUMN GPC_DM.DIM_STAFFING_TIMELINE.DT_PERIOD     IS 'Period in YYYYMM format, derived from PERIOD_START_DATE. Mandatory field.';
COMMENT ON COLUMN GPC_DM.DIM_STAFFING_TIMELINE.RECORD_HASH   IS 'SHA-256 hash over tracked attributes; used in MERGE WHERE clause to skip no-change updates.';


-- ----------------------------------------------------------------
-- DIM_COST  (SCD2)
-- Business key : COST_ID
-- Tracked attrs: PROJECT_ID, COST_CENTER, COST_TYPE,
--                COST_CATEGORY, CURRENCY, BUDGET_VERSION
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.DIM_COST (
    DIM_COST_ID           NUMBER         NOT NULL,
    -- Business key
    COST_ID               VARCHAR2(50)   NOT NULL,
    -- Tracked attributes
    PROJECT_ID            VARCHAR2(50),
    COST_CENTER           VARCHAR2(50),
    COST_TYPE             VARCHAR2(50),
    COST_CATEGORY         VARCHAR2(50),
    CURRENCY              VARCHAR2(10),
    BUDGET_VERSION        VARCHAR2(50),
    -- SCD2 control columns
    EFFECTIVE_START_DATE  DATE           NOT NULL,
    EFFECTIVE_END_DATE    DATE           DEFAULT DATE '9999-12-31' NOT NULL,
    IS_CURRENT            VARCHAR2(1)    DEFAULT 'Y' NOT NULL
                          CONSTRAINT CHK_DIM_COST_CURRENT CHECK (IS_CURRENT IN ('Y','N')),
    RECORD_HASH           VARCHAR2(64),
    -- Audit
    REPORTING_DATE        DATE,
    ETL_RUN_ID            NUMBER,
    ETL_LOAD_DATE         DATE           DEFAULT SYSDATE,
    CONSTRAINT PK_DIM_COST          PRIMARY KEY (DIM_COST_ID),
    CONSTRAINT UQ_DIM_COST_EFFSTART UNIQUE (COST_ID, EFFECTIVE_START_DATE)
);

CREATE INDEX GPC_DM.IDX_DIM_COST_BK      ON GPC_DM.DIM_COST(COST_ID, IS_CURRENT);
CREATE INDEX GPC_DM.IDX_DIM_COST_CURRENT ON GPC_DM.DIM_COST(IS_CURRENT);

COMMENT ON TABLE  GPC_DM.DIM_COST                              IS 'SCD2 dimension for cost master data. One row per cost record version.';
COMMENT ON COLUMN GPC_DM.DIM_COST.DIM_COST_ID                 IS 'Surrogate key; populated from SEQ_DIM_COST.';
COMMENT ON COLUMN GPC_DM.DIM_COST.COST_ID                     IS 'Natural/business key from source system.';
COMMENT ON COLUMN GPC_DM.DIM_COST.EFFECTIVE_END_DATE          IS 'Date this version was superseded. 9999-12-31 = currently active.';
COMMENT ON COLUMN GPC_DM.DIM_COST.IS_CURRENT                  IS 'Y = active/current version; N = historical.';
COMMENT ON COLUMN GPC_DM.DIM_COST.RECORD_HASH                 IS 'SHA-256 hash over tracked attribute columns; used for efficient change detection.';


-- ----------------------------------------------------------------
-- DIM_TIMELINE_COST  (INCREMENTAL / MERGE)
-- Business key : COST_ID + DT_PERIOD
-- CURVE_TYPE   : Derived from FORECAST_TYPE / BUDGET_VERSION (mandatory)
-- DT_PERIOD    : Derived from PERIOD_DATE as YYYYMM (mandatory)
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.DIM_TIMELINE_COST (
    DIM_TC_ID             NUMBER         NOT NULL,
    -- Business key (composite)
    COST_ID               VARCHAR2(50)   NOT NULL,
    DT_PERIOD             VARCHAR2(6)    NOT NULL,
    -- Attributes
    PERIOD_DATE           DATE,
    AMOUNT                NUMBER(22,6),
    CURVE_TYPE            VARCHAR2(50),
    FORECAST_TYPE         VARCHAR2(50),
    RECORD_HASH           VARCHAR2(64),
    -- Audit
    REPORTING_DATE        DATE,
    ETL_RUN_ID            NUMBER,
    ETL_LOAD_DATE         DATE           DEFAULT SYSDATE,
    CONSTRAINT PK_DIM_TC     PRIMARY KEY (DIM_TC_ID),
    CONSTRAINT UQ_DIM_TC_BK  UNIQUE (COST_ID, DT_PERIOD)
);

CREATE INDEX GPC_DM.IDX_DIM_TC_BK ON GPC_DM.DIM_TIMELINE_COST(COST_ID, DT_PERIOD);

COMMENT ON TABLE  GPC_DM.DIM_TIMELINE_COST              IS 'Incremental timeline table for cost amounts per period. Merged on COST_ID + DT_PERIOD.';
COMMENT ON COLUMN GPC_DM.DIM_TIMELINE_COST.DT_PERIOD    IS 'Period in YYYYMM format, derived from PERIOD_DATE. Mandatory field.';
COMMENT ON COLUMN GPC_DM.DIM_TIMELINE_COST.CURVE_TYPE   IS 'Derived field: ACTUAL / BUDGET / FORECAST. Mandatory; records without derivable value are rejected.';
COMMENT ON COLUMN GPC_DM.DIM_TIMELINE_COST.RECORD_HASH  IS 'SHA-256 hash over tracked attributes; used in MERGE WHERE clause to skip no-change updates.';
