-- ============================================================
-- FILE: 04_logging_tables.sql
-- DESC: Run, step, and error logging tables
-- SCHEMA: GPC_DM
-- All writes go through PKG_ETL_LOGGER which uses
-- PRAGMA AUTONOMOUS_TRANSACTION so logs persist on rollback.
-- ============================================================

-- ----------------------------------------------------------------
-- ETL_RUN_LOG
-- One row per entity-level run and per mapping-level run.
-- MAPPING_ID IS NULL for the entity-level parent run entry.
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.ETL_RUN_LOG (
    RUN_ID           NUMBER         DEFAULT GPC_DM.SEQ_ETL_RUN_LOG.NEXTVAL NOT NULL,
    ENTITY_ID        NUMBER         NOT NULL,
    MAPPING_ID       NUMBER,
    START_TIME       DATE           NOT NULL,
    END_TIME         DATE,
    STATUS           VARCHAR2(20)   DEFAULT 'RUNNING' NOT NULL
                     CONSTRAINT CHK_ETL_RL_STATUS
                         CHECK (STATUS IN ('RUNNING','SUCCESS','FAILED','PARTIAL')),
    ROWS_READ        NUMBER         DEFAULT 0,
    ROWS_INSERTED    NUMBER         DEFAULT 0,
    ROWS_UPDATED     NUMBER         DEFAULT 0,
    ROWS_EXPIRED     NUMBER         DEFAULT 0,
    ROWS_SKIPPED     NUMBER         DEFAULT 0,
    ROWS_REJECTED    NUMBER         DEFAULT 0,
    ERROR_MESSAGE    VARCHAR2(4000),
    CONSTRAINT PK_ETL_RUN_LOG PRIMARY KEY (RUN_ID)
);

CREATE INDEX GPC_DM.IDX_ETL_RL_ENTITY  ON GPC_DM.ETL_RUN_LOG(ENTITY_ID, START_TIME);
CREATE INDEX GPC_DM.IDX_ETL_RL_MAPPING ON GPC_DM.ETL_RUN_LOG(MAPPING_ID, START_TIME);

COMMENT ON TABLE  GPC_DM.ETL_RUN_LOG                 IS 'One row per ETL run at entity and mapping level.';
COMMENT ON COLUMN GPC_DM.ETL_RUN_LOG.MAPPING_ID      IS 'NULL for entity-level parent run entry; populated for per-mapping child entries.';
COMMENT ON COLUMN GPC_DM.ETL_RUN_LOG.ROWS_EXPIRED    IS 'SCD2 rows set to IS_CURRENT=N during this run.';
COMMENT ON COLUMN GPC_DM.ETL_RUN_LOG.ROWS_SKIPPED    IS 'Rows where no change was detected (UNCHANGED classification).';
COMMENT ON COLUMN GPC_DM.ETL_RUN_LOG.ROWS_REJECTED   IS 'Rows that failed validation and were not loaded.';


-- ----------------------------------------------------------------
-- ETL_STEP_LOG
-- Fine-grained step tracking within a run.
-- Each logical step (TRANSFORM, CLASSIFY, EXPIRE, INSERT, MERGE, etc.)
-- gets its own row with timing and row counts.
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.ETL_STEP_LOG (
    STEP_ID          NUMBER         DEFAULT GPC_DM.SEQ_ETL_STEP_LOG.NEXTVAL NOT NULL,
    RUN_ID           NUMBER         NOT NULL,
    STEP_NAME        VARCHAR2(200)  NOT NULL,
    START_TIME       DATE           NOT NULL,
    END_TIME         DATE,
    STATUS           VARCHAR2(20)   DEFAULT 'RUNNING' NOT NULL
                     CONSTRAINT CHK_ETL_SL_STATUS
                         CHECK (STATUS IN ('RUNNING','SUCCESS','FAILED','SKIPPED')),
    ROWS_AFFECTED    NUMBER         DEFAULT 0,
    STEP_MESSAGE     VARCHAR2(4000),
    CONSTRAINT PK_ETL_STEP_LOG PRIMARY KEY (STEP_ID),
    CONSTRAINT FK_ETL_SL_RUN   FOREIGN KEY (RUN_ID)
                               REFERENCES GPC_DM.ETL_RUN_LOG(RUN_ID)
);

CREATE INDEX GPC_DM.IDX_ETL_SL_RUN ON GPC_DM.ETL_STEP_LOG(RUN_ID);

COMMENT ON TABLE  GPC_DM.ETL_STEP_LOG              IS 'Step-level detail for each ETL run; used for diagnosing failures and performance profiling.';
COMMENT ON COLUMN GPC_DM.ETL_STEP_LOG.STEP_NAME    IS 'Convention: PHASE:TARGET_TABLE, e.g. SCD2_EXPIRE:GPC_DM.DIM_STAFFING_SCHEDULE.';


-- ----------------------------------------------------------------
-- ETL_ERROR_LOG
-- Error records with entity, target, code, message, and
-- optional serialised row data for investigation.
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.ETL_ERROR_LOG (
    ERR_ID           NUMBER         DEFAULT GPC_DM.SEQ_ETL_ERROR_LOG.NEXTVAL NOT NULL,
    RUN_ID           NUMBER         NOT NULL,
    ENTITY_NAME      VARCHAR2(100),
    TARGET_TABLE     VARCHAR2(200),
    ERROR_CODE       VARCHAR2(50),
    ERROR_MESSAGE    VARCHAR2(4000) NOT NULL,
    ERROR_TIME       DATE           DEFAULT SYSDATE NOT NULL,
    RECORD_KEY       VARCHAR2(500),
    RECORD_DATA      CLOB,
    CONSTRAINT PK_ETL_ERROR_LOG PRIMARY KEY (ERR_ID)
);

CREATE INDEX GPC_DM.IDX_ETL_EL_RUN    ON GPC_DM.ETL_ERROR_LOG(RUN_ID);
CREATE INDEX GPC_DM.IDX_ETL_EL_ENTITY ON GPC_DM.ETL_ERROR_LOG(ENTITY_NAME, ERROR_TIME);

COMMENT ON TABLE  GPC_DM.ETL_ERROR_LOG               IS 'Persistent error log; written via autonomous transaction so entries survive rollback.';
COMMENT ON COLUMN GPC_DM.ETL_ERROR_LOG.RECORD_KEY    IS 'Serialised business key of the failing record, e.g. SCHEDULE_ID=SCH001.';
COMMENT ON COLUMN GPC_DM.ETL_ERROR_LOG.RECORD_DATA   IS 'Full serialised staging row stored as CLOB for post-mortem investigation.';
