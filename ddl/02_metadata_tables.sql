-- ============================================================
-- FILE: 02_metadata_tables.sql
-- DESC: Metadata registry tables for the EDWH framework
-- SCHEMA: GPC_DM
-- ============================================================

-- ----------------------------------------------------------------
-- ETL_SOURCE_SYSTEM
-- Registers each upstream source system (e.g. KBR_IHUB)
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.ETL_SOURCE_SYSTEM (
    SS_ID          NUMBER         DEFAULT GPC_DM.SEQ_ETL_SS.NEXTVAL NOT NULL,
    SS_NAME        VARCHAR2(100)  NOT NULL,
    SS_SCHEMA      VARCHAR2(100)  NOT NULL,
    SS_DESCRIPTION VARCHAR2(500),
    IS_ACTIVE      VARCHAR2(1)    DEFAULT 'Y' NOT NULL
                   CONSTRAINT CHK_ETL_SS_ACTIVE CHECK (IS_ACTIVE IN ('Y','N')),
    CREATED_DATE   DATE           DEFAULT SYSDATE NOT NULL,
    CONSTRAINT PK_ETL_SOURCE_SYSTEM PRIMARY KEY (SS_ID),
    CONSTRAINT UQ_ETL_SS_NAME       UNIQUE (SS_NAME)
);

COMMENT ON TABLE  GPC_DM.ETL_SOURCE_SYSTEM          IS 'Registry of source systems feeding the EDWH.';
COMMENT ON COLUMN GPC_DM.ETL_SOURCE_SYSTEM.SS_ID    IS 'Surrogate primary key.';
COMMENT ON COLUMN GPC_DM.ETL_SOURCE_SYSTEM.SS_SCHEMA IS 'Oracle schema prefix used when querying source tables.';


-- ----------------------------------------------------------------
-- ETL_ENTITY
-- One row per logical entity / source table.
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.ETL_ENTITY (
    ENTITY_ID         NUMBER         DEFAULT GPC_DM.SEQ_ETL_ENTITY.NEXTVAL NOT NULL,
    SS_ID             NUMBER         NOT NULL,
    ENTITY_NAME       VARCHAR2(100)  NOT NULL,
    SOURCE_TABLE      VARCHAR2(200)  NOT NULL,
    WATERMARK_COLUMN  VARCHAR2(100)  DEFAULT 'REPORTING_DATE' NOT NULL,
    WATERMARK_TYPE    VARCHAR2(20)   DEFAULT 'DATE' NOT NULL
                      CONSTRAINT CHK_ETL_ENT_WMTYPE
                          CHECK (WATERMARK_TYPE IN ('DATE','TIMESTAMP','NUMBER')),
    DESCRIPTION       VARCHAR2(500),
    IS_ACTIVE         VARCHAR2(1)    DEFAULT 'Y' NOT NULL
                      CONSTRAINT CHK_ETL_ENT_ACTIVE CHECK (IS_ACTIVE IN ('Y','N')),
    CREATED_DATE      DATE           DEFAULT SYSDATE NOT NULL,
    CONSTRAINT PK_ETL_ENTITY       PRIMARY KEY (ENTITY_ID),
    CONSTRAINT UQ_ETL_ENTITY_NAME  UNIQUE (ENTITY_NAME),
    CONSTRAINT FK_ETL_ENTITY_SS    FOREIGN KEY (SS_ID)
                                   REFERENCES GPC_DM.ETL_SOURCE_SYSTEM(SS_ID)
);

COMMENT ON TABLE  GPC_DM.ETL_ENTITY                     IS 'One row per logical ETL entity / source table.';
COMMENT ON COLUMN GPC_DM.ETL_ENTITY.WATERMARK_COLUMN    IS 'Column used to detect new/changed records (default REPORTING_DATE).';
COMMENT ON COLUMN GPC_DM.ETL_ENTITY.SOURCE_TABLE        IS 'Fully qualified source table name, e.g. KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB.';


-- ----------------------------------------------------------------
-- ETL_TARGET_MAPPING
-- Each target table a source entity feeds; defines load type,
-- staging table, surrogate key sequence, and SCD2 control columns.
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.ETL_TARGET_MAPPING (
    MAPPING_ID         NUMBER         DEFAULT GPC_DM.SEQ_ETL_MAPPING.NEXTVAL NOT NULL,
    ENTITY_ID          NUMBER         NOT NULL,
    TARGET_TABLE       VARCHAR2(200)  NOT NULL,
    STAGING_TABLE      VARCHAR2(200)  NOT NULL,
    LOAD_TYPE          VARCHAR2(20)   NOT NULL
                       CONSTRAINT CHK_ETL_TM_LOADTYPE
                           CHECK (LOAD_TYPE IN ('SCD1','SCD2','INCREMENTAL','APPEND')),
    SURROGATE_KEY_COL  VARCHAR2(100),
    SURROGATE_SEQ_NAME VARCHAR2(200),
    SCD2_CURRENT_COL   VARCHAR2(100)  DEFAULT 'IS_CURRENT',
    SCD2_EFF_START_COL VARCHAR2(100)  DEFAULT 'EFFECTIVE_START_DATE',
    SCD2_EFF_END_COL   VARCHAR2(100)  DEFAULT 'EFFECTIVE_END_DATE',
    SCD2_END_SENTINEL  DATE           DEFAULT DATE '9999-12-31',
    HASH_COL           VARCHAR2(100)  DEFAULT 'RECORD_HASH',
    LOAD_ORDER         NUMBER         DEFAULT 10 NOT NULL,
    IS_ACTIVE          VARCHAR2(1)    DEFAULT 'Y' NOT NULL
                       CONSTRAINT CHK_ETL_TM_ACTIVE CHECK (IS_ACTIVE IN ('Y','N')),
    CREATED_DATE       DATE           DEFAULT SYSDATE NOT NULL,
    CONSTRAINT PK_ETL_TARGET_MAPPING PRIMARY KEY (MAPPING_ID),
    CONSTRAINT UQ_ETL_TM_ENTITY_TGT  UNIQUE (ENTITY_ID, TARGET_TABLE),
    CONSTRAINT FK_ETL_TM_ENTITY      FOREIGN KEY (ENTITY_ID)
                                     REFERENCES GPC_DM.ETL_ENTITY(ENTITY_ID)
);

COMMENT ON TABLE  GPC_DM.ETL_TARGET_MAPPING                    IS 'Maps each source entity to one or more target tables with load strategy.';
COMMENT ON COLUMN GPC_DM.ETL_TARGET_MAPPING.LOAD_TYPE          IS 'SCD1=upsert, SCD2=full history, INCREMENTAL=merge, APPEND=insert-only.';
COMMENT ON COLUMN GPC_DM.ETL_TARGET_MAPPING.LOAD_ORDER         IS 'Controls execution sequence when one entity feeds multiple targets.';
COMMENT ON COLUMN GPC_DM.ETL_TARGET_MAPPING.SURROGATE_SEQ_NAME IS 'Fully qualified sequence name, e.g. GPC_DM.SEQ_DIM_SS.';


-- ----------------------------------------------------------------
-- ETL_COLUMN_MAPPING
-- Column-level source → target mapping for each target mapping.
-- IS_BUSINESS_KEY=Y  → used in join conditions (BK is immutable in SCD2).
-- IS_TRACKED=Y       → included in RECORD_HASH for change detection.
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.ETL_COLUMN_MAPPING (
    CM_ID            NUMBER        DEFAULT GPC_DM.SEQ_ETL_COL_MAP.NEXTVAL NOT NULL,
    MAPPING_ID       NUMBER        NOT NULL,
    SOURCE_COLUMN    VARCHAR2(100) NOT NULL,
    TARGET_COLUMN    VARCHAR2(100) NOT NULL,
    IS_BUSINESS_KEY  VARCHAR2(1)   DEFAULT 'N' NOT NULL
                     CONSTRAINT CHK_ETL_CM_BK CHECK (IS_BUSINESS_KEY IN ('Y','N')),
    IS_TRACKED       VARCHAR2(1)   DEFAULT 'Y' NOT NULL
                     CONSTRAINT CHK_ETL_CM_TRACKED CHECK (IS_TRACKED IN ('Y','N')),
    COLUMN_ORDER     NUMBER        DEFAULT 10 NOT NULL,
    IS_ACTIVE        VARCHAR2(1)   DEFAULT 'Y' NOT NULL
                     CONSTRAINT CHK_ETL_CM_ACTIVE CHECK (IS_ACTIVE IN ('Y','N')),
    CONSTRAINT PK_ETL_COLUMN_MAPPING PRIMARY KEY (CM_ID),
    CONSTRAINT FK_ETL_CM_MAPPING     FOREIGN KEY (MAPPING_ID)
                                     REFERENCES GPC_DM.ETL_TARGET_MAPPING(MAPPING_ID)
);

CREATE INDEX GPC_DM.IDX_ETL_CM_MAPPING ON GPC_DM.ETL_COLUMN_MAPPING(MAPPING_ID);

COMMENT ON TABLE  GPC_DM.ETL_COLUMN_MAPPING                    IS 'Column-level source-to-target mapping per ETL_TARGET_MAPPING row.';
COMMENT ON COLUMN GPC_DM.ETL_COLUMN_MAPPING.IS_BUSINESS_KEY    IS 'Y = this column is part of the business key used for matching/SCD2 joins.';
COMMENT ON COLUMN GPC_DM.ETL_COLUMN_MAPPING.IS_TRACKED         IS 'Y = this column is included in STANDARD_HASH for change detection.';
COMMENT ON COLUMN GPC_DM.ETL_COLUMN_MAPPING.COLUMN_ORDER       IS 'Order used when concatenating columns for hash computation. Must be consistent.';


-- ----------------------------------------------------------------
-- ETL_VALIDATION_RULE
-- Per-mapping validation rules for mandatory and derived fields.
-- DERIVED rules attempt to populate NULL columns before rejecting.
-- ----------------------------------------------------------------
CREATE TABLE GPC_DM.ETL_VALIDATION_RULE (
    RULE_ID          NUMBER         DEFAULT GPC_DM.SEQ_ETL_VAL_RULE.NEXTVAL NOT NULL,
    MAPPING_ID       NUMBER         NOT NULL,
    RULE_NAME        VARCHAR2(100)  NOT NULL,
    RULE_TYPE        VARCHAR2(20)   NOT NULL
                     CONSTRAINT CHK_ETL_VR_TYPE
                         CHECK (RULE_TYPE IN ('NOT_NULL','DERIVED','CUSTOM','CHECK')),
    COLUMN_NAME      VARCHAR2(100)  NOT NULL,
    DERIVED_SQL      VARCHAR2(4000),
    ERROR_ACTION     VARCHAR2(10)   DEFAULT 'REJECT' NOT NULL
                     CONSTRAINT CHK_ETL_VR_ACTION CHECK (ERROR_ACTION IN ('REJECT','FAIL')),
    IS_ACTIVE        VARCHAR2(1)    DEFAULT 'Y' NOT NULL
                     CONSTRAINT CHK_ETL_VR_ACTIVE CHECK (IS_ACTIVE IN ('Y','N')),
    CONSTRAINT PK_ETL_VAL_RULE   PRIMARY KEY (RULE_ID),
    CONSTRAINT FK_ETL_VR_MAPPING FOREIGN KEY (MAPPING_ID)
                                 REFERENCES GPC_DM.ETL_TARGET_MAPPING(MAPPING_ID)
);

COMMENT ON TABLE  GPC_DM.ETL_VALIDATION_RULE               IS 'Validation rules applied to staging rows before loading to target.';
COMMENT ON COLUMN GPC_DM.ETL_VALIDATION_RULE.RULE_TYPE     IS 'NOT_NULL=reject if null, DERIVED=attempt derivation via DERIVED_SQL then reject if still null, CHECK=reject if DERIVED_SQL predicate evaluates to FALSE or NULL.';
COMMENT ON COLUMN GPC_DM.ETL_VALIDATION_RULE.DERIVED_SQL   IS 'SQL expression valid in a SELECT over the staging table, e.g. TO_CHAR(PERIOD_START_DATE,''YYYYMM'').';
COMMENT ON COLUMN GPC_DM.ETL_VALIDATION_RULE.ERROR_ACTION  IS 'REJECT=mark row as rejected and continue, FAIL=raise exception and abort the run.';
