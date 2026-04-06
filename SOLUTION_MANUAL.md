# GPC_DM EDWH Framework — Solution Manual

**Version:** 1.0
**Date:** 2026-04-01
**Schema:** GPC_DM
**Source:** KBR_IHUB

---

## Table of Contents

1. [Solution Overview](#1-solution-overview)
2. [Architecture](#2-architecture)
3. [File Inventory](#3-file-inventory)
4. [Prerequisites](#4-prerequisites)
5. [Installation — Fresh Environment](#5-installation--fresh-environment)
6. [Installation — Existing Environment (Incremental Apply)](#6-installation--existing-environment-incremental-apply)
7. [Running the ETL](#7-running-the-etl)
8. [Monitoring and Operations](#8-monitoring-and-operations)
9. [Troubleshooting and Recovery](#9-troubleshooting-and-recovery)
10. [How to Add a New Source System](#10-how-to-add-a-new-source-system)
11. [How to Add a New Entity / Table](#11-how-to-add-a-new-entity--table)
12. [How to Modify an Existing Entity (Columns, Load Type)](#12-how-to-modify-an-existing-entity-columns-load-type)
13. [How to Add or Modify Validation Rules](#13-how-to-add-or-modify-validation-rules)
14. [Validation Rules Reference](#14-validation-rules-reference)
15. [Metadata Table Reference](#15-metadata-table-reference)

---

## 1. Solution Overview

The GPC_DM EDWH Framework is a **metadata-driven Oracle ETL pipeline** that loads data from upstream source systems (currently KBR_IHUB) into the GPC_DM data warehouse dimension tables.

Key characteristics:

- **Metadata-driven** — source tables, column mappings, load types, and validation rules are stored in registry tables. No code changes are needed to add columns or adjust mappings.
- **Validation parity** — the same data quality rules enforced by the C# backend (FluentValidation) on Excel uploads are also enforced by the SQL pipeline when loading from any source.
- **Idempotent / restartable** — failed runs can be safely re-run. Staging tables are cleared and re-populated at the start of each run.
- **Full audit trail** — every run, step, row count, rejection reason, and error is written to logging tables that survive rollback (autonomous transactions).
- **SCD2 + Incremental** — slowly changing dimension logic (SCD2) for master records, incremental MERGE for timeline/period data.

### Current Entities

| Entity Name | Source Table | Target Tables | Load Types |
|---|---|---|---|
| `STAFFING_SCHEDULE` | `KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB` | `DIM_STAFFING_SCHEDULE` | SCD2 |
| | | `DIM_STAFFING_TIMELINE` | INCREMENTAL |
| `COST` | `KBR_IHUB.APAC_PCDM_COST_IHUB` | `DIM_COST` | SCD2 |
| | | `DIM_TIMELINE_COST` | INCREMENTAL |

---

## 2. Architecture

### ETL Flow

```
KBR_IHUB source tables
        │
        ▼
 PKG_ETL_ORCHESTRATOR.run_entity()
        │
        ├─ 1. Validate metadata
        ├─ 2. Acquire lock (ETL_CONTROL)
        ├─ 3. Determine watermark window
        │
        ├─ 4. TRANSFORM (entity-specific package)
        │       └─ Extract → deduplicate → hash → INSERT into staging tables
        │
        └─ 5. For each target mapping:
               ├─ a. VALIDATE STAGING (PKG_ETL_VALIDATOR)
               │       └─ Apply ETL_VALIDATION_RULE rows:
               │            NOT_NULL → reject if NULL
               │            DERIVED  → derive value, then reject if still NULL
               │            CHECK    → reject if predicate fails
               │
               ├─ b. LOAD (PKG_ETL_SCD2_LOADER)
               │       ├─ SCD2:        CLASSIFY → EXPIRE → INSERT
               │       └─ INCREMENTAL: MERGE on business key
               │
               ├─ c. Post-load SCD2 duplicate check
               └─ d. COMMIT + advance watermark
```

### Staging Table Pattern

Each target table has a dedicated staging table with these control columns:

| Column | Values | Meaning |
|---|---|---|
| `STG_STATUS` | `PENDING` | Loaded by transform, awaiting validation and load |
| | `LOADED` | Successfully written to target |
| | `REJECTED` | Failed validation; not loaded |
| `STG_ACTION` | `INSERT` | New record, not found in target |
| | `UPDATE` | Existing record with changed tracked columns |
| | `UNCHANGED` | Existing record, no change detected |
| | `MERGE` | Written via MERGE (incremental targets) |
| `STG_REJECT_REASON` | Free text | Appended by each failing validation rule |
| `STG_RUN_ID` | NUMBER | Links staging row to its ETL run |

### SCD2 Control Columns (DIM tables)

| Column | Meaning |
|---|---|
| `EFFECTIVE_START_DATE` | Date this version became active |
| `EFFECTIVE_END_DATE` | Date this version was superseded; `9999-12-31` = active |
| `IS_CURRENT` | `Y` = active version; `N` = historical |
| `RECORD_HASH` | SHA-256 hash of tracked attributes; change detection |

### ETL Control States

```
IDLE ──run_entity()──► RUNNING ──success──► IDLE
                                └──failure──► FAILED
FAILED ──manual reset──► IDLE
IDLE/FAILED ──disable──► DISABLED  (skipped by run_all)
```

---

## 3. File Inventory

```
EDWH_Framework/
├── 00_install_all.sql                    Master install script
│
├── ddl/
│   ├── 01_sequences.sql                  All sequences (surrogate keys, log IDs)
│   ├── 02_metadata_tables.sql            Registry tables (source, entity, mappings, rules)
│   ├── 03_control_tables.sql             ETL_CONTROL (watermark + state per entity)
│   ├── 04_logging_tables.sql             ETL_RUN_LOG, ETL_STEP_LOG, ETL_ERROR_LOG
│   ├── 05_target_tables.sql              DIM_STAFFING_SCHEDULE, DIM_STAFFING_TIMELINE,
│   │                                     DIM_COST, DIM_TIMELINE_COST
│   ├── 06_staging_tables.sql             STG_* tables (one per target table)
│   └── 07_views.sql                      V_ETL_ACTIVE_ENTITIES, V_ETL_RUN_SUMMARY
│
├── packages/
│   ├── 08_pkg_etl_logger.sql             Logging package (autonomous transactions)
│   ├── 09_pkg_etl_metadata.sql           Metadata read helpers (get_entity, get_mappings)
│   ├── 10_pkg_etl_control.sql            Watermark, lock, entity state management
│   ├── 11_pkg_etl_validator.sql          Staging validation (NOT_NULL, DERIVED, CHECK)
│   ├── 12_pkg_etl_scd2_loader.sql        Generic SCD2 and incremental loader
│   ├── 13_pkg_etl_transform_staffing.sql Entity-specific transform for STAFFING_SCHEDULE
│   ├── 14_pkg_etl_transform_cost.sql     Entity-specific transform for COST
│   └── 15_pkg_etl_orchestrator.sql       Main entry point (run_entity, run_all)
│
├── data/
│   └── 16_metadata_inserts.sql           Seed data (entities, mappings, validation rules)
│
└── monitoring/
    └── 17_monitoring_queries.sql          Operational monitoring SQL queries
```

---

## 4. Prerequisites

### Oracle Environment

| Requirement | Details |
|---|---|
| Oracle version | 12c or later (uses `GENERATED ALWAYS AS IDENTITY`, `STANDARD_HASH`) |
| Target schema | `GPC_DM` must exist |
| Source schema | `KBR_IHUB` must be accessible from the GPC_DM connection |
| Privileges | `CREATE TABLE`, `CREATE SEQUENCE`, `CREATE VIEW`, `CREATE PROCEDURE` on GPC_DM |
| Source access | `SELECT` privilege on `KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB` and `KBR_IHUB.APAC_PCDM_COST_IHUB` granted to GPC_DM |
| SQL client | SQL*Plus or SQLcl (scripts use `@@` for relative `@` includes) |

### Verify Source Access Before Installing

```sql
-- Run as GPC_DM user
SELECT COUNT(*) FROM KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB;
SELECT COUNT(*) FROM KBR_IHUB.APAC_PCDM_COST_IHUB;
```

Both must return without error.

---

## 5. Installation — Fresh Environment

### Step 1 — Connect as GPC_DM owner

```sql
-- SQL*Plus / SQLcl
CONNECT gpc_dm/<password>@<tns_alias>
```

### Step 2 — Navigate to the framework folder

```sql
-- SQLcl
cd /path/to/EDWH_Framework

-- SQL*Plus (set the correct path)
-- The @@ calls in 00_install_all.sql use relative paths,
-- so you must run the script from the EDWH_Framework directory.
```

### Step 3 — Run the master install script

```sql
@00_install_all.sql
```

The script runs 16 sub-scripts in dependency order:

| Step | Script | Creates |
|---|---|---|
| 1 | `ddl/01_sequences.sql` | 10 sequences |
| 2 | `ddl/02_metadata_tables.sql` | 4 metadata registry tables |
| 3 | `ddl/03_control_tables.sql` | `ETL_CONTROL` |
| 4 | `ddl/04_logging_tables.sql` | 3 logging tables |
| 5 | `ddl/05_target_tables.sql` | 4 DIM tables |
| 6 | `ddl/06_staging_tables.sql` | 4 STG tables |
| 7 | `ddl/07_views.sql` | 2 views |
| 8–15 | `packages/*.sql` | 8 packages |
| 16 | `data/16_metadata_inserts.sql` | Seed metadata + validation rules |

The script uses `WHENEVER SQLERROR EXIT FAILURE ROLLBACK` — any failure stops execution immediately and rolls back the current statement. Fix the error and re-run.

### Step 4 — Verify Installation

```sql
-- Check all packages compiled successfully
SELECT OBJECT_NAME, OBJECT_TYPE, STATUS
FROM   ALL_OBJECTS
WHERE  OWNER       = 'GPC_DM'
AND    OBJECT_TYPE IN ('PACKAGE','PACKAGE BODY')
ORDER BY OBJECT_NAME;
-- All rows should show STATUS = VALID

-- Check entities registered
SELECT * FROM GPC_DM.V_ETL_ACTIVE_ENTITIES;
-- Should return STAFFING_SCHEDULE and COST rows

-- Check validation rules registered
SELECT RULE_NAME, RULE_TYPE, COLUMN_NAME, ERROR_ACTION
FROM   GPC_DM.ETL_VALIDATION_RULE
ORDER BY MAPPING_ID, RULE_ID;
-- Should return 14 rules across 4 mappings
```

---

## 6. Installation — Existing Environment (Incremental Apply)

Use this when the schema already exists and you are applying updates from a newer version of the framework files.

### 6.1 — Add CHECK Rule Type to Constraint

Only needed if your existing environment was installed before the validation rule porting was done.

```sql
ALTER TABLE GPC_DM.ETL_VALIDATION_RULE DROP CONSTRAINT CHK_ETL_VR_TYPE;

ALTER TABLE GPC_DM.ETL_VALIDATION_RULE
    ADD CONSTRAINT CHK_ETL_VR_TYPE
        CHECK (RULE_TYPE IN ('NOT_NULL','DERIVED','CUSTOM','CHECK'));
```

### 6.2 — Recompile Updated Packages

Run any package file that has changed:

```sql
@packages/11_pkg_etl_validator.sql
@packages/15_pkg_etl_orchestrator.sql
```

Verify:

```sql
SELECT OBJECT_NAME, STATUS
FROM   ALL_OBJECTS
WHERE  OWNER = 'GPC_DM'
AND    OBJECT_TYPE = 'PACKAGE BODY'
AND    STATUS != 'VALID';
-- Must return no rows
```

### 6.3 — Apply New Metadata / Rules

Run only the INSERT blocks from `data/16_metadata_inserts.sql` that correspond to new content. The existing entity and mapping IDs must already exist.

```sql
-- First retrieve existing mapping IDs
SELECT m.MAPPING_ID, e.ENTITY_NAME, m.TARGET_TABLE
FROM   GPC_DM.ETL_TARGET_MAPPING m
JOIN   GPC_DM.ETL_ENTITY         e ON e.ENTITY_ID = m.ENTITY_ID
ORDER BY m.MAPPING_ID;

-- Then insert new validation rules using the actual MAPPING_IDs
INSERT INTO GPC_DM.ETL_VALIDATION_RULE (MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, ERROR_ACTION)
VALUES (<mapping_id>, 'MY_NEW_RULE', 'NOT_NULL', 'MY_COLUMN', 'REJECT');
COMMIT;
```

### 6.4 — Add New Columns to Existing Tables

If a source column is new and needs to be added to a target/staging table:

```sql
-- 1. Add column to target table
ALTER TABLE GPC_DM.DIM_STAFFING_SCHEDULE ADD (NEW_COLUMN VARCHAR2(100));

-- 2. Add column to staging table
ALTER TABLE GPC_DM.STG_STAFFING_SCHEDULE ADD (NEW_COLUMN VARCHAR2(100));

-- 3. Register column mapping (metadata only, no code change)
INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
    (MAPPING_ID, SOURCE_COLUMN, TARGET_COLUMN, IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
VALUES
    (<mapping_id>, 'SOURCE_NEW_COLUMN', 'NEW_COLUMN', 'N', 'Y', <next_order>);
COMMIT;
```

The transform package reads from the source using the source column names; if the source column is new, the transform package must also be updated (see section 12).

---

## 7. Running the ETL

### Run a Single Entity

```sql
EXEC GPC_DM.PKG_ETL_ORCHESTRATOR.run_entity('STAFFING_SCHEDULE');
EXEC GPC_DM.PKG_ETL_ORCHESTRATOR.run_entity('COST');
```

Entity names are case-insensitive (converted internally with `UPPER()`).

### Run All Active Entities

```sql
EXEC GPC_DM.PKG_ETL_ORCHESTRATOR.run_all;
```

Entities are processed in `ENTITY_ID` order. A failure in one entity is caught and logged; other entities continue.

### Scheduling (Oracle DBMS_SCHEDULER)

```sql
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'GPC_DM.ETL_DAILY_RUN',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN GPC_DM.PKG_ETL_ORCHESTRATOR.run_all; END;',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0',
        enabled         => TRUE,
        comments        => 'Daily ETL load for all GPC_DM entities'
    );
END;
/
```

### Watermark Behaviour

- First run (NULL watermark): processes **all** rows from the source table.
- Subsequent runs: processes only rows where `REPORTING_DATE > LAST_WATERMARK`.
- Watermark advances only on successful completion of all mappings for an entity.

### Manual Watermark Override

Use this to reprocess data from a specific date:

```sql
UPDATE GPC_DM.ETL_CONTROL
SET    LAST_WATERMARK = DATE '2025-01-01'
WHERE  ENTITY_ID = (SELECT ENTITY_ID FROM GPC_DM.ETL_ENTITY WHERE ENTITY_NAME = 'COST');
COMMIT;
```

Setting watermark to `NULL` triggers a full reload on the next run.

---

## 8. Monitoring and Operations

All monitoring queries are in `monitoring/17_monitoring_queries.sql`. Key queries:

### Current Entity Status

```sql
SELECT e.ENTITY_NAME, c.STATUS, c.LAST_WATERMARK, c.LAST_RUN_DATE
FROM   GPC_DM.ETL_CONTROL c
JOIN   GPC_DM.ETL_ENTITY  e ON e.ENTITY_ID = c.ENTITY_ID
ORDER BY e.ENTITY_NAME;
```

### Recent Run Summary

```sql
SELECT * FROM GPC_DM.V_ETL_RUN_SUMMARY
WHERE  ROWNUM <= 10;
```

### Errors in Last 24 Hours

```sql
SELECT RUN_ID, ENTITY_NAME, TARGET_TABLE, ERROR_CODE, ERROR_MESSAGE, ERROR_TIME
FROM   GPC_DM.ETL_ERROR_LOG
WHERE  ERROR_TIME >= SYSDATE - 1
ORDER BY ERROR_TIME DESC;
```

### Step-by-Step Detail for a Specific Run

```sql
SELECT STEP_NAME, STATUS, ROWS_AFFECTED, START_TIME, END_TIME, STEP_MESSAGE
FROM   GPC_DM.ETL_STEP_LOG
WHERE  RUN_ID = <your_run_id>
ORDER BY STEP_ID;
```

### Rejected Rows for a Run

```sql
-- STAFFING_SCHEDULE example
SELECT STG_ID, SCHEDULE_ID, STG_REJECT_REASON
FROM   GPC_DM.STG_STAFFING_SCHEDULE
WHERE  STG_RUN_ID = <your_run_id>
AND    STG_STATUS = 'REJECTED';

-- COST example
SELECT STG_ID, COST_ID, STG_REJECT_REASON
FROM   GPC_DM.STG_COST
WHERE  STG_RUN_ID = <your_run_id>
AND    STG_STATUS = 'REJECTED';
```

### Active Records in Dimension Tables

```sql
-- Current staffing schedules
SELECT * FROM GPC_DM.DIM_STAFFING_SCHEDULE WHERE IS_CURRENT = 'Y';

-- Current cost records
SELECT * FROM GPC_DM.DIM_COST WHERE IS_CURRENT = 'Y';
```

---

## 9. Troubleshooting and Recovery

### Entity Stuck in RUNNING State

If a run was interrupted (e.g. session killed), the entity remains in RUNNING state and cannot be re-run automatically.

```sql
-- Manually release the lock
UPDATE GPC_DM.ETL_CONTROL
SET    STATUS = 'IDLE'
WHERE  ENTITY_ID = (SELECT ENTITY_ID FROM GPC_DM.ETL_ENTITY WHERE ENTITY_NAME = 'STAFFING_SCHEDULE');
COMMIT;
```

### Entity in FAILED State

The entity will be retried automatically on the next `run_all` call. To retry immediately:

```sql
-- FAILED entities are automatically retried by run_all.
-- To retry a specific entity now:
EXEC GPC_DM.PKG_ETL_ORCHESTRATOR.run_entity('STAFFING_SCHEDULE');
```

### Package Compilation Errors

```sql
-- Show errors for a specific package
SELECT TEXT FROM ALL_ERRORS
WHERE  OWNER = 'GPC_DM'
AND    NAME  = 'PKG_ETL_VALIDATOR'
ORDER BY SEQUENCE;
```

### Many Rows Being Rejected

```sql
-- Count rejections by reason for a run
SELECT REGEXP_SUBSTR(STG_REJECT_REASON, '\[([^\]]+)\]', 1, LEVEL) AS RULE_FIRED,
       COUNT(*) AS ROW_COUNT
FROM   GPC_DM.STG_STAFFING_SCHEDULE
WHERE  STG_RUN_ID = <your_run_id>
AND    STG_STATUS = 'REJECTED'
CONNECT BY LEVEL <= REGEXP_COUNT(STG_REJECT_REASON, '\[')
GROUP BY REGEXP_SUBSTR(STG_REJECT_REASON, '\[([^\]]+)\]', 1, LEVEL)
ORDER BY 2 DESC;
```

### Temporarily Disable a Validation Rule

```sql
UPDATE GPC_DM.ETL_VALIDATION_RULE
SET    IS_ACTIVE = 'N'
WHERE  RULE_NAME = 'SCHEDULE_TYPE_CHECK';
COMMIT;
-- Re-enable after investigation:
UPDATE GPC_DM.ETL_VALIDATION_RULE SET IS_ACTIVE = 'Y' WHERE RULE_NAME = 'SCHEDULE_TYPE_CHECK';
COMMIT;
```

### Disable an Entity from run_all

```sql
UPDATE GPC_DM.ETL_CONTROL
SET    STATUS = 'DISABLED',
       NOTES  = 'Disabled 2026-04-01 — source table migration in progress'
WHERE  ENTITY_ID = (SELECT ENTITY_ID FROM GPC_DM.ETL_ENTITY WHERE ENTITY_NAME = 'COST');
COMMIT;
-- Re-enable:
UPDATE GPC_DM.ETL_CONTROL SET STATUS = 'IDLE' WHERE ENTITY_ID = ...;
COMMIT;
```

---

## 10. How to Add a New Source System

A source system is the upstream Oracle schema that contains the source tables (e.g. `KBR_IHUB`).

### Step 1 — Register the Source System

```sql
INSERT INTO GPC_DM.ETL_SOURCE_SYSTEM (SS_NAME, SS_SCHEMA, SS_DESCRIPTION)
VALUES ('MY_SOURCE', 'MY_SOURCE_SCHEMA', 'Description of the new source system');
COMMIT;
```

`SS_SCHEMA` is the Oracle schema name used in fully-qualified table references (e.g. `MY_SOURCE_SCHEMA.MY_TABLE`).

### Step 2 — Grant SELECT Privileges

```sql
-- Run as DBA or schema owner
GRANT SELECT ON MY_SOURCE_SCHEMA.MY_TABLE TO GPC_DM;
```

### Step 3 — Add Entities from this Source

Follow section 11 to register entities from this source system.

---

## 11. How to Add a New Entity / Table

An "entity" is a source table that feeds one or more target dimension tables. Adding a new entity requires changes in three areas: **metadata**, **DDL**, and **a new transform package**.

### Overview of Steps

1. Create the target DIM table(s) and staging STG table(s) in DDL
2. Create sequences for surrogate keys
3. Write a transform package
4. Register the entity, mappings, columns, and validation rules in metadata
5. Add a dispatch branch in the orchestrator

---

### Step 1 — Create DDL (target, staging, sequences)

#### Sequence for surrogate key

Add to `ddl/01_sequences.sql` (or run directly):

```sql
CREATE SEQUENCE GPC_DM.SEQ_DIM_MY_ENTITY START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
```

#### Target DIM table

```sql
CREATE TABLE GPC_DM.DIM_MY_ENTITY (
    DIM_ME_ID            NUMBER         NOT NULL,
    -- Business key
    MY_ENTITY_ID         VARCHAR2(50)   NOT NULL,
    -- Tracked attributes
    ATTRIBUTE_1          VARCHAR2(100),
    ATTRIBUTE_2          DATE,
    -- SCD2 control columns (for SCD2 load type)
    EFFECTIVE_START_DATE DATE           NOT NULL,
    EFFECTIVE_END_DATE   DATE           DEFAULT DATE '9999-12-31' NOT NULL,
    IS_CURRENT           VARCHAR2(1)    DEFAULT 'Y' NOT NULL,
    RECORD_HASH          VARCHAR2(64),
    -- Audit
    REPORTING_DATE       DATE,
    ETL_RUN_ID           NUMBER,
    ETL_LOAD_DATE        DATE           DEFAULT SYSDATE,
    CONSTRAINT PK_DIM_ME          PRIMARY KEY (DIM_ME_ID),
    CONSTRAINT UQ_DIM_ME_EFFSTART UNIQUE (MY_ENTITY_ID, EFFECTIVE_START_DATE)
);

CREATE INDEX GPC_DM.IDX_DIM_ME_BK ON GPC_DM.DIM_MY_ENTITY(MY_ENTITY_ID, IS_CURRENT);
```

For INCREMENTAL (no history), omit the SCD2 columns and use a simple UNIQUE on the business key:

```sql
CONSTRAINT UQ_DIM_ME_BK UNIQUE (MY_ENTITY_ID)
```

#### Staging table

```sql
CREATE TABLE GPC_DM.STG_MY_ENTITY (
    STG_ID              NUMBER         GENERATED ALWAYS AS IDENTITY NOT NULL,
    STG_RUN_ID          NUMBER         NOT NULL,
    STG_STATUS          VARCHAR2(20)   DEFAULT 'PENDING' NOT NULL
                        CONSTRAINT CHK_STG_ME_STATUS
                            CHECK (STG_STATUS IN ('PENDING','LOADED','REJECTED')),
    STG_ACTION          VARCHAR2(20)
                        CONSTRAINT CHK_STG_ME_ACTION
                            CHECK (STG_ACTION IN ('INSERT','UPDATE','UNCHANGED')),
    STG_REJECT_REASON   VARCHAR2(500),
    STG_RECORD_HASH     VARCHAR2(64),
    -- Payload columns (mirror DIM columns, minus SCD2 control cols)
    MY_ENTITY_ID        VARCHAR2(50),
    ATTRIBUTE_1         VARCHAR2(100),
    ATTRIBUTE_2         DATE,
    REPORTING_DATE      DATE,
    CONSTRAINT PK_STG_ME PRIMARY KEY (STG_ID)
);

CREATE INDEX GPC_DM.IDX_STG_ME_RUN ON GPC_DM.STG_MY_ENTITY(STG_RUN_ID, STG_STATUS);
CREATE INDEX GPC_DM.IDX_STG_ME_BK  ON GPC_DM.STG_MY_ENTITY(MY_ENTITY_ID, STG_RUN_ID);
```

---

### Step 2 — Write the Transform Package

Create `packages/XX_pkg_etl_transform_my_entity.sql`. Use `13_pkg_etl_transform_staffing.sql` or `14_pkg_etl_transform_cost.sql` as a template.

Key requirements for the transform package:

1. **Delete PENDING rows** from staging at the start (idempotency):
   ```sql
   DELETE FROM GPC_DM.STG_MY_ENTITY WHERE STG_STATUS = 'PENDING';
   ```

2. **Extract from source with watermark filter**:
   ```sql
   INSERT INTO GPC_DM.STG_MY_ENTITY (STG_RUN_ID, MY_ENTITY_ID, ATTRIBUTE_1, ...)
   SELECT p_run_id, MY_ENTITY_ID, ATTRIBUTE_1, ...
   FROM   MY_SOURCE_SCHEMA.MY_SOURCE_TABLE
   WHERE  REPORTING_DATE > p_from_watermark
   AND    REPORTING_DATE <= p_to_watermark;
   ```

3. **Compute RECORD_HASH** over tracked columns using `STANDARD_HASH`:
   ```sql
   STANDARD_HASH(ATTRIBUTE_1 || '|' || TO_CHAR(ATTRIBUTE_2,'YYYY-MM-DD'), 'SHA256')
   ```

4. **Log step counts** using `PKG_ETL_LOGGER.log_step` and `end_step`.

The package signature must be:

```sql
CREATE OR REPLACE PACKAGE GPC_DM.PKG_ETL_TRANSFORM_MY_ENTITY AS
    PROCEDURE transform(
        p_run_id         IN NUMBER,
        p_entity_id      IN NUMBER,
        p_from_watermark IN DATE,
        p_to_watermark   IN DATE
    );
END PKG_ETL_TRANSFORM_MY_ENTITY;
/
```

---

### Step 3 — Register Metadata

Run the following as a single PL/SQL block:

```sql
DECLARE
    v_ss_id   NUMBER;
    v_eid     NUMBER;
    v_mid     NUMBER;
BEGIN
    -- Source system (use existing or insert new)
    SELECT SS_ID INTO v_ss_id FROM GPC_DM.ETL_SOURCE_SYSTEM WHERE SS_NAME = 'KBR_IHUB';

    -- Entity
    INSERT INTO GPC_DM.ETL_ENTITY (SS_ID, ENTITY_NAME, SOURCE_TABLE, WATERMARK_COLUMN, WATERMARK_TYPE, DESCRIPTION)
    VALUES (v_ss_id, 'MY_ENTITY', 'MY_SOURCE_SCHEMA.MY_SOURCE_TABLE', 'REPORTING_DATE', 'DATE', 'My entity description')
    RETURNING ENTITY_ID INTO v_eid;

    -- Control row
    INSERT INTO GPC_DM.ETL_CONTROL (ENTITY_ID, STATUS) VALUES (v_eid, 'IDLE');

    -- Target mapping
    INSERT INTO GPC_DM.ETL_TARGET_MAPPING (
        ENTITY_ID, TARGET_TABLE, STAGING_TABLE, LOAD_TYPE,
        SURROGATE_KEY_COL, SURROGATE_SEQ_NAME, LOAD_ORDER
    ) VALUES (
        v_eid, 'GPC_DM.DIM_MY_ENTITY', 'GPC_DM.STG_MY_ENTITY', 'SCD2',
        'DIM_ME_ID', 'GPC_DM.SEQ_DIM_MY_ENTITY', 10
    ) RETURNING MAPPING_ID INTO v_mid;

    -- Column mappings (IS_BUSINESS_KEY='Y' for the join key; IS_TRACKED='Y' for hash)
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING (MAPPING_ID, SOURCE_COLUMN, TARGET_COLUMN, IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid, 'MY_ENTITY_ID', 'MY_ENTITY_ID', 'Y', 'N', 10);

    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING (MAPPING_ID, SOURCE_COLUMN, TARGET_COLUMN, IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid, 'ATTRIBUTE_1', 'ATTRIBUTE_1', 'N', 'Y', 20);

    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING (MAPPING_ID, SOURCE_COLUMN, TARGET_COLUMN, IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid, 'ATTRIBUTE_2', 'ATTRIBUTE_2', 'N', 'Y', 30);

    -- Validation rules
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, ERROR_ACTION)
    VALUES (v_mid, 'MY_ENTITY_ID_REQUIRED', 'NOT_NULL', 'MY_ENTITY_ID', 'REJECT');

    COMMIT;
END;
/
```

---

### Step 4 — Add Dispatch Branch in Orchestrator

Edit `packages/15_pkg_etl_orchestrator.sql` — add a `WHEN` branch in `dispatch_transform`:

```sql
WHEN 'MY_ENTITY' THEN
    GPC_DM.PKG_ETL_TRANSFORM_MY_ENTITY.transform(
        p_run_id, p_entity_id, p_from_watermark, p_to_watermark);
```

Then recompile the orchestrator:

```sql
@packages/15_pkg_etl_orchestrator.sql
```

---

### Step 5 — Test

```sql
-- Run the new entity
EXEC GPC_DM.PKG_ETL_ORCHESTRATOR.run_entity('MY_ENTITY');

-- Check results
SELECT * FROM GPC_DM.V_ETL_RUN_SUMMARY WHERE ROWNUM <= 5;
SELECT COUNT(*) FROM GPC_DM.DIM_MY_ENTITY WHERE IS_CURRENT = 'Y';
SELECT COUNT(*) FROM GPC_DM.STG_MY_ENTITY WHERE STG_STATUS = 'REJECTED';
```

---

## 12. How to Modify an Existing Entity (Columns, Load Type)

### Add a New Column to an Existing Entity

**1. Alter both the target and staging tables:**

```sql
ALTER TABLE GPC_DM.DIM_STAFFING_SCHEDULE   ADD (NEW_COL VARCHAR2(100));
ALTER TABLE GPC_DM.STG_STAFFING_SCHEDULE   ADD (NEW_COL VARCHAR2(100));
```

**2. Register the column mapping:**

```sql
-- Get the mapping_id
SELECT MAPPING_ID FROM GPC_DM.ETL_TARGET_MAPPING
WHERE  TARGET_TABLE = 'GPC_DM.DIM_STAFFING_SCHEDULE';

-- Insert column mapping
INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
    (MAPPING_ID, SOURCE_COLUMN, TARGET_COLUMN, IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
VALUES
    (<mapping_id>, 'SOURCE_NEW_COL', 'NEW_COL', 'N', 'Y', <next_order>);
COMMIT;
```

**3. Update the transform package** to include the new column in the INSERT SELECT statement. Recompile the package after editing.

**4. If the column should be included in change detection (SCD2)**, set `IS_TRACKED = 'Y'` and update the `STANDARD_HASH` expression in the transform package to include the new column.

### Remove a Column

1. Set `IS_ACTIVE = 'N'` on the column mapping (do not delete — preserves audit history).
2. Remove the column from the transform package INSERT SELECT.
3. Optionally: `ALTER TABLE ... DROP COLUMN ...` (destructive — confirm no historical queries depend on it).

### Change Load Type

Update the metadata row — no code change required:

```sql
UPDATE GPC_DM.ETL_TARGET_MAPPING
SET    LOAD_TYPE = 'INCREMENTAL'
WHERE  TARGET_TABLE = 'GPC_DM.DIM_MY_ENTITY';
COMMIT;
```

Note: changing from `SCD2` to `INCREMENTAL` means existing historical rows remain but new runs will MERGE rather than expire/insert. Verify the target table structure supports the new load type.

### Rename a Column

Do not rename metadata columns in place — this breaks hash consistency. Instead:

1. Add the new column (see above).
2. Backfill: `UPDATE GPC_DM.DIM_MY_ENTITY SET NEW_COL = OLD_COL;`
3. Set `IS_ACTIVE = 'N'` on the old column mapping.
4. Remove the old column from the transform package.
5. After a full reload cycle, optionally drop the old column.

---

## 13. How to Add or Modify Validation Rules

Validation rules are purely metadata — no package recompilation is needed to add or change rules.

### Add a NOT_NULL Rule

```sql
INSERT INTO GPC_DM.ETL_VALIDATION_RULE
    (MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, ERROR_ACTION)
VALUES
    (<mapping_id>, 'MY_COL_REQUIRED', 'NOT_NULL', 'MY_COLUMN', 'REJECT');
COMMIT;
```

### Add a CHECK Rule (enum or condition)

```sql
INSERT INTO GPC_DM.ETL_VALIDATION_RULE
    (MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, DERIVED_SQL, ERROR_ACTION)
VALUES (
    <mapping_id>,
    'MY_COL_VALUE_CHECK',
    'CHECK',
    'MY_COLUMN',
    'MY_COLUMN IS NULL OR MY_COLUMN IN (''VALUE_A'',''VALUE_B'',''VALUE_C'')',
    'REJECT'
);
COMMIT;
```

`DERIVED_SQL` for a CHECK rule must be a valid Oracle WHERE-clause expression over the staging table columns. The validator executes `WHERE NOT (<DERIVED_SQL>)` to find failing rows.

**NULL-safe pattern for optional columns:**
```sql
-- Allow NULL, but if populated must be a valid value:
'MY_COLUMN IS NULL OR MY_COLUMN IN (''A'',''B'')'

-- Mandatory column (combine with a NOT_NULL rule):
'MY_COLUMN IN (''A'',''B'')'
```

### Add a DERIVED Rule (compute a value then validate it is not NULL)

```sql
INSERT INTO GPC_DM.ETL_VALIDATION_RULE
    (MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, DERIVED_SQL, ERROR_ACTION)
VALUES (
    <mapping_id>,
    'DT_PERIOD_DERIVED',
    'DERIVED',
    'DT_PERIOD',
    'TO_CHAR(PERIOD_START_DATE, ''YYYYMM'')',
    'REJECT'
);
COMMIT;
```

The DERIVED rule:
1. Runs `UPDATE staging SET DT_PERIOD = (TO_CHAR(PERIOD_START_DATE,'YYYYMM')) WHERE DT_PERIOD IS NULL`
2. Then rejects any rows where `DT_PERIOD` is still NULL.

### Escalate a Rule to FAIL (abort on any violation)

```sql
UPDATE GPC_DM.ETL_VALIDATION_RULE
SET    ERROR_ACTION = 'FAIL'
WHERE  RULE_NAME = 'MY_COL_REQUIRED';
COMMIT;
```

When `ERROR_ACTION = 'FAIL'`, the entire run is aborted with an exception on first violation. Use this only for truly critical fields where partial loads are unacceptable.

### Disable a Rule Without Deleting It

```sql
UPDATE GPC_DM.ETL_VALIDATION_RULE
SET    IS_ACTIVE = 'N'
WHERE  RULE_NAME = 'MY_COL_VALUE_CHECK';
COMMIT;
```

### Execution Order

Rules execute in `RULE_ID` ascending order. Since IDs are assigned by sequence at insert time, rules inserted first execute first. To force a specific order, insert rules in the desired sequence. Typically:

1. DERIVED rules (populate columns first)
2. NOT_NULL rules (check required columns)
3. CHECK rules (validate values, including newly derived ones)

---

## 14. Validation Rules Reference

### Current Rules — STAFFING_SCHEDULE → DIM_STAFFING_SCHEDULE

| Rule Name | Type | Column | Condition | Action | C# Source |
|---|---|---|---|---|---|
| `PROJECT_ID_REQUIRED` | NOT_NULL | `PROJECT_ID` | Must not be NULL | REJECT | PaffExelValidator |
| `POSITION_ID_REQUIRED` | NOT_NULL | `POSITION_ID` | Must not be NULL | REJECT | TimelineExelValidator |
| `SCHEDULE_TYPE_CHECK` | CHECK | `SCHEDULE_TYPE` | NULL or IN ('Exempt','Non-Exempt','Exempt Agency','Non-Exempt Agency') | REJECT | TimelineExelValidator, PaffExelValidator |
| `SCHEDULE_STATUS_CHECK` | CHECK | `SCHEDULE_STATUS` | NULL or IN ('Open','Filled','Canceled','Approved','Rejected','Withdrawn','Pending') | REJECT | TimelineExelValidator, PaffExelValidator |
| `SCHEDULE_DATE_ORDER` | CHECK | `SCHEDULE_START_DATE` | NULL or START_DATE <= END_DATE | REJECT | TimelineExelValidator, PaffExelValidator |

### Current Rules — STAFFING_SCHEDULE → DIM_STAFFING_TIMELINE

| Rule Name | Type | Column | Condition | Action | C# Source |
|---|---|---|---|---|---|
| `ALLOCATED_HOURS_REQUIRED` | NOT_NULL | `ALLOCATED_HOURS` | Must not be NULL | REJECT | TimelineExelValidator |
| `ALLOCATED_HOURS_NON_NEGATIVE` | CHECK | `ALLOCATED_HOURS` | NULL or >= 0 | REJECT | TimelineExelValidator |
| `PERIOD_DATE_ORDER` | CHECK | `PERIOD_START_DATE` | NULL or PERIOD_START_DATE <= PERIOD_END_DATE | REJECT | TimelineExelValidator |

### Current Rules — COST → DIM_COST

| Rule Name | Type | Column | Condition | Action | C# Source |
|---|---|---|---|---|---|
| `PROJECT_ID_REQUIRED` | NOT_NULL | `PROJECT_ID` | Must not be NULL | REJECT | CostExcelValidator |
| `COST_TYPE_REQUIRED` | NOT_NULL | `COST_TYPE` | Must not be NULL | REJECT | CostExcelValidator |
| `COST_CATEGORY_CHECK` | CHECK | `COST_CATEGORY` | NULL or UPPER(COST_CATEGORY) IN ('COMPANY','CLIENT') | REJECT | CostExcelValidator |

### Current Rules — COST → DIM_TIMELINE_COST

| Rule Name | Type | Column | Condition | Action | C# Source |
|---|---|---|---|---|---|
| `AMOUNT_REQUIRED` | NOT_NULL | `AMOUNT` | Must not be NULL | REJECT | CostExcelValidator |
| `FORECAST_TYPE_REQUIRED` | NOT_NULL | `FORECAST_TYPE` | Must not be NULL | REJECT | CostExcelValidator |
| `CURVE_TYPE_VALUE_CHECK` | CHECK | `CURVE_TYPE` | NULL or IN ('ACTUAL','BUDGET','FORECAST') | REJECT | CostExcelValidator |

> **Note on CURVE_TYPE:** The C# validator checks against scheduling curve names ("0 - Linear" etc.). In EDWH, `CURVE_TYPE` is derived from `FORECAST_TYPE`/`BUDGET_VERSION` into ETL classification values. The SQL CHECK validates the derived classification, not the C# UI enum values.

---

## 15. Metadata Table Reference

### ETL_SOURCE_SYSTEM

Registers each upstream source system.

| Column | Type | Description |
|---|---|---|
| `SS_ID` | NUMBER | Surrogate PK |
| `SS_NAME` | VARCHAR2(100) | Unique name (e.g. `KBR_IHUB`) |
| `SS_SCHEMA` | VARCHAR2(100) | Oracle schema prefix for source tables |
| `SS_DESCRIPTION` | VARCHAR2(500) | Free text |
| `IS_ACTIVE` | VARCHAR2(1) | Y/N |

### ETL_ENTITY

One row per source table / logical entity.

| Column | Type | Description |
|---|---|---|
| `ENTITY_ID` | NUMBER | Surrogate PK |
| `SS_ID` | NUMBER | FK to ETL_SOURCE_SYSTEM |
| `ENTITY_NAME` | VARCHAR2(100) | Unique name used in run_entity() calls |
| `SOURCE_TABLE` | VARCHAR2(200) | Fully qualified source table |
| `WATERMARK_COLUMN` | VARCHAR2(100) | Column used for incremental detection |
| `WATERMARK_TYPE` | VARCHAR2(20) | DATE / TIMESTAMP / NUMBER |

### ETL_TARGET_MAPPING

Maps each entity to one or more target tables.

| Column | Type | Description |
|---|---|---|
| `MAPPING_ID` | NUMBER | Surrogate PK |
| `ENTITY_ID` | NUMBER | FK to ETL_ENTITY |
| `TARGET_TABLE` | VARCHAR2(200) | Fully qualified target DIM table |
| `STAGING_TABLE` | VARCHAR2(200) | Fully qualified staging STG table |
| `LOAD_TYPE` | VARCHAR2(20) | SCD1 / SCD2 / INCREMENTAL / APPEND |
| `SURROGATE_KEY_COL` | VARCHAR2(100) | Target column for surrogate key |
| `SURROGATE_SEQ_NAME` | VARCHAR2(200) | Sequence to call for surrogate key |
| `LOAD_ORDER` | NUMBER | Execution order within entity (lower = first) |

### ETL_COLUMN_MAPPING

Column-level source → target mapping.

| Column | Type | Description |
|---|---|---|
| `CM_ID` | NUMBER | Surrogate PK |
| `MAPPING_ID` | NUMBER | FK to ETL_TARGET_MAPPING |
| `SOURCE_COLUMN` | VARCHAR2(100) | Column name in staging table |
| `TARGET_COLUMN` | VARCHAR2(100) | Column name in target DIM table |
| `IS_BUSINESS_KEY` | VARCHAR2(1) | Y = included in join for SCD2/MERGE matching |
| `IS_TRACKED` | VARCHAR2(1) | Y = included in RECORD_HASH change detection |
| `COLUMN_ORDER` | NUMBER | Order for hash concatenation (must be consistent) |

### ETL_VALIDATION_RULE

Validation rules applied to staging rows before loading.

| Column | Type | Description |
|---|---|---|
| `RULE_ID` | NUMBER | Surrogate PK; determines execution order |
| `MAPPING_ID` | NUMBER | FK to ETL_TARGET_MAPPING |
| `RULE_NAME` | VARCHAR2(100) | Descriptive name; appears in rejection reasons |
| `RULE_TYPE` | VARCHAR2(20) | NOT_NULL / DERIVED / CHECK / CUSTOM |
| `COLUMN_NAME` | VARCHAR2(100) | Staging column the rule operates on |
| `DERIVED_SQL` | VARCHAR2(4000) | Expression (DERIVED) or WHERE predicate (CHECK) |
| `ERROR_ACTION` | VARCHAR2(10) | REJECT = continue; FAIL = abort run |
| `IS_ACTIVE` | VARCHAR2(1) | Y/N; disable without deleting |

### ETL_CONTROL

Run-state and watermark per entity.

| Column | Type | Description |
|---|---|---|
| `ENTITY_ID` | NUMBER | FK to ETL_ENTITY (unique) |
| `LAST_WATERMARK` | DATE | Watermark after last successful run; NULL = never run |
| `STATUS` | VARCHAR2(20) | IDLE / RUNNING / FAILED / DISABLED |
| `LAST_RUN_DATE` | DATE | Timestamp of last run attempt |
| `NOTES` | VARCHAR2(500) | Operator notes |

### ETL_RUN_LOG

One row per entity-level run and per mapping-level run.

| Column | Description |
|---|---|
| `RUN_ID` | Unique run identifier |
| `ENTITY_ID` | Entity being run |
| `MAPPING_ID` | NULL for entity-level entry; populated for mapping-level entries |
| `STATUS` | RUNNING / SUCCESS / FAILED / PARTIAL |
| `ROWS_READ` | Rows extracted from source |
| `ROWS_INSERTED` | New rows written to target |
| `ROWS_UPDATED` | Rows expired (SCD2) or updated |
| `ROWS_REJECTED` | Rows failed validation |

### ETL_STEP_LOG

Fine-grained step tracking (TRANSFORM, CLASSIFY, EXPIRE, INSERT, VALIDATE, MERGE).

### ETL_ERROR_LOG

Persistent error records. Written via autonomous transaction — survives ROLLBACK.

| Column | Description |
|---|---|
| `ERROR_CODE` | VAL_NOT_NULL / VAL_DERIVED_NULL / VAL_CHECK_FAILED / SCD2_POST_LOAD_DUP / ETL-nnnnn |
| `RECORD_KEY` | Serialised business key of the failing row |
| `RECORD_DATA` | Full staging row as CLOB for post-mortem |

---

*End of Manual*
