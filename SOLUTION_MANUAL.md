# GPC_DM EDWH Framework — Solution Manual

**Version:** 2.0
**Date:** 2026-04-07
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
10. [How to Add a New Source Entity](#10-how-to-add-a-new-source-entity)
11. [How to Modify an Existing Load Package](#11-how-to-modify-an-existing-load-package)
12. [Source Column Mapping Reference](#12-source-column-mapping-reference)
13. [Metadata Table Reference](#13-metadata-table-reference)

---

## 1. Solution Overview

The GPC_DM EDWH Framework is an Oracle PL/SQL pipeline that loads data from upstream source systems (KBR_IHUB) into the GPC_DM data warehouse dimension tables.

Key characteristics:

- **Non-destructive on target tables** — target DIM tables are shared with other processes. The framework never adds, removes, or modifies columns in those tables. Load packages write only to columns that already exist.
- **Dedicated load packages** — each entity (STAFFING, COST) has a purpose-built PL/SQL package that matches the specific load pattern, pivoting, and is_valid management required by the business.
- **GCS pre-process** — before loading, each package stages distinct coding structure combinations, calls `PRC_MERGE_GLOBAL_CODING_STRUCTURE`, and resolves `GCS_KEY` for every row.
- **is_valid flag management** — existing rows are invalidated (IS_VALID=0) before new rows are inserted (IS_VALID=1). No SCD2 IS_CURRENT logic on target tables.
- **Full audit trail** — every run, step, row count, and error is written to ETL logging tables.
- **IICS orchestration** — Informatica IICS calls the load packages via JDBC on a daily schedule. IICS does not move data; all processing happens inside Oracle.

### Current Entities

| Entity | Source Table | Target Tables | Load Pattern |
|---|---|---|---|
| STAFFING | `KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB` | `DIM_STAFFING_SCHEDULE` | is_valid, REPLACE_ALL or ADD_UPDATE |
| | | `DIM_STAFFING_TIMELINE` | is_valid, period unpivot |
| COST | `KBR_IHUB.APAC_PCDM_COST_IHUB` | `DIM_COST` | is_valid, full load per portfolio+date, CLASS pivot |

---

## 2. Architecture

### ETL Flow — STAFFING

```
KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB
        │
        ▼
PKG_STAFFING_LOAD.load(
    p_portfolio_id, p_reporting_date,
    p_execution_center,
    p_import_mode => 'REPLACE_ALL' | 'ADD_UPDATE',
    p_user
)
        │
        ├─ 1. GCS pre-process
        │       ├─ Stage distinct coding combinations → STG_GLOBAL_CODING_STRUCTURE
        │       └─ EXEC PRC_MERGE_GLOBAL_CODING_STRUCTURE → REF_GLOBAL_CODING_STRUCTURE
        │
        ├─ 2. Invalidate DIM_STAFFING_SCHEDULE rows (scope: portfolio + exec center + date)
        │       REPLACE_ALL → invalidate all rows in scope
        │       ADD_UPDATE  → invalidate only rows matching incoming GCS+employee+position
        │
        ├─ 3. Insert new DIM_STAFFING_SCHEDULE rows (IS_VALID=1)
        │       One row per source row, GCS_KEY resolved from REF_GLOBAL_CODING_STRUCTURE
        │
        ├─ 4. Invalidate DIM_STAFFING_TIMELINE rows (same scope/mode logic)
        │
        └─ 5. Insert new DIM_STAFFING_TIMELINE rows (IS_VALID=1)
                Period unpivot: one row per calendar month between
                PLAN and FORECAST date ranges (CROSS JOIN numbers generator)
                PLAN_HOURS / FORECAST_HOURS = HOURS_PER_WEEK within respective range
```

### ETL Flow — COST

```
KBR_IHUB.APAC_PCDM_COST_IHUB
        │
        ▼
PKG_COST_LOAD.load(p_portfolio_id, p_reporting_date, p_user)
        │
        ├─ 1. GCS pre-process (same as STAFFING, COST_FLAG=1)
        │
        ├─ 2. Invalidate DIM_COST rows (full load: all IS_VALID=1 for portfolio+date → 0)
        │
        └─ 3. Insert pivoted DIM_COST rows (IS_VALID=1)
                Each source row → N rows, one per CLASS:
                COST_BASIS=COMPANY → 5 rows (OB COMPANY, BUDGET, EAC COMPANY, FORECAST, EARNED)
                COST_BASIS=CLIENT  → 6 rows (OB CLIENT, BUDGET CLIENT, EAC CLIENT,
                                             FORECAST CLIENT, EARNED CLIENT, ACTUAL CLIENT)
```

### Import Modes (STAFFING only)

| Mode | Behaviour |
|---|---|
| `REPLACE_ALL` | Invalidate ALL existing rows for the portfolio + execution center + reporting date, then insert all incoming rows |
| `ADD_UPDATE` | Invalidate only rows whose GCS_KEY + EMPLOYEE_ID + POSITION_NUMBER matches an incoming row. Rows not in the file are left untouched |

### Staging Tables

Staging tables (`STG_*`) are used as intermediate landing zones before load to target.

| Column | Values | Meaning |
|---|---|---|
| `STG_STATUS` | `PENDING` | Inserted by transform; awaiting load |
| | `LOADED` | Written to target |
| | `REJECTED` | Failed validation; not loaded |
| `STG_ACTION` | `INSERT` / `UPDATE` / `UNCHANGED` / `MERGE` | Set by loader |
| `STG_REJECT_REASON` | Free text | Appended by each failing validation rule |

### Target Table Constraint

Target tables (`DIM_STAFFING_SCHEDULE`, `DIM_STAFFING_TIMELINE`, `DIM_COST`) are shared with other processes. This framework:
- **Never adds, removes, or modifies columns** on these tables
- Writes only to columns that already exist
- Uses `IS_VALID` for record validity management (not SCD2 IS_CURRENT)

---

## 3. File Inventory

```
EDWH_Framework/
├── 00_install_all.sql                     Master install script
├── verify_install.sql                     Post-install health check
│
├── ddl/
│   ├── 00_migrate_target_tables.sql       DISABLED — no column changes to target tables
│   ├── 01_sequences.sql                   All sequences (surrogate keys, log IDs)
│   ├── 02_metadata_tables.sql             Registry tables (source, entity, mappings, rules)
│   ├── 03_control_tables.sql              ETL_CONTROL (state per entity)
│   ├── 04_logging_tables.sql              ETL_RUN_LOG, ETL_STEP_LOG, ETL_ERROR_LOG
│   ├── 05_target_tables.sql               Reference DDL only (NOT run — tables pre-exist)
│   ├── 06_staging_tables.sql              STG_* tables (one per target table)
│   └── 07_views.sql                       7 operational views
│
├── packages/
│   ├── 08_pkg_etl_logger.sql              Logging package (autonomous transactions)
│   ├── 09_pkg_etl_metadata.sql            Metadata read helpers
│   ├── 10_pkg_etl_control.sql             Entity state management
│   ├── 11_pkg_etl_validator.sql           Staging row validation
│   ├── 12_pkg_etl_scd2_loader.sql         Generic SCD2 loader (framework infrastructure)
│   ├── 13_pkg_etl_transform_staffing.sql  Legacy transform (reference only)
│   ├── 14_pkg_etl_transform_cost.sql      Legacy transform (reference only)
│   ├── 15_pkg_etl_orchestrator.sql        Generic orchestrator (framework infrastructure)
│   ├── 16_pkg_staffing_load.sql           STAFFING dedicated load package (active)
│   └── 17_pkg_cost_load.sql               COST dedicated load package (active)
│
├── data/
│   └── 16_metadata_inserts.sql            Seed data (entities, mappings, validation rules)
│
├── IICS/
│   ├── IICS_SETUP_GUIDE.md                Step-by-step IICS configuration guide
│   ├── connections/
│   │   └── Oracle_GPC_DM_Connection.xml   Oracle JDBC connection definition
│   ├── taskflows/
│   │   └── TF_GPC_DM_ETL_DAILY_RUN.xml    IICS taskflow definition
│   └── sql/
│       ├── 01_pre_check.sql               Verify entities are runnable
│       ├── 02_run_staffing_schedule.sql   Call STAFFING load procedure
│       ├── 03_run_cost.sql                Call COST load procedure
│       ├── 04_capture_run_log.sql         Read V_IICS_RUN_SUMMARY
│       └── 05_capture_error_log.sql       Read V_IICS_ERROR_SUMMARY
│
├── SOLUTION_MANUAL.md                     This file
└── SOURCE_TABLES_SETUP.md                 Quick-start setup guide
```

### Views Created by 07_views.sql

| View | Purpose |
|---|---|
| `V_ETL_ACTIVE_ENTITIES` | Active entities with mapping details and control status |
| `V_ETL_RUN_SUMMARY` | Last 7 days of run history with elapsed time and row counts |
| `V_DIM_STAFFING_CURRENT` | All columns of DIM_STAFFING_SCHEDULE where IS_CURRENT=1 |
| `V_DIM_COST_CURRENT` | All columns of DIM_COST where IS_CURRENT=1 |
| `V_ETL_STAGING_SUMMARY` | Staging row counts by run, status, and action |
| `V_IICS_RUN_SUMMARY` | Today's run log entries (used by IICS T4 task) |
| `V_IICS_ERROR_SUMMARY` | Today's error log entries (used by IICS T5 task) |

---

## 4. Prerequisites

### Oracle Environment

| Requirement | Details |
|---|---|
| Oracle version | 12c or later |
| Target schema | `GPC_DM` must exist |
| Source schema | `KBR_IHUB` must be accessible from the GPC_DM connection |
| Privileges | `CREATE TABLE`, `CREATE SEQUENCE`, `CREATE VIEW`, `CREATE PROCEDURE` on GPC_DM |
| Source access | `SELECT` on `KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB` and `KBR_IHUB.APAC_PCDM_COST_IHUB` |
| GCS tables | `GPC_DM.STG_GLOBAL_CODING_STRUCTURE`, `GPC_DM.REF_GLOBAL_CODING_STRUCTURE`, `GPC_DM.PRC_MERGE_GLOBAL_CODING_STRUCTURE` must exist |
| SQL client | SQL Developer (VS Code extension) or SQLcl |

### Verify Before Installing

```sql
SELECT COUNT(*) FROM KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB;
SELECT COUNT(*) FROM KBR_IHUB.APAC_PCDM_COST_IHUB;
SELECT COUNT(*) FROM GPC_DM.STG_GLOBAL_CODING_STRUCTURE;
SELECT COUNT(*) FROM GPC_DM.REF_GLOBAL_CODING_STRUCTURE;
```

---

## 5. Installation — Fresh Environment

### Step 1 — Connect as GPC_DM

```sql
CONNECT gpc_dm/<password>@<tns_alias>
```

### Step 2 — Run the master install script

In SQL Developer (VS Code extension):

```sql
@C:\<path>\EDWH_Framework\00_install_all.sql
```

The script runs sub-scripts in this order:

| Step | Script | Creates |
|---|---|---|
| 1 | `ddl/01_sequences.sql` | 12 sequences |
| 2 | `ddl/02_metadata_tables.sql` | 5 metadata registry tables |
| 3 | `ddl/03_control_tables.sql` | `ETL_CONTROL` |
| 4 | `ddl/04_logging_tables.sql` | 3 logging tables |
| 5 | `ddl/05_target_tables.sql` | Skipped — target tables pre-exist |
| 6 | `ddl/06_staging_tables.sql` | 4 STG tables |
| 7 | `ddl/07_views.sql` | 7 views |
| 8–15 | `packages/08–15_*.sql` | 8 framework packages |
| 16 | `data/16_metadata_inserts.sql` | Seed metadata + validation rules |
| 16b | `packages/16_pkg_staffing_load.sql` | STAFFING dedicated load package |
| 16c | `packages/17_pkg_cost_load.sql` | COST dedicated load package |

### Step 3 — Verify with verify_install.sql

```sql
@C:\<path>\EDWH_Framework\verify_install.sql
```

Expected result:
- Sequences: 12 FOUND
- Tables: 17 FOUND
- Views: 7 VALID
- Packages: 16 rows, all VALID (8 packages × spec + body) + 2 dedicated packages
- Seed data: ETL_ENTITY ≥ 2 rows, ETL_VALIDATION_RULE ≥ 1 row

---

## 6. Installation — Existing Environment (Incremental Apply)

When target tables already exist, only run the steps that install new objects:

```sql
-- Views (CREATE OR REPLACE — always safe to re-run)
@C:\<path>\EDWH_Framework\ddl\07_views.sql

-- Packages (in order)
@C:\<path>\EDWH_Framework\packages\08_pkg_etl_logger.sql
@C:\<path>\EDWH_Framework\packages\09_pkg_etl_metadata.sql
@C:\<path>\EDWH_Framework\packages\10_pkg_etl_control.sql
@C:\<path>\EDWH_Framework\packages\11_pkg_etl_validator.sql
@C:\<path>\EDWH_Framework\packages\12_pkg_etl_scd2_loader.sql
@C:\<path>\EDWH_Framework\packages\13_pkg_etl_transform_staffing.sql
@C:\<path>\EDWH_Framework\packages\14_pkg_etl_transform_cost.sql
@C:\<path>\EDWH_Framework\packages\15_pkg_etl_orchestrator.sql
@C:\<path>\EDWH_Framework\packages\16_pkg_staffing_load.sql
@C:\<path>\EDWH_Framework\packages\17_pkg_cost_load.sql

-- Seed metadata
@C:\<path>\EDWH_Framework\data\16_metadata_inserts.sql
```

Do **not** run `ddl/05_target_tables.sql` or `ddl/00_migrate_target_tables.sql` — target tables must not be altered.

---

## 7. Running the ETL

### Run STAFFING Load

```sql
-- Replace All mode (default): invalidates all existing rows for this scope
EXEC GPC_DM.PKG_STAFFING_LOAD.load(
    p_portfolio_id     => 'P001',
    p_reporting_date   => DATE '2026-04-01',
    p_execution_center => 'AB',
    p_import_mode      => 'REPLACE_ALL',
    p_user             => 'system'
);

-- Add/Update mode: only invalidates rows matching incoming data
EXEC GPC_DM.PKG_STAFFING_LOAD.load(
    p_portfolio_id     => 'P001',
    p_reporting_date   => DATE '2026-04-01',
    p_execution_center => 'AB',
    p_import_mode      => 'ADD_UPDATE',
    p_user             => 'john.doe'
);
```

### Run COST Load

```sql
-- Full load per portfolio + reporting date
EXEC GPC_DM.PKG_COST_LOAD.load(
    p_portfolio_id   => 'P001',
    p_reporting_date => DATE '2026-04-01',
    p_user           => 'system'
);
```

### IICS Daily Schedule

IICS calls these procedures automatically at 02:00 UTC via the `TF_GPC_DM_ETL_DAILY_RUN` taskflow. See `IICS/IICS_SETUP_GUIDE.md` for configuration details.

---

## 8. Monitoring and Operations

### Check Recent Run History

```sql
SELECT ENTITY_NAME, TARGET_TABLE, STATUS, ROWS_INSERTED, ROWS_REJECTED,
       START_TIME, ELAPSED_SECS
FROM   GPC_DM.V_ETL_RUN_SUMMARY
ORDER BY RUN_ID DESC;
```

### Check Today's Errors

```sql
SELECT * FROM GPC_DM.V_IICS_ERROR_SUMMARY ORDER BY ERROR_TIME DESC;
```

### Check Staging Status

```sql
SELECT STAGING_TABLE, STG_STATUS, STG_ACTION, ROW_COUNT
FROM   GPC_DM.V_ETL_STAGING_SUMMARY
ORDER BY STG_RUN_ID DESC, STAGING_TABLE;
```

### Check Target Table Validity Counts

```sql
SELECT 'DIM_STAFFING_SCHEDULE' AS TBL,
       SUM(CASE WHEN IS_VALID = 1 THEN 1 ELSE 0 END) AS VALID_ROWS,
       SUM(CASE WHEN IS_VALID = 0 THEN 1 ELSE 0 END) AS INVALID_ROWS
FROM   GPC_DM.DIM_STAFFING_SCHEDULE
UNION ALL
SELECT 'DIM_STAFFING_TIMELINE',
       SUM(CASE WHEN IS_VALID = 1 THEN 1 ELSE 0 END),
       SUM(CASE WHEN IS_VALID = 0 THEN 1 ELSE 0 END)
FROM   GPC_DM.DIM_STAFFING_TIMELINE
UNION ALL
SELECT 'DIM_COST',
       SUM(CASE WHEN IS_VALID = 1 THEN 1 ELSE 0 END),
       SUM(CASE WHEN IS_VALID = 0 THEN 1 ELSE 0 END)
FROM   GPC_DM.DIM_COST;
```

### Step-Level Timing

```sql
SELECT s.STEP_NAME, s.STATUS, s.ROWS_AFFECTED,
       s.START_TIME, s.END_TIME, s.MESSAGE
FROM   GPC_DM.ETL_STEP_LOG s
WHERE  s.RUN_ID = <run_id>
ORDER BY s.STEP_ID;
```

---

## 9. Troubleshooting and Recovery

### Load Rolled Back / Partial Data

Both `PKG_STAFFING_LOAD` and `PKG_COST_LOAD` issue a `ROLLBACK` on any unhandled exception. The target tables are left in their pre-load state. Simply re-run after fixing the root cause.

### ORA-00904 on Package Compilation

A column name referenced in the package doesn't exist in the source or target table. Check:
```sql
SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS
WHERE  OWNER = 'KBR_IHUB'
AND    TABLE_NAME = 'APAC_PCDM_STAFFING_SCHEDULE_IHUB'
ORDER BY COLUMN_ID;
```

### GCS_KEY = NULL After Load

The `PRC_MERGE_GLOBAL_CODING_STRUCTURE` procedure did not create a matching entry in `REF_GLOBAL_CODING_STRUCTURE`. Check:
```sql
SELECT * FROM GPC_DM.STG_GLOBAL_CODING_STRUCTURE
WHERE  PORTFOLIO_ID = '<portfolio>'
AND    TRUNC(DT_CREATED) = TRUNC(SYSDATE);
```

### No Rows Loaded

Check that the portfolio + reporting date combination exists in the source:
```sql
SELECT COUNT(*), PORTFOLIO_ID, TRUNC(REPORTING_DATE)
FROM   KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB
WHERE  PORTFOLIO_ID = '<portfolio>'
GROUP BY PORTFOLIO_ID, TRUNC(REPORTING_DATE);
```

### Re-run ETL Error Log

```sql
SELECT ENTITY_NAME, ERROR_CODE, ERROR_MESSAGE, ERROR_TIME
FROM   GPC_DM.ETL_ERROR_LOG
WHERE  ERROR_TIME >= SYSDATE - 1
ORDER BY ERROR_TIME DESC;
```

---

## 10. How to Add a New Source Entity

1. **Create a dedicated load package** following the pattern of `16_pkg_staffing_load.sql` or `17_pkg_cost_load.sql`.
2. **Create staging table(s)** mirroring the target table columns needed.
3. **Register in metadata** (`ETL_ENTITY`, `ETL_TARGET_MAPPING`, `ETL_COLUMN_MAPPING`, `ETL_VALIDATION_RULE`) via `16_metadata_inserts.sql`.
4. **Add to IICS taskflow** — add a new SQL task calling the new package, wire into the existing decision + email tasks.
5. **Wire into 00_install_all.sql** — add the new package file reference.

---

## 11. How to Modify an Existing Load Package

### Change a Column Name (Source Renamed)

Edit the relevant package file and update the column reference:
- `packages/16_pkg_staffing_load.sql` for STAFFING
- `packages/17_pkg_cost_load.sql` for COST

Re-run the package file to recompile:
```sql
@C:\<path>\EDWH_Framework\packages\16_pkg_staffing_load.sql
```

Verify:
```sql
SELECT OBJECT_NAME, STATUS FROM ALL_OBJECTS
WHERE  OWNER = 'GPC_DM' AND OBJECT_NAME = 'PKG_STAFFING_LOAD';
```

### Add a New CLASS to the COST Pivot

Add a new `UNION ALL` block to the pivot query in `17_pkg_cost_load.sql` with the new CLASS label and the source column(s) for HOURS and COST.

### Change the STAFFING Period Expansion Logic

The period expansion is in STEP 5 of `PKG_STAFFING_LOAD`. The numbers generator (`CONNECT BY LEVEL <= 360`) produces 360 monthly periods maximum. Adjust if needed.

---

## 12. Source Column Mapping Reference

### STAFFING — DIM_STAFFING_SCHEDULE

| Source Column | Target Column | Notes |
|---|---|---|
| `PORTFOLIO_ID` (parameter) | `PROJECT_ID` | PORTFOLIO_ID is passed as parameter |
| `PROJECT_ID` | — | Used for GCS key lookup only |
| `BUSINESS_UNIT` | `BUSINESS_UNIT_ID` | |
| `EXECUTION_CENTER` (first 2 chars) | `OPERATING_CENTER_ID` | |
| `BILL_TYPE` (first 2 chars) | `BILL_TYPE_ID` | |
| `WBS_1_CODE` | `WBS1_ID` | |
| `WBS_2_CODE` | `WBS2_ID` | |
| `COST_TYPE` (first 5 chars) | `COST_TYPE_ID` | |
| `CBS` (first 7 chars) | `CBS_ID` | |
| `POSITION_NUMBER` | `POSITION_NUMBER` | |
| `POSITION_TITLE` | `POSITION_TITLE` | |
| `FULL_PART_TIME` | `FULLTIME_PARTIME` | |
| `EMPLOYEE_ID` | `EMPLOYEE_ID` | |
| `EMPLOYEE_NAME` | `EMPLOYEE_NAME` | |
| `CONTRACT_TYPE` | `CONTRACT_TYPE` | |
| `JOB_TITLE` | `JOB_TITLE` | |
| `PRIORITY` | `PRIORITY` | |
| `PLAN_START_DATE` | `DT_PLAN_START` | |
| `PLAN_END_DATE` | `DT_PLAN_END` | |
| `FORECAST_START_DATE` | `DT_FORECAST_START` | |
| `FORECAST_END_DATE` | `DT_FORECAST_END` | |
| `ACTUAL_START_DATE` | `DT_ACTUAL_START` | |
| `ACTUAL_END_DATE` | `DT_ACTUAL_END` | |
| `HOURS_PER_WEEK` | `HOURS_PER_WEEK` | |
| `STATUS` | `STATUS` | |
| `NEW_HIRE` | `NEW_HIRE` | |
| Derived | `GCS_KEY` | From REF_GLOBAL_CODING_STRUCTURE |
| Derived | `STS_GLOBAL_STRUCTURE` | PORTFOLIO.PROJECT.BU.EXEC.BILL.WBS1.WBS2.COSTYPE.CBS |

### STAFFING — DIM_STAFFING_TIMELINE

| Source Column | Target Column | Notes |
|---|---|---|
| `PORTFOLIO_ID` (parameter) | `PROJECT_ID` | |
| `POSITION_NUMBER` | `POSITION_NUMBER` | Repeated per period |
| `POSITION_TITLE` | `POSITION_TITLE` | Repeated per period |
| `EMPLOYEE_ID` | `EMPLOYEE_ID` | Repeated per period |
| `EMPLOYEE_NAME` | `EMPLOYEE_NAME` | Repeated per period |
| `HOURS_PER_WEEK` | `PLAN_HOURS` | Applied for periods within PLAN range |
| `HOURS_PER_WEEK` | `FORECAST_HOURS` | Applied for periods within FORECAST range |
| Derived | `DT_PERIOD` | First day of each month in the date range |
| Derived | `GCS_KEY` | From REF_GLOBAL_CODING_STRUCTURE |

### COST — DIM_COST (per CLASS row)

| Source Column | Target Column | Notes |
|---|---|---|
| `PORTFOLIO_ID` (parameter) | `PROJECT_ID` | |
| `BUSINESS_UNIT` | `BUSINESS_UNIT_ID` | |
| `EXECUTION_CENTER` (first 2 chars) | `OPERATING_CENTER_ID` | |
| `BILL_TYPE` (first 2 chars) | `BILL_TYPE_ID` | |
| `WBS_1_CODE` | `WBS1_ID` | |
| `WBS_2_CODE` | `WBS2_ID` | |
| `COST_TYPE` (first 5 chars) | `COST_TYPE_ID` | |
| `CBS` (first 7 chars) | `CBS_ID` | |
| `COST_BASIS` | `COST_BASIS` | COMPANY or CLIENT |
| `CURRENCY_CODE` | `CURRENCY_CODE` | |
| `ACTUAL_PERCENT_COMPLETE` | `ACTUAL_PERCENT_COMPLETE` | |
| `BASELINE_START` | `DT_BASELINE_START` | |
| `BASELINE_END` | `DT_BASELINE_END` | |
| `FORECAST_START_DATE` | `DT_FORECAST_START` | |
| `FORECAST_END_DATE` | `DT_FORECAST_END` | |
| Derived | `CLASS` | See pivot logic below |
| Derived | `HOURS` | Budget hours column per CLASS |
| Derived | `COST` | Budget cost column per CLASS |
| Derived | `GCS_KEY` | From REF_GLOBAL_CODING_STRUCTURE |
| Derived | `STS_GLOBAL_STRUCTURE` | PORTFOLIO.PROJECT.BU.EXEC.BILL.WBS1.WBS2.COSTYPE.CBS |
| Literal | `DATA_SOURCE` | Always `'GENERIC LOAD'` |

#### COST CLASS Pivot Logic

| COST_BASIS | CLASS | HOURS column | COST column |
|---|---|---|---|
| COMPANY | OB COMPANY | `ORIGINAL_BUDGET_HOURS` | `ORIGINAL_BUDGET_COST` |
| COMPANY | BUDGET | `CURRENT_BUDGET_HOURS` | `CURRENT_BUDGET_COST` |
| COMPANY | EAC COMPANY | `FORECAST_BUDGET_HOURS` | `FORECAST_BUDGET_COST` |
| COMPANY | FORECAST | `ETC_BUDGET_HOURS` | `ETC_BUDGET_COST` |
| COMPANY | EARNED | `EARNED_BUDGET_HOURS` | `EARNED_BUDGET_COST` |
| CLIENT | OB CLIENT | `ORIGINAL_BUDGET_HOURS` | `ORIGINAL_BUDGET_COST` |
| CLIENT | BUDGET CLIENT | `CURRENT_BUDGET_HOURS` | `CURRENT_BUDGET_COST` |
| CLIENT | EAC CLIENT | `FORECAST_BUDGET_HOURS` | `FORECAST_BUDGET_COST` |
| CLIENT | FORECAST CLIENT | `ETC_BUDGET_HOURS` | `ETC_BUDGET_COST` |
| CLIENT | EARNED CLIENT | `EARNED_BUDGET_HOURS` | `EARNED_BUDGET_COST` |
| CLIENT | ACTUAL CLIENT | `CLIENT_HOURS` | `VALUE_OF_WORK_DONE` |

---

## 13. Metadata Table Reference

| Table | Purpose |
|---|---|
| `ETL_SOURCE_SYSTEM` | Registered source systems (KBR_IHUB) |
| `ETL_ENTITY` | One row per entity (STAFFING, COST) |
| `ETL_TARGET_MAPPING` | One row per source→target mapping |
| `ETL_COLUMN_MAPPING` | Column-level mappings per target mapping |
| `ETL_VALIDATION_RULE` | Validation rules per target mapping |
| `ETL_CONTROL` | Current state and last run per entity |
| `ETL_RUN_LOG` | One row per run per mapping |
| `ETL_STEP_LOG` | Step-level timing within a run |
| `ETL_ERROR_LOG` | Row-level errors with full messages |
