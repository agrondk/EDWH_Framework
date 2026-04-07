# IICS Setup Guide — GPC_DM ETL Daily Run

How to deploy and configure the IICS taskflow that triggers the EDWH ETL loads for STAFFING_SCHEDULE and COST.

---

## Overview

IICS acts as the **trigger and monitor** only. All data movement, transformation, and loading happens inside Oracle via dedicated PL/SQL packages. IICS passes parameters (portfolio, date, mode) to Oracle and reads the Oracle log views to confirm success or report errors.

```
IICS Taskflow (TF_GPC_DM_ETL_DAILY_RUN)
    │
    ├─ T1  Pre-check         → Verify source tables accessible; no stuck runs for this scope
    ├─ T2  Run Staffing      → BEGIN GPC_DM.PKG_STAFFING_LOAD.load(...); END;
    ├─ T3  Run Cost          → BEGIN GPC_DM.PKG_COST_LOAD.load(...); END;
    ├─ T4  Capture Run Log   → SELECT from V_IICS_RUN_SUMMARY
    ├─ T5  Capture Errors    → SELECT from V_IICS_ERROR_SUMMARY
    ├─ T6  Decision          → error_count = 0 ?
    ├─ T7  Email Success     → run summary to team
    └─ T8  Email Failure     → error detail + diagnostic queries to team
```

**Input parameters** (passed to the taskflow per execution):

| Parameter | Example | Description |
|---|---|---|
| `$$v_portfolio_id` | `GPC-001` | Portfolio to process |
| `$$v_reporting_date` | `2025-03-31` | Reporting date (YYYY-MM-DD) |
| `$$v_execution_center` | `AU` | Execution centre code (STAFFING) |
| `$$v_import_mode` | `REPLACE_ALL` | `REPLACE_ALL` or `ADD_UPDATE` (STAFFING) |
| `$$v_user` | `IICS_DAILY` | Audit user label |

---

## File Inventory

```
IICS/
├── IICS_SETUP_GUIDE.md              ← this file
├── connections/
│   └── Oracle_GPC_DM_Connection.xml ← Oracle JDBC connection definition
├── taskflows/
│   └── TF_GPC_DM_ETL_DAILY_RUN.xml ← main taskflow
└── sql/
    ├── 01_pre_check.sql             ← T1: verify source access and no stuck runs
    ├── 02_run_staffing_schedule.sql ← T2: call PKG_STAFFING_LOAD.load
    ├── 03_run_cost.sql              ← T3: call PKG_COST_LOAD.load
    ├── 04_capture_run_log.sql       ← T4: read V_IICS_RUN_SUMMARY
    └── 05_capture_error_log.sql     ← T5: read V_IICS_ERROR_SUMMARY
```

Oracle views `V_IICS_RUN_SUMMARY` and `V_IICS_ERROR_SUMMARY` are defined in `ddl/07_views.sql` and deployed by `00_install_all.sql`.

---

## Prerequisites

| Item | Requirement |
|---|---|
| Oracle Framework | `00_install_all.sql` fully deployed in GPC_DM |
| Seed data | `data/16_metadata_inserts.sql` executed |
| IICS version | Cloud Data Integration (CDI) — any current version |
| Secure Agent | Installed in the same network zone as the Oracle DB |
| Oracle JDBC driver | `ojdbc8.jar` or `ojdbc11.jar` on the Secure Agent |
| Oracle privileges | GPC_DM user must have EXECUTE on `PKG_STAFFING_LOAD`, `PKG_COST_LOAD` and SELECT on all ETL log tables and IICS views |
| Email relay | SMTP configured in IICS Administrator |

### Verify Oracle prerequisites

```sql
-- Run as GPC_DM
SELECT OBJECT_NAME, OBJECT_TYPE, STATUS
FROM   ALL_OBJECTS
WHERE  OWNER = 'GPC_DM'
AND    OBJECT_NAME IN (
    'PKG_STAFFING_LOAD', 'PKG_COST_LOAD',
    'V_IICS_RUN_SUMMARY', 'V_IICS_ERROR_SUMMARY'
);
-- All 4 must show STATUS = VALID
```

---

## Step 1 — Install Oracle JDBC Driver on Secure Agent

1. Download `ojdbc8.jar` from Oracle (must match your Oracle DB version).
2. Copy to the Secure Agent host:
   ```
   $INFA_HOME/services/shared/javalib/ojdbc8.jar
   ```
3. Restart the Secure Agent service.
4. Verify in IICS Administrator → Runtime Environments → your agent → Connector details.

---

## Step 2 — Create the Oracle Connection in IICS

**IICS Administrator → Connections → New Connection**

| Field | Value |
|---|---|
| Connection Name | `Oracle_GPC_DM` |
| Type | Oracle |
| Runtime Environment | `<your Secure Agent group>` |
| Host | `<oracle_host>` |
| Port | `1521` (or your listener port) |
| Service Name | `<oracle_service_name>` |
| Username | `GPC_DM` |
| Password | `<gpc_dm_password>` — use IICS secure credential store |
| Schema | `GPC_DM` |

**Test the connection** before proceeding — the Test button must return "Connection successful".

Alternatively, import `connections/Oracle_GPC_DM_Connection.xml` and update the `{HOST}`, `{PORT}`, `{SERVICE_NAME}`, `{GPC_DM_PASSWORD}` placeholders.

---

## Step 3 — Deploy the IICS Views in Oracle

If not already deployed (run this once):

```sql
@ddl/07_views.sql

-- Verify:
SELECT VIEW_NAME FROM ALL_VIEWS WHERE OWNER = 'GPC_DM' AND VIEW_NAME LIKE 'V_IICS%';
-- Expected: V_IICS_RUN_SUMMARY, V_IICS_ERROR_SUMMARY
```

---

## Step 4 — Create the Taskflow in IICS

### Option A — Manual creation in IICS Designer

**IICS Designer → New → Taskflow → Blank**

Create the following tasks in order:

#### T1 — SQL Task: T1_PRE_CHECK
- **Connection:** `Oracle_GPC_DM`
- **SQL:** paste content of `sql/01_pre_check.sql`
- **Output parameter:** `BLOCKED_COUNT` → integer variable `$$v_blocked_count`
- **On Success:** go to T2
- **On Failure:** go to T8_EMAIL_FAILURE

#### T2 — SQL Task: T2_RUN_STAFFING
- **Connection:** `Oracle_GPC_DM`
- **SQL:** paste content of `sql/02_run_staffing_schedule.sql`
- **Input parameters:** `$$v_portfolio_id`, `$$v_reporting_date`, `$$v_execution_center`, `$$v_import_mode`, `$$v_user`
- **On Success:** go to T3
- **On Failure:** go to T5_CAPTURE_ERRORS

#### T3 — SQL Task: T3_RUN_COST
- **Connection:** `Oracle_GPC_DM`
- **SQL:** paste content of `sql/03_run_cost.sql`
- **Input parameters:** `$$v_portfolio_id`, `$$v_reporting_date`, `$$v_user`
- **On Success:** go to T4
- **On Failure:** go to T5_CAPTURE_ERRORS

#### T4 — SQL Task: T4_CAPTURE_RUN_LOG
- **Connection:** `Oracle_GPC_DM`
- **SQL:** paste content of `sql/04_capture_run_log.sql`
- **Output parameters** (one per column returned):

| Column | Variable | Type |
|---|---|---|
| `RUN_DATE` | `$$v_run_date` | String |
| `STAFFING_RUN_ID` | `$$v_staffing_run_id` | Integer |
| `COST_RUN_ID` | `$$v_cost_run_id` | Integer |
| `STAFFING_STATUS` | `$$v_staffing_status` | String |
| `COST_STATUS` | `$$v_cost_status` | String |
| `STAFFING_ROWS_INSERTED` | `$$v_staffing_rows_inserted` | Integer |
| `COST_ROWS_INSERTED` | `$$v_cost_rows_inserted` | Integer |

- **On Success / Failure:** both go to T5

#### T5 — SQL Task: T5_CAPTURE_ERRORS
- **Connection:** `Oracle_GPC_DM`
- **SQL:** paste content of `sql/05_capture_error_log.sql`
- **Output parameters:**

| Column | Variable | Type |
|---|---|---|
| `ERROR_COUNT` | `$$v_error_count` | Integer |
| `ERROR_SUMMARY` | `$$v_error_summary` | String |

- **On Success:** go to T6_DECISION
- **On Failure:** go to T8_EMAIL_FAILURE

#### T6 — Decision: T6_DECISION
- **Condition:** `$$v_error_count = 0`
- **True:** go to T7_EMAIL_SUCCESS
- **False:** go to T8_EMAIL_FAILURE

#### T7 — Email Notification: T7_EMAIL_SUCCESS
- **To:** `agron.daka@aralytiks.cm; dren.sahiti@aralytiks.cm; elion.rrahmani@aralytiks.cm`
- **Subject:** `[GPC_DM ETL] SUCCESS — $$v_portfolio_id / $$v_reporting_date`
- **Body:** copy from the `<BODY>` block in `taskflows/TF_GPC_DM_ETL_DAILY_RUN.xml`
- **On Success:** End (Success)

#### T8 — Email Notification: T8_EMAIL_FAILURE
- **To:** `agron.daka@aralytiks.cm; dren.sahiti@aralytiks.cm; elion.rrahmani@aralytiks.cm`
- **Subject:** `[GPC_DM ETL] FAILURE — $$v_portfolio_id / $$v_reporting_date requires attention`
- **Body:** copy from the `<BODY>` block in `taskflows/TF_GPC_DM_ETL_DAILY_RUN.xml`
- **On Success:** End (Failed)

### Option B — Import XML
1. Zip the `taskflows/TF_GPC_DM_ETL_DAILY_RUN.xml` file.
2. IICS Designer → Import → upload the ZIP.
3. Update all `{placeholder}` values in the imported taskflow.
4. Re-map the connection to `Oracle_GPC_DM`.
5. Save and validate.

---

## Step 5 — Configure Taskflow Variables

In the taskflow **Parameters/Variables** panel, declare these variables:

| Variable | Type | Direction | Default |
|---|---|---|---|
| `$$v_portfolio_id` | String | Input | `` |
| `$$v_reporting_date` | String | Input | `` |
| `$$v_execution_center` | String | Input | `` |
| `$$v_import_mode` | String | Input | `REPLACE_ALL` |
| `$$v_user` | String | Input | `IICS` |
| `$$v_blocked_count` | Integer | Local | `0` |
| `$$v_run_date` | String | Local | `` |
| `$$v_staffing_run_id` | Integer | Local | `0` |
| `$$v_cost_run_id` | Integer | Local | `0` |
| `$$v_staffing_status` | String | Local | `UNKNOWN` |
| `$$v_cost_status` | String | Local | `UNKNOWN` |
| `$$v_staffing_rows_inserted` | Integer | Local | `0` |
| `$$v_cost_rows_inserted` | Integer | Local | `0` |
| `$$v_error_count` | Integer | Local | `0` |
| `$$v_error_summary` | String | Local | `` |

---

## Step 6 — Schedule / Trigger the Taskflow

The taskflow is designed to be triggered **per portfolio per reporting date**, either:

- **On-demand** — triggered by the application or data team when a new file upload is ready
- **Scheduled** — run via IICS Scheduler after the source table refresh window:

**IICS Designer → Taskflow → Schedule tab**

| Setting | Value |
|---|---|
| Frequency | Daily (or as needed) |
| Start time | 02:00 UTC (after source table refresh) |
| Time zone | UTC |
| Enabled | Yes |

When scheduling, configure default values for the input parameters (or trigger via API with parameter overrides per run).

---

## Step 7 — Test Run

1. In IICS Designer, open `TF_GPC_DM_ETL_DAILY_RUN`.
2. Click **Run Now** → provide input parameter values.
3. Monitor in **Monitor → My Jobs**.
4. After completion, verify in Oracle:

```sql
-- Check run result
SELECT * FROM GPC_DM.V_IICS_RUN_SUMMARY ORDER BY RUN_ID DESC;

-- Check for errors
SELECT * FROM GPC_DM.V_IICS_ERROR_SUMMARY ORDER BY ERROR_TIME DESC;

-- Confirm valid rows loaded
SELECT 'DIM_STAFFING_SCHEDULE' AS tbl,
       COUNT(*) AS total, SUM(CASE WHEN IS_VALID=1 THEN 1 END) AS valid
FROM   GPC_DM.DIM_STAFFING_SCHEDULE
UNION ALL
SELECT 'DIM_STAFFING_TIMELINE', COUNT(*), NULL FROM GPC_DM.DIM_STAFFING_TIMELINE
UNION ALL
SELECT 'DIM_COST', COUNT(*), SUM(CASE WHEN IS_VALID=1 THEN 1 END) FROM GPC_DM.DIM_COST;
```

---

## Error Handling Summary

| Scenario | What happens in IICS | What happens in Oracle |
|---|---|---|
| Source table not accessible (T1 fails) | Routes to T8 failure email | Nothing called |
| Oracle procedure raises exception | IICS catches ORA- error, marks T2 or T3 FAILED, routes to T5 then T8 | Run logged as FAILED in ETL_RUN_LOG; error written to ETL_ERROR_LOG; ROLLBACK issued |
| GCS merge step fails | Same as above — package rolls back and raises | STG_GLOBAL_CODING_STRUCTURE left in pre-merge state |
| No matching source rows for portfolio/date | Procedure completes normally; ROWS_INSERTED = 0 | Target rows for that scope invalidated; zero new rows inserted |
| All clean | Routes to T7 success email | IS_VALID rows updated; new rows inserted in target tables |

---

## Monitoring After Deployment

| Where | What to check |
|---|---|
| IICS Monitor → My Jobs | Job status, duration, task-level success/failure |
| IICS Monitor → My Jobs → task detail | IICS-level error messages (connection failures, timeout) |
| `GPC_DM.V_IICS_RUN_SUMMARY` | Oracle-side run status, row counts per entity |
| `GPC_DM.V_IICS_ERROR_SUMMARY` | Validation and load errors with full messages |
| `GPC_DM.ETL_STEP_LOG` | Step-by-step timing (GCS, INVALIDATE, INSERT) |
| Email inbox | Success/failure notification with embedded diagnostic queries |
