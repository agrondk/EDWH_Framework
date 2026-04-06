# IICS Setup Guide — GPC_DM ETL Daily Run

How to deploy and configure the IICS job that orchestrates the EDWH ETL pipeline for STAFFING_SCHEDULE and COST.

---

## Overview

IICS acts as the **scheduler and orchestrator** only. All data movement, transformation, validation, and loading happens inside Oracle via the GPC_DM PL/SQL packages. IICS calls two Oracle stored procedures and reads the Oracle log tables to confirm success or report errors.

```
IICS Taskflow
    │
    ├─ T1  Pre-check         → SELECT from ETL_CONTROL
    ├─ T2  Run Staffing      → BEGIN PKG_ETL_ORCHESTRATOR.run_entity('STAFFING_SCHEDULE'); END;
    ├─ T3  Run Cost          → BEGIN PKG_ETL_ORCHESTRATOR.run_entity('COST'); END;
    ├─ T4  Capture Run Log   → SELECT from V_IICS_RUN_SUMMARY
    ├─ T5  Capture Errors    → SELECT from V_IICS_ERROR_SUMMARY
    ├─ T6  Decision          → error_count = 0 ?
    ├─ T7  Email Success     → run summary to team
    └─ T8  Email Failure     → error detail + diagnostic queries to team
```

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
    ├── 01_pre_check.sql             ← T1: verify entities are runnable
    ├── 02_run_staffing_schedule.sql ← T2: call STAFFING_SCHEDULE procedure
    ├── 03_run_cost.sql              ← T3: call COST procedure
    ├── 04_capture_run_log.sql       ← T4: read V_IICS_RUN_SUMMARY
    └── 05_capture_error_log.sql     ← T5: read V_IICS_ERROR_SUMMARY
```

The Oracle views `V_IICS_RUN_SUMMARY` and `V_IICS_ERROR_SUMMARY` are defined in:
`ddl/07_views.sql` — deployed as part of `00_install_all.sql`.

---

## Prerequisites

| Item | Requirement |
|---|---|
| Oracle Framework | `00_install_all.sql` fully deployed in GPC_DM |
| IICS version | Cloud Data Integration (CDI) — any current version |
| Secure Agent | Installed in the same network zone as the Oracle DB |
| Oracle JDBC driver | `ojdbc8.jar` or `ojdbc11.jar` on the Secure Agent |
| Oracle privileges | GPC_DM user must have EXECUTE on all `PKG_ETL_*` packages and SELECT on all ETL log tables and IICS views |
| Email relay | SMTP configured in IICS Administrator for email notifications |

### Verify Oracle prerequisites

```sql
-- Run as GPC_DM
SELECT OBJECT_NAME, OBJECT_TYPE, STATUS
FROM   ALL_OBJECTS
WHERE  OWNER = 'GPC_DM'
AND    OBJECT_NAME IN ('PKG_ETL_ORCHESTRATOR','V_IICS_RUN_SUMMARY','V_IICS_ERROR_SUMMARY');
-- All 3 must show STATUS = VALID
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

Alternatively, import `connections/Oracle_GPC_DM_Connection.xml` via Administrator → Connections → Import (update the `{HOST}`, `{PORT}`, `{SERVICE_NAME}`, `{GPC_DM_PASSWORD}` placeholders first).

---

## Step 3 — Deploy the IICS Views in Oracle

If not already deployed (run this once):

```sql
-- Run as GPC_DM or DBA
-- These views are included in ddl/07_views.sql and in 00_install_all.sql.
-- If the framework is already installed, run only these two CREATE OR REPLACE VIEW statements.

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
- **On Success:** go to T3
- **On Failure:** go to T5_CAPTURE_ERRORS

#### T3 — SQL Task: T3_RUN_COST
- **Connection:** `Oracle_GPC_DM`
- **SQL:** paste content of `sql/03_run_cost.sql`
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
| `STAFFING_ROWS_REJECTED` | `$$v_staffing_rows_rejected` | Integer |
| `COST_ROWS_INSERTED` | `$$v_cost_rows_inserted` | Integer |
| `COST_ROWS_REJECTED` | `$$v_cost_rows_rejected` | Integer |

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
- **Subject:** `[GPC_DM ETL] SUCCESS — Daily Run $$v_run_date`
- **Body:** copy from the `<BODY>` block in `taskflows/TF_GPC_DM_ETL_DAILY_RUN.xml`
- **On Success:** End (Success)

#### T8 — Email Notification: T8_EMAIL_FAILURE
- **To:** `agron.daka@aralytiks.cm; dren.sahiti@aralytiks.cm; elion.rrahmani@aralytiks.cm`
- **Subject:** `[GPC_DM ETL] VALIDATION FAILURE — Daily Run $$v_run_date requires attention`
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

| Variable | Type | Default |
|---|---|---|
| `$$v_run_date` | String | `` |
| `$$v_staffing_run_id` | Integer | `0` |
| `$$v_cost_run_id` | Integer | `0` |
| `$$v_staffing_status` | String | `UNKNOWN` |
| `$$v_cost_status` | String | `UNKNOWN` |
| `$$v_staffing_rows_inserted` | Integer | `0` |
| `$$v_staffing_rows_rejected` | Integer | `0` |
| `$$v_cost_rows_inserted` | Integer | `0` |
| `$$v_cost_rows_rejected` | Integer | `0` |
| `$$v_error_count` | Integer | `0` |
| `$$v_error_summary` | String | `` |

---

## Step 6 — Schedule the Taskflow

**IICS Designer → Taskflow → Schedule tab**

| Setting | Value |
|---|---|
| Frequency | Daily |
| Start time | 02:00 UTC (adjust to run after source table refresh completes) |
| Time zone | UTC |
| Enabled | Yes |

---

## Step 7 — Test Run

1. In IICS Designer, open `TF_GPC_DM_ETL_DAILY_RUN`.
2. Click **Run Now** (ad-hoc execution).
3. Monitor the job in **Monitor → My Jobs**.
4. After completion, verify in Oracle:

```sql
-- Check run result
SELECT * FROM GPC_DM.V_IICS_RUN_SUMMARY ORDER BY RUN_ID DESC;

-- Check for errors
SELECT * FROM GPC_DM.V_IICS_ERROR_SUMMARY ORDER BY ERROR_TIME DESC;

-- Confirm target table row counts
SELECT 'DIM_STAFFING_SCHEDULE' AS tbl, COUNT(*) FROM GPC_DM.DIM_STAFFING_SCHEDULE WHERE IS_CURRENT = 'Y'
UNION ALL
SELECT 'DIM_STAFFING_TIMELINE',          COUNT(*) FROM GPC_DM.DIM_STAFFING_TIMELINE
UNION ALL
SELECT 'DIM_COST',                        COUNT(*) FROM GPC_DM.DIM_COST WHERE IS_CURRENT = 'Y'
UNION ALL
SELECT 'DIM_TIMELINE_COST',              COUNT(*) FROM GPC_DM.DIM_TIMELINE_COST;
```

---

## Error Handling Summary

| Scenario | What happens in IICS | What happens in Oracle |
|---|---|---|
| Entity in RUNNING/DISABLED state | T1 returns BLOCKED_COUNT > 0 → routes to T8 failure email | Nothing called |
| Oracle procedure raises exception (FAIL rule, metadata error) | IICS catches ORA- error, marks T2 or T3 as FAILED, routes to T5 then T8 | Run logged as FAILED in ETL_RUN_LOG; error written to ETL_ERROR_LOG |
| Rows rejected by validation (REJECT rules) | Procedure completes successfully; IICS marks T2/T3 as SUCCESS | Rejected rows stay in staging with STG_STATUS=REJECTED; count in ROWS_REJECTED |
| T5 reads errors > 0 after successful procedure call | Routes to T8 failure email even though procedures succeeded | Data is loaded; rejected rows are flagged |
| All clean | Routes to T7 success email | Watermark advanced; target tables updated |

---

## Monitoring After Deployment

| Where | What to check |
|---|---|
| IICS Monitor → My Jobs | Job status, duration, task-level success/failure |
| IICS Monitor → My Jobs → task detail | IICS-level error messages (connection failures, timeout) |
| `GPC_DM.V_IICS_RUN_SUMMARY` | Oracle-side run status, row counts per entity |
| `GPC_DM.V_IICS_ERROR_SUMMARY` | Validation and load errors with full messages |
| `GPC_DM.ETL_STEP_LOG` | Step-by-step timing (TRANSFORM, CLASSIFY, EXPIRE, INSERT, MERGE) |
| Email inbox | Success/failure notification with embedded diagnostic queries |
