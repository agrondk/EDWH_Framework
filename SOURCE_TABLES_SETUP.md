# Source Tables Setup — STAFFING_SCHEDULE & COST

Quick reference for setting up and running the two source table ETL loads.

---

## What This Covers

| Entity | Source Table | Target Tables |
|---|---|---|
| `STAFFING_SCHEDULE` | `KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB` | `DIM_STAFFING_SCHEDULE` (SCD2) + `DIM_STAFFING_TIMELINE` (Incremental) |
| `COST` | `KBR_IHUB.APAC_PCDM_COST_IHUB` | `DIM_COST` (SCD2) + `DIM_TIMELINE_COST` (Incremental) |

---

## Step 1 — Verify Source Access

Connect as `GPC_DM` and confirm both source tables are readable:

```sql
SELECT COUNT(*) FROM KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB;
SELECT COUNT(*) FROM KBR_IHUB.APAC_PCDM_COST_IHUB;
```

If either query fails, a DBA needs to grant access:

```sql
GRANT SELECT ON KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB TO GPC_DM;
GRANT SELECT ON KBR_IHUB.APAC_PCDM_COST_IHUB TO GPC_DM;
```

---

## Step 2 — Install the Framework

> Skip this step if the GPC_DM schema is already installed.

From the `EDWH_Framework` directory, connect as GPC_DM and run:

```sql
@00_install_all.sql
```

This creates all tables, sequences, packages, and seeds the metadata for both entities in one step.

---

## Step 3 — Confirm Both Entities Are Registered

```sql
SELECT e.ENTITY_NAME, e.SOURCE_TABLE, c.STATUS, c.LAST_WATERMARK
FROM   GPC_DM.ETL_ENTITY  e
JOIN   GPC_DM.ETL_CONTROL c ON c.ENTITY_ID = e.ENTITY_ID
WHERE  e.ENTITY_NAME IN ('STAFFING_SCHEDULE', 'COST');
```

Expected result:

| ENTITY_NAME | SOURCE_TABLE | STATUS | LAST_WATERMARK |
|---|---|---|---|
| STAFFING_SCHEDULE | KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB | IDLE | *(null — first run will load all data)* |
| COST | KBR_IHUB.APAC_PCDM_COST_IHUB | IDLE | *(null)* |

---

## Step 4 — Run the Initial Load

Run both entities together:

```sql
EXEC GPC_DM.PKG_ETL_ORCHESTRATOR.run_all;
```

Or run them individually:

```sql
EXEC GPC_DM.PKG_ETL_ORCHESTRATOR.run_entity('STAFFING_SCHEDULE');
EXEC GPC_DM.PKG_ETL_ORCHESTRATOR.run_entity('COST');
```

The first run has a `NULL` watermark — it loads the full history from the source tables.

---

## Step 5 — Verify Results

```sql
-- Run summary
SELECT ENTITY_NAME, STATUS, ROWS_READ, ROWS_INSERTED, ROWS_REJECTED, START_TIME, END_TIME
FROM   GPC_DM.V_ETL_RUN_SUMMARY
WHERE  ROWNUM <= 10;

-- Record counts in target tables
SELECT 'DIM_STAFFING_SCHEDULE' AS tbl, COUNT(*) AS total, SUM(CASE WHEN IS_CURRENT='Y' THEN 1 END) AS current_rows FROM GPC_DM.DIM_STAFFING_SCHEDULE
UNION ALL
SELECT 'DIM_STAFFING_TIMELINE',        COUNT(*), NULL                                                                 FROM GPC_DM.DIM_STAFFING_TIMELINE
UNION ALL
SELECT 'DIM_COST',                      COUNT(*), SUM(CASE WHEN IS_CURRENT='Y' THEN 1 END)                           FROM GPC_DM.DIM_COST
UNION ALL
SELECT 'DIM_TIMELINE_COST',             COUNT(*), NULL                                                                FROM GPC_DM.DIM_TIMELINE_COST;

-- Any rejected rows?
SELECT 'STAFFING' AS entity, COUNT(*) AS rejected FROM GPC_DM.STG_STAFFING_SCHEDULE WHERE STG_STATUS = 'REJECTED'
UNION ALL
SELECT 'STAFFING_TL',                             COUNT(*)             FROM GPC_DM.STG_STAFFING_TIMELINE  WHERE STG_STATUS = 'REJECTED'
UNION ALL
SELECT 'COST',                                    COUNT(*)             FROM GPC_DM.STG_COST               WHERE STG_STATUS = 'REJECTED'
UNION ALL
SELECT 'COST_TL',                                 COUNT(*)             FROM GPC_DM.STG_TIMELINE_COST      WHERE STG_STATUS = 'REJECTED';
```

---

## Step 6 — Schedule Daily Runs (Optional)

```sql
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'GPC_DM.ETL_DAILY_RUN',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN GPC_DM.PKG_ETL_ORCHESTRATOR.run_all; END;',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY;BYHOUR=2;BYMINUTE=0',
        enabled         => TRUE,
        comments        => 'Daily ETL load — STAFFING_SCHEDULE and COST'
    );
END;
/
```

Subsequent runs are incremental — only records where `REPORTING_DATE > LAST_WATERMARK` are processed.

---

## Troubleshooting

**Entity stuck in RUNNING state** (e.g. session was killed):
```sql
UPDATE GPC_DM.ETL_CONTROL SET STATUS = 'IDLE'
WHERE  ENTITY_ID = (SELECT ENTITY_ID FROM GPC_DM.ETL_ENTITY WHERE ENTITY_NAME = 'STAFFING_SCHEDULE');
COMMIT;
```

**Check errors from the last run:**
```sql
SELECT ENTITY_NAME, ERROR_CODE, ERROR_MESSAGE, ERROR_TIME
FROM   GPC_DM.ETL_ERROR_LOG
WHERE  ERROR_TIME >= SYSDATE - 1
ORDER BY ERROR_TIME DESC;
```

**Inspect rejected rows with reasons:**
```sql
SELECT SCHEDULE_ID, STG_REJECT_REASON
FROM   GPC_DM.STG_STAFFING_SCHEDULE
WHERE  STG_STATUS = 'REJECTED'
ORDER BY STG_ID DESC;

SELECT COST_ID, STG_REJECT_REASON
FROM   GPC_DM.STG_COST
WHERE  STG_STATUS = 'REJECTED'
ORDER BY STG_ID DESC;
```

**Force a full reload** (reset the watermark):
```sql
UPDATE GPC_DM.ETL_CONTROL SET LAST_WATERMARK = NULL
WHERE  ENTITY_ID IN (SELECT ENTITY_ID FROM GPC_DM.ETL_ENTITY WHERE ENTITY_NAME IN ('STAFFING_SCHEDULE','COST'));
COMMIT;
```

---

## Source Table Column Mapping Summary

### STAFFING_SCHEDULE

| Source Column | Target Column | Table | Role |
|---|---|---|---|
| `SCHEDULE_ID` | `SCHEDULE_ID` | DIM_STAFFING_SCHEDULE | Business key |
| `EMPLOYEE_ID` | `EMPLOYEE_ID` | DIM_STAFFING_SCHEDULE | Tracked |
| `POSITION_ID` | `POSITION_ID` | DIM_STAFFING_SCHEDULE | Tracked / Required |
| `PROJECT_ID` | `PROJECT_ID` | DIM_STAFFING_SCHEDULE | Tracked / Required |
| `SCHEDULE_TYPE` | `SCHEDULE_TYPE` | DIM_STAFFING_SCHEDULE | Tracked / Validated |
| `SCHEDULE_STATUS` | `SCHEDULE_STATUS` | DIM_STAFFING_SCHEDULE | Tracked / Validated |
| `SCHEDULE_START_DATE` | `SCHEDULE_START_DATE` | DIM_STAFFING_SCHEDULE | Tracked / Date order check |
| `SCHEDULE_END_DATE` | `SCHEDULE_END_DATE` | DIM_STAFFING_SCHEDULE | Tracked / Date order check |
| `SCHEDULE_ID` | `SCHEDULE_ID` | DIM_STAFFING_TIMELINE | Business key (composite) |
| `PERIOD_START_DATE` | `PERIOD_START_DATE` → `DT_PERIOD` | DIM_STAFFING_TIMELINE | Derived YYYYMM period |
| `ALLOCATED_HOURS` | `ALLOCATED_HOURS` | DIM_STAFFING_TIMELINE | Required / Non-negative |

### COST

| Source Column | Target Column | Table | Role |
|---|---|---|---|
| `COST_ID` | `COST_ID` | DIM_COST | Business key |
| `PROJECT_ID` | `PROJECT_ID` | DIM_COST | Tracked / Required |
| `COST_CENTER` | `COST_CENTER` | DIM_COST | Tracked |
| `COST_TYPE` | `COST_TYPE` | DIM_COST | Tracked / Required |
| `COST_CATEGORY` | `COST_CATEGORY` | DIM_COST | Tracked / Validated (COMPANY or CLIENT) |
| `CURRENCY` | `CURRENCY` | DIM_COST | Tracked |
| `BUDGET_VERSION` | `BUDGET_VERSION` | DIM_COST | Tracked |
| `COST_ID` | `COST_ID` | DIM_TIMELINE_COST | Business key (composite) |
| `PERIOD_DATE` | `PERIOD_DATE` → `DT_PERIOD` | DIM_TIMELINE_COST | Derived YYYYMM period |
| `AMOUNT` | `AMOUNT` | DIM_TIMELINE_COST | Required |
| `FORECAST_TYPE` | `FORECAST_TYPE` → `CURVE_TYPE` | DIM_TIMELINE_COST | Derived: ACTUAL/BUDGET/FORECAST |
