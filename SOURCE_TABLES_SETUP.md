# Source Tables Setup — STAFFING_SCHEDULE & COST

Quick reference for setting up and triggering the two ETL loads.

---

## What This Covers

| Entity | Source Table | Target Tables |
|---|---|---|
| `STAFFING_SCHEDULE` | `KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB` | `DIM_STAFFING_SCHEDULE` + `DIM_STAFFING_TIMELINE` |
| `COST` | `KBR_IHUB.APAC_PCDM_COST_IHUB` | `DIM_COST` |

> **Note:** `DIM_TIMELINE_COST` is out of scope for this framework. Target tables are shared with other
> processes — the packages write only to columns that already exist. No structural changes are made
> to target tables.

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

> Skip if the GPC_DM schema is already installed. Run `verify_install.sql` to check.

From the `EDWH_Framework` directory, connect as GPC_DM and run:

```sql
@00_install_all.sql
```

Then seed the metadata:

```sql
@data/16_metadata_inserts.sql
```

Verify with:

```sql
@verify_install.sql
```

---

## Step 3 — Run a STAFFING Load (manual)

`PKG_STAFFING_LOAD.load` takes five parameters:

| Parameter | Type | Description |
|---|---|---|
| `p_portfolio_id` | VARCHAR2 | Portfolio identifier (e.g. `'GPC-001'`) |
| `p_reporting_date` | DATE | Reporting cut-off date |
| `p_execution_center` | VARCHAR2 | Execution centre code (first 2 chars used) |
| `p_import_mode` | VARCHAR2 | `REPLACE_ALL` — full reload for this scope; `ADD_UPDATE` — incremental |
| `p_user` | VARCHAR2 | Calling user / system identifier for audit |

Example — full reload for a portfolio:

```sql
BEGIN
    GPC_DM.PKG_STAFFING_LOAD.load(
        p_portfolio_id     => 'GPC-001',
        p_reporting_date   => DATE '2025-03-31',
        p_execution_center => 'AU',
        p_import_mode      => 'REPLACE_ALL',
        p_user             => 'MANUAL_LOAD'
    );
    COMMIT;
END;
/
```

Example — incremental update:

```sql
BEGIN
    GPC_DM.PKG_STAFFING_LOAD.load(
        p_portfolio_id     => 'GPC-001',
        p_reporting_date   => DATE '2025-03-31',
        p_execution_center => 'AU',
        p_import_mode      => 'ADD_UPDATE',
        p_user             => 'IICS_DAILY'
    );
    COMMIT;
END;
/
```

**What the package does internally:**

1. GCS pre-process — stages distinct GCS combinations with `STAFFING_FLAG=1`, calls `PRC_MERGE_GLOBAL_CODING_STRUCTURE`, obtains `GCS_KEY`
2. Invalidates `DIM_STAFFING_SCHEDULE` rows in scope (`IS_VALID = 0`)
3. Inserts current `DIM_STAFFING_SCHEDULE` rows with `IS_VALID = 1`
4. Invalidates `DIM_STAFFING_TIMELINE` rows in scope
5. Inserts period rows into `DIM_STAFFING_TIMELINE` (one row per calendar month, unpivoted via CONNECT BY)

---

## Step 4 — Run a COST Load (manual)

`PKG_COST_LOAD.load` takes three parameters:

| Parameter | Type | Description |
|---|---|---|
| `p_portfolio_id` | VARCHAR2 | Portfolio identifier |
| `p_reporting_date` | DATE | Reporting cut-off date |
| `p_user` | VARCHAR2 | Calling user / system identifier for audit |

Example:

```sql
BEGIN
    GPC_DM.PKG_COST_LOAD.load(
        p_portfolio_id   => 'GPC-001',
        p_reporting_date => DATE '2025-03-31',
        p_user           => 'MANUAL_LOAD'
    );
    COMMIT;
END;
/
```

**What the package does internally:**

1. GCS pre-process — stages distinct GCS combinations with `COST_FLAG=1`, calls `PRC_MERGE_GLOBAL_CODING_STRUCTURE`, obtains `GCS_KEY`
2. Invalidates all `DIM_COST` rows for this portfolio + reporting date (full load)
3. Inserts pivoted COST CLASS rows — 11 blocks via UNION ALL (5 COMPANY rows + 6 CLIENT rows per source record)

---

## Step 5 — Verify Results

```sql
-- Latest run log entries
SELECT ENTITY_NAME, STATUS, ROWS_INSERTED, ROWS_REJECTED, START_TIME, END_TIME
FROM   GPC_DM.V_ETL_RUN_SUMMARY
ORDER BY START_TIME DESC
FETCH FIRST 10 ROWS ONLY;

-- Active (valid) rows in target tables
SELECT 'DIM_STAFFING_SCHEDULE' AS tbl,
       COUNT(*)                  AS total_rows,
       SUM(CASE WHEN IS_VALID = 1 THEN 1 END) AS valid_rows
FROM   GPC_DM.DIM_STAFFING_SCHEDULE
UNION ALL
SELECT 'DIM_STAFFING_TIMELINE',
       COUNT(*), NULL
FROM   GPC_DM.DIM_STAFFING_TIMELINE
UNION ALL
SELECT 'DIM_COST',
       COUNT(*),
       SUM(CASE WHEN IS_VALID = 1 THEN 1 END)
FROM   GPC_DM.DIM_COST;

-- Any errors from today's runs?
SELECT *
FROM   GPC_DM.V_IICS_ERROR_SUMMARY
WHERE  TRUNC(ERROR_TIME) = TRUNC(SYSDATE)
ORDER BY ERROR_TIME DESC;
```

---

## Step 6 — IICS-Triggered Loads

When IICS orchestrates the loads, it calls the same packages with parameters passed in as taskflow variables.
See `IICS/IICS_SETUP_GUIDE.md` for full taskflow configuration.

Quick SQL validation after an IICS run:

```sql
-- Confirm today's IICS run completed
SELECT * FROM GPC_DM.V_IICS_RUN_SUMMARY ORDER BY RUN_ID DESC;

-- Check for errors
SELECT * FROM GPC_DM.V_IICS_ERROR_SUMMARY ORDER BY ERROR_TIME DESC;
```

---

## Troubleshooting

**Load raised an exception — check the error log:**
```sql
SELECT ENTITY_NAME, ERROR_CODE, ERROR_MESSAGE, ERROR_TIME
FROM   GPC_DM.ETL_ERROR_LOG
WHERE  TRUNC(ERROR_TIME) = TRUNC(SYSDATE)
ORDER BY ERROR_TIME DESC;
```

**No rows appearing in target after load:**
```sql
-- Check ETL_RUN_LOG for the run
SELECT r.*, l.ENTITY_NAME
FROM   GPC_DM.ETL_RUN_LOG r
JOIN   GPC_DM.ETL_ENTITY  l ON l.ENTITY_ID = r.ENTITY_ID
WHERE  TRUNC(r.START_TIME) = TRUNC(SYSDATE)
ORDER BY r.RUN_ID DESC;
```

**GCS key lookup returns zero rows:**
```sql
-- Verify GCS reference table has data
SELECT COUNT(*) FROM GPC_DM.REF_GLOBAL_CODING_STRUCTURE;
-- If 0, the GCS merge procedure did not complete. Check PRC_MERGE_GLOBAL_CODING_STRUCTURE.

-- Verify staging table was populated
SELECT COUNT(*) FROM GPC_DM.STG_GLOBAL_CODING_STRUCTURE;
```

**Check which rows are currently valid in a target table:**
```sql
SELECT COUNT(*) AS valid_rows, PORTFOLIO_ID
FROM   GPC_DM.DIM_STAFFING_SCHEDULE
WHERE  IS_VALID = 1
GROUP BY PORTFOLIO_ID
ORDER BY 2;
```

---

## Source Table Column Reference

### STAFFING — KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB

| Source Column | Used As | Notes |
|---|---|---|
| `PORTFOLIO_ID` | Filter key | Matches `p_portfolio_id` |
| `REPORTING_DATE` | Filter key | Matches `p_reporting_date` |
| `EMPLOYEE_ID` | Business key component | Composite SCD key |
| `POSITION_NUMBER` | Business key component | Composite SCD key |
| `EXECUTION_CENTER` | GCS dimension | First 2 chars used |
| `BILL_TYPE` | GCS dimension | First 2 chars used |
| `COST_TYPE` | Business column | First 5 chars used |
| `CBS` | Business column | First 7 chars used |
| `JOB_TITLE` | Business column | |
| `GRADE_LEVEL` | Business column | |
| `PROJECT_ROLE` | Business column | |
| `WBS_CODE` | Business column | |
| `HOURS_PER_WEEK` | Timeline source | Used for `PLAN_HOURS` and `FORECAST_HOURS` |
| `PLAN_START_DATE` | Timeline bounds | Period rows generated from this date |
| `PLAN_END_DATE` | Timeline bounds | |
| `FORECAST_START_DATE` | Timeline bounds | |
| `FORECAST_END_DATE` | Timeline bounds | |
| `SCHEDULE_TYPE` | Business column | Set to NULL (not mapped) |

**Period unpivot logic:** One row per calendar month is generated via CROSS JOIN with a CONNECT BY numbers generator (up to 360 months). `PLAN_HOURS = HOURS_PER_WEEK` if the generated month falls within the plan date range; `FORECAST_HOURS = HOURS_PER_WEEK` if within the forecast range.

### COST — KBR_IHUB.APAC_PCDM_COST_IHUB

| Source Column | Used As | Notes |
|---|---|---|
| `PORTFOLIO_ID` | Filter key | Matches `p_portfolio_id` |
| `REPORTING_DATE` | Filter key | Matches `p_reporting_date` |
| `EXECUTION_CENTER` | GCS dimension | Full value |
| `BILL_TYPE` | GCS dimension | Full value |
| `PROJECT_ID` | Business column | |
| `WBS_CODE` | Business column | |
| `COST_TYPE` | Business column | |
| `CBS` | Business column | |
| `FORECAST_TYPE` | `CURVE_TYPE` derivation | `ACTUAL` → ACTUAL; `FORECAST` → FORECAST; else → BUDGET |
| `OB_COMPANY` | Pivoted to row | COST_CLASS = `OB COMPANY` |
| `BUDGET_COMPANY` | Pivoted to row | COST_CLASS = `BUDGET` |
| `EAC_COMPANY` | Pivoted to row | COST_CLASS = `EAC COMPANY` |
| `FORECAST_COMPANY` | Pivoted to row | COST_CLASS = `FORECAST` |
| `EARNED_VALUE_COMPANY` | Pivoted to row | COST_CLASS = `EARNED` |
| `OB_CLIENT` | Pivoted to row | COST_CLASS = `OB CLIENT` |
| `BUDGET_CLIENT` | Pivoted to row | COST_CLASS = `BUDGET CLIENT` |
| `EAC_CLIENT` | Pivoted to row | COST_CLASS = `EAC CLIENT` |
| `FORECAST_CLIENT` | Pivoted to row | COST_CLASS = `FORECAST CLIENT` |
| `EARNED_VALUE_CLIENT` | Pivoted to row | COST_CLASS = `EARNED CLIENT` |
| `ACTUAL_CLIENT` | Pivoted to row | COST_CLASS = `ACTUAL CLIENT` |

**Pivot logic:** Each source row produces up to 11 `DIM_COST` rows — one per COST_CLASS value. `DATA_SOURCE = 'GENERIC LOAD'` is hardcoded on all rows.
