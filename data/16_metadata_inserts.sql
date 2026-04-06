-- ============================================================
-- FILE: 16_metadata_inserts.sql
-- DESC: Seed data — registers both entities (STAFFING_SCHEDULE
--       and COST) with all target mappings, column mappings,
--       validation rules, and control rows.
--
-- Run order:
--   1. ETL_SOURCE_SYSTEM
--   2. ETL_ENTITY
--   3. ETL_CONTROL  (one row per entity)
--   4. ETL_TARGET_MAPPING
--   5. ETL_COLUMN_MAPPING
--   6. ETL_VALIDATION_RULE
-- SCHEMA: GPC_DM
-- ============================================================

-- ================================================================
-- STEP 1 — SOURCE SYSTEM
-- ================================================================
INSERT INTO GPC_DM.ETL_SOURCE_SYSTEM (SS_NAME, SS_SCHEMA, SS_DESCRIPTION)
VALUES (
    'KBR_IHUB',
    'KBR_IHUB',
    'KBR Integration Hub — APAC PCDM source feeds'
);

COMMIT;


-- ================================================================
-- STEP 2 — ENTITIES
-- ================================================================
INSERT INTO GPC_DM.ETL_ENTITY (
    SS_ID, ENTITY_NAME, SOURCE_TABLE,
    WATERMARK_COLUMN, WATERMARK_TYPE, DESCRIPTION
)
SELECT
    SS_ID,
    'STAFFING_SCHEDULE',
    'KBR_IHUB.APAC_PCDM_STAFFING_SCHEDULE_IHUB',
    'REPORTING_DATE',
    'DATE',
    'APAC staffing schedule data — splits into dimension and timeline targets'
FROM   GPC_DM.ETL_SOURCE_SYSTEM
WHERE  SS_NAME = 'KBR_IHUB';

INSERT INTO GPC_DM.ETL_ENTITY (
    SS_ID, ENTITY_NAME, SOURCE_TABLE,
    WATERMARK_COLUMN, WATERMARK_TYPE, DESCRIPTION
)
SELECT
    SS_ID,
    'COST',
    'KBR_IHUB.APAC_PCDM_COST_IHUB',
    'REPORTING_DATE',
    'DATE',
    'APAC project cost data — splits into dimension and timeline targets'
FROM   GPC_DM.ETL_SOURCE_SYSTEM
WHERE  SS_NAME = 'KBR_IHUB';

COMMIT;


-- ================================================================
-- STEP 3 — CONTROL ROWS
-- One per entity, initialised to IDLE with NULL watermark
-- (NULL = first run, will process full history)
-- ================================================================
INSERT INTO GPC_DM.ETL_CONTROL (ENTITY_ID, STATUS)
SELECT ENTITY_ID, 'IDLE'
FROM   GPC_DM.ETL_ENTITY
WHERE  ENTITY_NAME IN ('STAFFING_SCHEDULE', 'COST');

COMMIT;


-- ================================================================
-- STEP 4 + 5 + 6 — STAFFING_SCHEDULE mappings, columns, rules
-- ================================================================
DECLARE
    v_eid    NUMBER;
    v_mid1   NUMBER;   -- DIM_STAFFING_SCHEDULE mapping
    v_mid2   NUMBER;   -- DIM_STAFFING_TIMELINE  mapping
BEGIN
    SELECT ENTITY_ID INTO v_eid
    FROM   GPC_DM.ETL_ENTITY
    WHERE  ENTITY_NAME = 'STAFFING_SCHEDULE';

    -- --------------------------------------------------------
    -- TARGET MAPPING 1: DIM_STAFFING_SCHEDULE (SCD2)
    -- --------------------------------------------------------
    INSERT INTO GPC_DM.ETL_TARGET_MAPPING (
        ENTITY_ID, TARGET_TABLE, STAGING_TABLE,
        LOAD_TYPE, SURROGATE_KEY_COL, SURROGATE_SEQ_NAME, LOAD_ORDER
    ) VALUES (
        v_eid,
        'GPC_DM.DIM_STAFFING_SCHEDULE',
        'GPC_DM.STG_STAFFING_SCHEDULE',
        'SCD2',
        'DIM_SS_ID',
        'GPC_DM.SEQ_DIM_SS',
        10
    ) RETURNING MAPPING_ID INTO v_mid1;

    -- Column mappings for DIM_STAFFING_SCHEDULE
    -- IS_BUSINESS_KEY=Y, IS_TRACKED=N (BKs are not hashed — they are immutable in SCD2)
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,  TARGET_COLUMN,  IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'SCHEDULE_ID',  'SCHEDULE_ID',  'Y', 'N', 10);

    -- IS_BUSINESS_KEY=N, IS_TRACKED=Y (included in RECORD_HASH)
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,        TARGET_COLUMN,        IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'EMPLOYEE_ID',         'EMPLOYEE_ID',         'N', 'Y', 20);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,        TARGET_COLUMN,        IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'POSITION_ID',         'POSITION_ID',         'N', 'Y', 30);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,        TARGET_COLUMN,        IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'PROJECT_ID',          'PROJECT_ID',          'N', 'Y', 40);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,        TARGET_COLUMN,        IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'SCHEDULE_TYPE',       'SCHEDULE_TYPE',       'N', 'Y', 50);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,        TARGET_COLUMN,        IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'SCHEDULE_STATUS',     'SCHEDULE_STATUS',     'N', 'Y', 60);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,        TARGET_COLUMN,        IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'SCHEDULE_START_DATE', 'SCHEDULE_START_DATE', 'N', 'Y', 70);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,        TARGET_COLUMN,        IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'SCHEDULE_END_DATE',   'SCHEDULE_END_DATE',   'N', 'Y', 80);

    -- --------------------------------------------------------
    -- Validation rules for DIM_STAFFING_SCHEDULE
    -- Ported from C# PaffExelValidator + TimelineExelValidator
    -- --------------------------------------------------------

    -- Required fields
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, ERROR_ACTION
    ) VALUES (v_mid1, 'PROJECT_ID_REQUIRED',  'NOT_NULL', 'PROJECT_ID',  'REJECT');

    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, ERROR_ACTION
    ) VALUES (v_mid1, 'POSITION_ID_REQUIRED', 'NOT_NULL', 'POSITION_ID', 'REJECT');

    -- SCHEDULE_TYPE: must be a recognised contract type.
    -- Source: PaffExelValidator.ContractType + TimelineExelValidator.ContractType.
    -- NULL is allowed (field is optional on the source).
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, DERIVED_SQL, ERROR_ACTION
    ) VALUES (
        v_mid1,
        'SCHEDULE_TYPE_CHECK',
        'CHECK',
        'SCHEDULE_TYPE',
        'SCHEDULE_TYPE IS NULL OR SCHEDULE_TYPE IN (''Exempt'',''Non-Exempt'',''Exempt Agency'',''Non-Exempt Agency'')',
        'REJECT'
    );

    -- SCHEDULE_STATUS: must be a recognised status value.
    -- Source: TimelineExelValidator.Status + PaffExelValidator.PafStatus.
    -- NULL is allowed (field is optional on the source).
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, DERIVED_SQL, ERROR_ACTION
    ) VALUES (
        v_mid1,
        'SCHEDULE_STATUS_CHECK',
        'CHECK',
        'SCHEDULE_STATUS',
        'SCHEDULE_STATUS IS NULL OR SCHEDULE_STATUS IN (''Open'',''Filled'',''Canceled'',''Approved'',''Rejected'',''Withdrawn'',''Pending'')',
        'REJECT'
    );

    -- Date order: SCHEDULE_START_DATE must not be later than SCHEDULE_END_DATE.
    -- Source: PaffExelValidator (StartDate <= EndDate) and
    --         TimelineExelValidator (PLAN START DATE <= PLAN END DATE).
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, DERIVED_SQL, ERROR_ACTION
    ) VALUES (
        v_mid1,
        'SCHEDULE_DATE_ORDER',
        'CHECK',
        'SCHEDULE_START_DATE',
        'SCHEDULE_START_DATE IS NULL OR SCHEDULE_END_DATE IS NULL OR SCHEDULE_START_DATE <= SCHEDULE_END_DATE',
        'REJECT'
    );

    -- --------------------------------------------------------
    -- TARGET MAPPING 2: DIM_STAFFING_TIMELINE (INCREMENTAL)
    -- --------------------------------------------------------
    INSERT INTO GPC_DM.ETL_TARGET_MAPPING (
        ENTITY_ID, TARGET_TABLE, STAGING_TABLE,
        LOAD_TYPE, SURROGATE_KEY_COL, SURROGATE_SEQ_NAME, LOAD_ORDER
    ) VALUES (
        v_eid,
        'GPC_DM.DIM_STAFFING_TIMELINE',
        'GPC_DM.STG_STAFFING_TIMELINE',
        'INCREMENTAL',
        'DIM_ST_ID',
        'GPC_DM.SEQ_DIM_ST',
        20
    ) RETURNING MAPPING_ID INTO v_mid2;

    -- Column mappings for DIM_STAFFING_TIMELINE
    -- Composite BK: SCHEDULE_ID + DT_PERIOD
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,      TARGET_COLUMN,      IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid2, 'SCHEDULE_ID',       'SCHEDULE_ID',       'Y', 'N', 10);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,      TARGET_COLUMN,      IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid2, 'DT_PERIOD',         'DT_PERIOD',         'Y', 'N', 20);

    -- Tracked measures
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,      TARGET_COLUMN,      IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid2, 'PERIOD_START_DATE', 'PERIOD_START_DATE', 'N', 'Y', 30);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,      TARGET_COLUMN,      IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid2, 'PERIOD_END_DATE',   'PERIOD_END_DATE',   'N', 'Y', 40);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,      TARGET_COLUMN,      IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid2, 'ALLOCATED_HOURS',   'ALLOCATED_HOURS',   'N', 'Y', 50);

    -- Validation rule: DT_PERIOD is mandatory (DERIVED from PERIOD_START_DATE)
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME,
        DERIVED_SQL, ERROR_ACTION
    ) VALUES (
        v_mid2,
        'DT_PERIOD_REQUIRED',
        'DERIVED',
        'DT_PERIOD',
        'TO_CHAR(PERIOD_START_DATE, ''YYYYMM'')',
        'REJECT'
    );

    -- --------------------------------------------------------
    -- Additional validation rules for DIM_STAFFING_TIMELINE
    -- Ported from C# TimelineExelValidator
    -- --------------------------------------------------------

    -- ALLOCATED_HOURS is required.
    -- Source: TimelineExelValidator (hours per week must be numeric/present).
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, ERROR_ACTION
    ) VALUES (v_mid2, 'ALLOCATED_HOURS_REQUIRED', 'NOT_NULL', 'ALLOCATED_HOURS', 'REJECT');

    -- ALLOCATED_HOURS must be non-negative.
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, DERIVED_SQL, ERROR_ACTION
    ) VALUES (
        v_mid2,
        'ALLOCATED_HOURS_NON_NEGATIVE',
        'CHECK',
        'ALLOCATED_HOURS',
        'ALLOCATED_HOURS IS NULL OR ALLOCATED_HOURS >= 0',
        'REJECT'
    );

    -- PERIOD_START_DATE must not be later than PERIOD_END_DATE.
    -- Source: TimelineExelValidator ValidateDateRange (PLAN / FORECAST start <= end).
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, DERIVED_SQL, ERROR_ACTION
    ) VALUES (
        v_mid2,
        'PERIOD_DATE_ORDER',
        'CHECK',
        'PERIOD_START_DATE',
        'PERIOD_START_DATE IS NULL OR PERIOD_END_DATE IS NULL OR PERIOD_START_DATE <= PERIOD_END_DATE',
        'REJECT'
    );

    COMMIT;
END;
/


-- ================================================================
-- STEP 4 + 5 + 6 — COST mappings, columns, rules
-- ================================================================
DECLARE
    v_eid    NUMBER;
    v_mid1   NUMBER;   -- DIM_COST mapping
    v_mid2   NUMBER;   -- DIM_TIMELINE_COST mapping
BEGIN
    SELECT ENTITY_ID INTO v_eid
    FROM   GPC_DM.ETL_ENTITY
    WHERE  ENTITY_NAME = 'COST';

    -- --------------------------------------------------------
    -- TARGET MAPPING 1: DIM_COST (SCD2)
    -- --------------------------------------------------------
    INSERT INTO GPC_DM.ETL_TARGET_MAPPING (
        ENTITY_ID, TARGET_TABLE, STAGING_TABLE,
        LOAD_TYPE, SURROGATE_KEY_COL, SURROGATE_SEQ_NAME, LOAD_ORDER
    ) VALUES (
        v_eid,
        'GPC_DM.DIM_COST',
        'GPC_DM.STG_COST',
        'SCD2',
        'DIM_COST_ID',
        'GPC_DM.SEQ_DIM_COST',
        10
    ) RETURNING MAPPING_ID INTO v_mid1;

    -- BK column
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,   TARGET_COLUMN,   IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'COST_ID',        'COST_ID',        'Y', 'N', 10);

    -- Tracked attributes
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,   TARGET_COLUMN,   IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'PROJECT_ID',     'PROJECT_ID',     'N', 'Y', 20);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,   TARGET_COLUMN,   IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'COST_CENTER',    'COST_CENTER',    'N', 'Y', 30);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,   TARGET_COLUMN,   IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'COST_TYPE',      'COST_TYPE',      'N', 'Y', 40);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,   TARGET_COLUMN,   IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'COST_CATEGORY',  'COST_CATEGORY',  'N', 'Y', 50);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,   TARGET_COLUMN,   IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'CURRENCY',       'CURRENCY',       'N', 'Y', 60);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,   TARGET_COLUMN,   IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid1, 'BUDGET_VERSION', 'BUDGET_VERSION', 'N', 'Y', 70);

    -- --------------------------------------------------------
    -- Validation rules for DIM_COST
    -- Ported from C# CostExcelValidator
    -- --------------------------------------------------------

    -- Required fields
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, ERROR_ACTION
    ) VALUES (v_mid1, 'PROJECT_ID_REQUIRED', 'NOT_NULL', 'PROJECT_ID', 'REJECT');

    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, ERROR_ACTION
    ) VALUES (v_mid1, 'COST_TYPE_REQUIRED',  'NOT_NULL', 'COST_TYPE',  'REJECT');

    -- COST_CATEGORY maps to COST_BASIS in the C# validator.
    -- Source: CostExcelValidator — COST_BASIS must be COMPANY or CLIENT.
    -- NULL is allowed (not all source rows carry this attribute).
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, DERIVED_SQL, ERROR_ACTION
    ) VALUES (
        v_mid1,
        'COST_CATEGORY_CHECK',
        'CHECK',
        'COST_CATEGORY',
        'COST_CATEGORY IS NULL OR UPPER(COST_CATEGORY) IN (''COMPANY'',''CLIENT'')',
        'REJECT'
    );

    -- --------------------------------------------------------
    -- TARGET MAPPING 2: DIM_TIMELINE_COST (INCREMENTAL)
    -- --------------------------------------------------------
    INSERT INTO GPC_DM.ETL_TARGET_MAPPING (
        ENTITY_ID, TARGET_TABLE, STAGING_TABLE,
        LOAD_TYPE, SURROGATE_KEY_COL, SURROGATE_SEQ_NAME, LOAD_ORDER
    ) VALUES (
        v_eid,
        'GPC_DM.DIM_TIMELINE_COST',
        'GPC_DM.STG_TIMELINE_COST',
        'INCREMENTAL',
        'DIM_TC_ID',
        'GPC_DM.SEQ_DIM_TC',
        20
    ) RETURNING MAPPING_ID INTO v_mid2;

    -- Composite BK: COST_ID + DT_PERIOD
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,  TARGET_COLUMN,  IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid2, 'COST_ID',       'COST_ID',       'Y', 'N', 10);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,  TARGET_COLUMN,  IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid2, 'DT_PERIOD',     'DT_PERIOD',     'Y', 'N', 20);

    -- Tracked measures / attributes
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,  TARGET_COLUMN,  IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid2, 'PERIOD_DATE',   'PERIOD_DATE',   'N', 'Y', 30);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,  TARGET_COLUMN,  IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid2, 'AMOUNT',        'AMOUNT',        'N', 'Y', 40);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,  TARGET_COLUMN,  IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid2, 'CURVE_TYPE',    'CURVE_TYPE',    'N', 'Y', 50);
    INSERT INTO GPC_DM.ETL_COLUMN_MAPPING
        (MAPPING_ID, SOURCE_COLUMN,  TARGET_COLUMN,  IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER)
    VALUES (v_mid2, 'FORECAST_TYPE', 'FORECAST_TYPE', 'N', 'Y', 60);

    -- Validation rule: CURVE_TYPE is mandatory (DERIVED)
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME,
        DERIVED_SQL, ERROR_ACTION
    ) VALUES (
        v_mid2,
        'CURVE_TYPE_REQUIRED',
        'DERIVED',
        'CURVE_TYPE',
        'CASE'
        || ' WHEN UPPER(FORECAST_TYPE) IN (''ACTUALS'',''ACTUAL'') THEN ''ACTUAL'''
        || ' WHEN BUDGET_VERSION LIKE ''BUD%''                     THEN ''BUDGET'''
        || ' WHEN UPPER(FORECAST_TYPE) IN (''FC'',''FORECAST'')    THEN ''FORECAST'''
        || ' ELSE NULL END',
        'REJECT'
    );

    -- Validation rule: DT_PERIOD is mandatory (DERIVED from PERIOD_DATE)
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME,
        DERIVED_SQL, ERROR_ACTION
    ) VALUES (
        v_mid2,
        'DT_PERIOD_REQUIRED',
        'DERIVED',
        'DT_PERIOD',
        'TO_CHAR(PERIOD_DATE, ''YYYYMM'')',
        'REJECT'
    );

    -- --------------------------------------------------------
    -- Additional validation rules for DIM_TIMELINE_COST
    -- Ported from C# CostExcelValidator
    -- --------------------------------------------------------

    -- AMOUNT is required.
    -- Source: CostExcelValidator — cost fields must be numeric / present.
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, ERROR_ACTION
    ) VALUES (v_mid2, 'AMOUNT_REQUIRED',        'NOT_NULL', 'AMOUNT',        'REJECT');

    -- FORECAST_TYPE is required (it drives CURVE_TYPE derivation).
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, ERROR_ACTION
    ) VALUES (v_mid2, 'FORECAST_TYPE_REQUIRED', 'NOT_NULL', 'FORECAST_TYPE', 'REJECT');

    -- CURVE_TYPE value check — runs AFTER the DERIVED rule above.
    -- After derivation the only valid classifications are ACTUAL / BUDGET / FORECAST.
    -- This rule catches rows where the source CURVE_TYPE column contained an
    -- unrecognised value that was passed through as-is (UPPER fallback) but does
    -- not match any of the three expected classifications.
    -- Source: CostExcelValidator — CURVE_TYPE must be a recognised value.
    INSERT INTO GPC_DM.ETL_VALIDATION_RULE (
        MAPPING_ID, RULE_NAME, RULE_TYPE, COLUMN_NAME, DERIVED_SQL, ERROR_ACTION
    ) VALUES (
        v_mid2,
        'CURVE_TYPE_VALUE_CHECK',
        'CHECK',
        'CURVE_TYPE',
        'CURVE_TYPE IS NULL OR CURVE_TYPE IN (''ACTUAL'',''BUDGET'',''FORECAST'')',
        'REJECT'
    );

    COMMIT;
END;
/

-- ================================================================
-- Verify registration
-- ================================================================
SELECT e.ENTITY_NAME, t.TARGET_TABLE, t.LOAD_TYPE, t.LOAD_ORDER,
       c.STATUS AS CTRL_STATUS, c.LAST_WATERMARK
FROM   GPC_DM.ETL_ENTITY          e
JOIN   GPC_DM.ETL_TARGET_MAPPING   t ON t.ENTITY_ID = e.ENTITY_ID
JOIN   GPC_DM.ETL_CONTROL          c ON c.ENTITY_ID = e.ENTITY_ID
ORDER BY e.ENTITY_NAME, t.LOAD_ORDER;
