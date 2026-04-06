-- ============================================================
-- FILE: 09_pkg_etl_metadata.sql
-- DESC: PKG_ETL_METADATA — metadata registry access package
--       Provides typed record lookups, column list builders,
--       and metadata consistency validation.
-- SCHEMA: GPC_DM
-- ============================================================

CREATE OR REPLACE PACKAGE GPC_DM.PKG_ETL_METADATA AS

    -- --------------------------------------------------------
    -- Record types for typed metadata lookups
    -- --------------------------------------------------------
    TYPE r_entity IS RECORD (
        ENTITY_ID        NUMBER,
        ENTITY_NAME      VARCHAR2(100),
        SOURCE_TABLE     VARCHAR2(200),
        WATERMARK_COLUMN VARCHAR2(100),
        WATERMARK_TYPE   VARCHAR2(20)
    );

    TYPE r_mapping IS RECORD (
        MAPPING_ID         NUMBER,
        ENTITY_ID          NUMBER,
        TARGET_TABLE       VARCHAR2(200),
        STAGING_TABLE      VARCHAR2(200),
        LOAD_TYPE          VARCHAR2(20),
        SURROGATE_KEY_COL  VARCHAR2(100),
        SURROGATE_SEQ_NAME VARCHAR2(200),
        SCD2_CURRENT_COL   VARCHAR2(100),
        SCD2_EFF_START_COL VARCHAR2(100),
        SCD2_EFF_END_COL   VARCHAR2(100),
        SCD2_END_SENTINEL  DATE,
        HASH_COL           VARCHAR2(100),
        LOAD_ORDER         NUMBER
    );

    TYPE r_column IS RECORD (
        CM_ID           NUMBER,
        SOURCE_COLUMN   VARCHAR2(100),
        TARGET_COLUMN   VARCHAR2(100),
        IS_BUSINESS_KEY VARCHAR2(1),
        IS_TRACKED      VARCHAR2(1),
        COLUMN_ORDER    NUMBER
    );

    TYPE t_mapping_tab IS TABLE OF r_mapping INDEX BY PLS_INTEGER;
    TYPE t_column_tab  IS TABLE OF r_column  INDEX BY PLS_INTEGER;

    -- --------------------------------------------------------
    -- Fetch entity record by name. Raises -20001 if not found.
    -- --------------------------------------------------------
    FUNCTION get_entity(p_entity_name IN VARCHAR2) RETURN r_entity;

    -- --------------------------------------------------------
    -- Fetch all active mappings for an entity, ordered by LOAD_ORDER.
    -- Raises -20002 if no active mappings found.
    -- --------------------------------------------------------
    FUNCTION get_mappings(p_entity_id IN NUMBER) RETURN t_mapping_tab;

    -- --------------------------------------------------------
    -- Fetch all active column mappings for a target mapping,
    -- ordered by COLUMN_ORDER.
    -- --------------------------------------------------------
    FUNCTION get_columns(p_mapping_id IN NUMBER) RETURN t_column_tab;

    -- --------------------------------------------------------
    -- Column list builders — return comma-separated string.
    -- Used by PKG_ETL_SCD2_LOADER to build dynamic SQL.
    -- --------------------------------------------------------
    FUNCTION get_bk_cols         (p_mapping_id IN NUMBER) RETURN VARCHAR2;
    FUNCTION get_tracked_cols    (p_mapping_id IN NUMBER) RETURN VARCHAR2;
    FUNCTION get_all_payload_cols(p_mapping_id IN NUMBER) RETURN VARCHAR2;

    -- --------------------------------------------------------
    -- Validate metadata consistency for an entity.
    -- Raises an application error on any inconsistency found.
    -- --------------------------------------------------------
    PROCEDURE validate_metadata(p_entity_id IN NUMBER);

END PKG_ETL_METADATA;
/


CREATE OR REPLACE PACKAGE BODY GPC_DM.PKG_ETL_METADATA AS

    FUNCTION get_entity(p_entity_name IN VARCHAR2) RETURN r_entity IS
        v_rec  r_entity;
    BEGIN
        SELECT ENTITY_ID, ENTITY_NAME, SOURCE_TABLE,
               WATERMARK_COLUMN, WATERMARK_TYPE
        INTO   v_rec.ENTITY_ID,   v_rec.ENTITY_NAME,   v_rec.SOURCE_TABLE,
               v_rec.WATERMARK_COLUMN, v_rec.WATERMARK_TYPE
        FROM   GPC_DM.ETL_ENTITY
        WHERE  ENTITY_NAME = p_entity_name
        AND    IS_ACTIVE   = 'Y';
        RETURN v_rec;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20001,
                'Entity not found or inactive: ' || p_entity_name);
    END get_entity;


    FUNCTION get_mappings(p_entity_id IN NUMBER) RETURN t_mapping_tab IS
        v_tab  t_mapping_tab;
        v_idx  PLS_INTEGER := 0;
    BEGIN
        FOR rec IN (
            SELECT MAPPING_ID, ENTITY_ID, TARGET_TABLE, STAGING_TABLE,
                   LOAD_TYPE, SURROGATE_KEY_COL, SURROGATE_SEQ_NAME,
                   SCD2_CURRENT_COL, SCD2_EFF_START_COL, SCD2_EFF_END_COL,
                   SCD2_END_SENTINEL, HASH_COL, LOAD_ORDER
            FROM   GPC_DM.ETL_TARGET_MAPPING
            WHERE  ENTITY_ID = p_entity_id
            AND    IS_ACTIVE = 'Y'
            ORDER BY LOAD_ORDER
        ) LOOP
            v_idx := v_idx + 1;
            v_tab(v_idx).MAPPING_ID         := rec.MAPPING_ID;
            v_tab(v_idx).ENTITY_ID          := rec.ENTITY_ID;
            v_tab(v_idx).TARGET_TABLE       := rec.TARGET_TABLE;
            v_tab(v_idx).STAGING_TABLE      := rec.STAGING_TABLE;
            v_tab(v_idx).LOAD_TYPE          := rec.LOAD_TYPE;
            v_tab(v_idx).SURROGATE_KEY_COL  := rec.SURROGATE_KEY_COL;
            v_tab(v_idx).SURROGATE_SEQ_NAME := rec.SURROGATE_SEQ_NAME;
            v_tab(v_idx).SCD2_CURRENT_COL   := rec.SCD2_CURRENT_COL;
            v_tab(v_idx).SCD2_EFF_START_COL := rec.SCD2_EFF_START_COL;
            v_tab(v_idx).SCD2_EFF_END_COL   := rec.SCD2_EFF_END_COL;
            v_tab(v_idx).SCD2_END_SENTINEL  := rec.SCD2_END_SENTINEL;
            v_tab(v_idx).HASH_COL           := rec.HASH_COL;
            v_tab(v_idx).LOAD_ORDER         := rec.LOAD_ORDER;
        END LOOP;

        IF v_idx = 0 THEN
            RAISE_APPLICATION_ERROR(-20002,
                'No active mappings for entity_id: ' || p_entity_id);
        END IF;
        RETURN v_tab;
    END get_mappings;


    FUNCTION get_columns(p_mapping_id IN NUMBER) RETURN t_column_tab IS
        v_tab  t_column_tab;
        v_idx  PLS_INTEGER := 0;
    BEGIN
        FOR rec IN (
            SELECT CM_ID, SOURCE_COLUMN, TARGET_COLUMN,
                   IS_BUSINESS_KEY, IS_TRACKED, COLUMN_ORDER
            FROM   GPC_DM.ETL_COLUMN_MAPPING
            WHERE  MAPPING_ID = p_mapping_id
            AND    IS_ACTIVE  = 'Y'
            ORDER BY COLUMN_ORDER
        ) LOOP
            v_idx := v_idx + 1;
            v_tab(v_idx).CM_ID           := rec.CM_ID;
            v_tab(v_idx).SOURCE_COLUMN   := rec.SOURCE_COLUMN;
            v_tab(v_idx).TARGET_COLUMN   := rec.TARGET_COLUMN;
            v_tab(v_idx).IS_BUSINESS_KEY := rec.IS_BUSINESS_KEY;
            v_tab(v_idx).IS_TRACKED      := rec.IS_TRACKED;
            v_tab(v_idx).COLUMN_ORDER    := rec.COLUMN_ORDER;
        END LOOP;
        RETURN v_tab;
    END get_columns;


    -- Comma-separated list of BK source column names (for ON clause)
    FUNCTION get_bk_cols(p_mapping_id IN NUMBER) RETURN VARCHAR2 IS
        v_list  VARCHAR2(2000) := '';
    BEGIN
        FOR rec IN (
            SELECT SOURCE_COLUMN
            FROM   GPC_DM.ETL_COLUMN_MAPPING
            WHERE  MAPPING_ID      = p_mapping_id
            AND    IS_BUSINESS_KEY = 'Y'
            AND    IS_ACTIVE       = 'Y'
            ORDER BY COLUMN_ORDER
        ) LOOP
            IF v_list IS NOT NULL THEN v_list := v_list || ', '; END IF;
            v_list := v_list || rec.SOURCE_COLUMN;
        END LOOP;
        RETURN v_list;
    END get_bk_cols;


    -- Comma-separated list of tracked (non-BK) source column names
    FUNCTION get_tracked_cols(p_mapping_id IN NUMBER) RETURN VARCHAR2 IS
        v_list  VARCHAR2(2000) := '';
    BEGIN
        FOR rec IN (
            SELECT SOURCE_COLUMN
            FROM   GPC_DM.ETL_COLUMN_MAPPING
            WHERE  MAPPING_ID      = p_mapping_id
            AND    IS_TRACKED      = 'Y'
            AND    IS_BUSINESS_KEY = 'N'
            AND    IS_ACTIVE       = 'Y'
            ORDER BY COLUMN_ORDER
        ) LOOP
            IF v_list IS NOT NULL THEN v_list := v_list || ', '; END IF;
            v_list := v_list || rec.SOURCE_COLUMN;
        END LOOP;
        RETURN v_list;
    END get_tracked_cols;


    -- Comma-separated list of all payload column names (source side)
    FUNCTION get_all_payload_cols(p_mapping_id IN NUMBER) RETURN VARCHAR2 IS
        v_list  VARCHAR2(4000) := '';
    BEGIN
        FOR rec IN (
            SELECT SOURCE_COLUMN
            FROM   GPC_DM.ETL_COLUMN_MAPPING
            WHERE  MAPPING_ID = p_mapping_id
            AND    IS_ACTIVE  = 'Y'
            ORDER BY COLUMN_ORDER
        ) LOOP
            IF v_list IS NOT NULL THEN v_list := v_list || ', '; END IF;
            v_list := v_list || rec.SOURCE_COLUMN;
        END LOOP;
        RETURN v_list;
    END get_all_payload_cols;


    PROCEDURE validate_metadata(p_entity_id IN NUMBER) IS
        v_count  NUMBER;
    BEGIN
        -- Rule 1: Must have at least one active mapping
        SELECT COUNT(*) INTO v_count
        FROM   GPC_DM.ETL_TARGET_MAPPING
        WHERE  ENTITY_ID = p_entity_id AND IS_ACTIVE = 'Y';

        IF v_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20010,
                'No active mappings for entity_id: ' || p_entity_id);
        END IF;

        -- Rule 2: SCD1/SCD2 mappings must have surrogate key + sequence
        SELECT COUNT(*) INTO v_count
        FROM   GPC_DM.ETL_TARGET_MAPPING
        WHERE  ENTITY_ID = p_entity_id
        AND    IS_ACTIVE = 'Y'
        AND    LOAD_TYPE IN ('SCD1','SCD2')
        AND    (SURROGATE_KEY_COL IS NULL OR SURROGATE_SEQ_NAME IS NULL);

        IF v_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20011,
                'SCD1/SCD2 mappings are missing SURROGATE_KEY_COL or '
                || 'SURROGATE_SEQ_NAME for entity_id: ' || p_entity_id);
        END IF;

        -- Rule 3: Each mapping must have at least one business key column
        FOR m IN (
            SELECT MAPPING_ID, TARGET_TABLE
            FROM   GPC_DM.ETL_TARGET_MAPPING
            WHERE  ENTITY_ID = p_entity_id AND IS_ACTIVE = 'Y'
        ) LOOP
            SELECT COUNT(*) INTO v_count
            FROM   GPC_DM.ETL_COLUMN_MAPPING
            WHERE  MAPPING_ID      = m.MAPPING_ID
            AND    IS_BUSINESS_KEY = 'Y'
            AND    IS_ACTIVE       = 'Y';

            IF v_count = 0 THEN
                RAISE_APPLICATION_ERROR(-20012,
                    'No business key column defined for mapping to: '
                    || m.TARGET_TABLE);
            END IF;
        END LOOP;

        -- Rule 4: INCREMENTAL mappings must also have surrogate key defined
        SELECT COUNT(*) INTO v_count
        FROM   GPC_DM.ETL_TARGET_MAPPING
        WHERE  ENTITY_ID = p_entity_id
        AND    IS_ACTIVE = 'Y'
        AND    LOAD_TYPE = 'INCREMENTAL'
        AND    SURROGATE_KEY_COL IS NULL;

        IF v_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20013,
                'INCREMENTAL mappings are missing SURROGATE_KEY_COL '
                || 'for entity_id: ' || p_entity_id);
        END IF;

    END validate_metadata;

END PKG_ETL_METADATA;
/
