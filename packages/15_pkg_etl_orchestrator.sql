-- ============================================================
-- FILE: 15_pkg_etl_orchestrator.sql
-- DESC: PKG_ETL_ORCHESTRATOR — main entry point for ETL execution
--
-- run_entity(p_entity_name)
--   Full end-to-end run for a single named entity:
--     1. Resolve entity metadata + validate
--     2. Check entity is runnable
--     3. Open entity-level run log entry
--     4. Acquire run lock
--     5. Determine watermark window (from_wm → to_wm)
--     6. Dispatch to entity-specific transform
--     7. For each target mapping (ordered by LOAD_ORDER):
--        a. Open mapping-level run log
--        b. Validate staging
--        c. Load (SCD2 or INCREMENTAL)
--        d. Post-load SCD2 duplicate check
--        e. Commit
--     8. Advance watermark
--     9. Release lock
--
-- run_all()
--   Iterates over all active IDLE/FAILED entities and calls run_entity.
--   Failures in one entity are caught and logged; other entities continue.
--
-- dispatch_transform()
--   Internal CASE dispatch to entity-specific transform packages.
--   Add a WHEN branch here when onboarding a new entity.
-- SCHEMA: GPC_DM
-- ============================================================

CREATE OR REPLACE PACKAGE GPC_DM.PKG_ETL_ORCHESTRATOR AS

    PROCEDURE run_entity(p_entity_name IN VARCHAR2);
    PROCEDURE run_all;

END PKG_ETL_ORCHESTRATOR;
/


CREATE OR REPLACE PACKAGE BODY GPC_DM.PKG_ETL_ORCHESTRATOR AS

    ---------------------------------------------------------------------------
    -- Dispatch to the correct entity-specific transform package.
    -- TO ONBOARD A NEW ENTITY: add a WHEN branch here.
    ---------------------------------------------------------------------------
    PROCEDURE dispatch_transform(
        p_entity_name    IN VARCHAR2,
        p_run_id         IN NUMBER,
        p_entity_id      IN NUMBER,
        p_from_watermark IN DATE,
        p_to_watermark   IN DATE
    ) IS
    BEGIN
        CASE UPPER(p_entity_name)
            WHEN 'STAFFING_SCHEDULE' THEN
                GPC_DM.PKG_ETL_TRANSFORM_STAFFING.transform(
                    p_run_id, p_entity_id, p_from_watermark, p_to_watermark);

            WHEN 'COST' THEN
                GPC_DM.PKG_ETL_TRANSFORM_COST.transform(
                    p_run_id, p_entity_id, p_from_watermark, p_to_watermark);

            -- -------------------------------------------------------
            -- Template for new entities:
            -- WHEN 'MY_NEW_ENTITY' THEN
            --     GPC_DM.PKG_ETL_TRANSFORM_MY_NEW_ENTITY.transform(
            --         p_run_id, p_entity_id, p_from_watermark, p_to_watermark);
            -- -------------------------------------------------------

            ELSE
                RAISE_APPLICATION_ERROR(-20050,
                    'No transform package registered for entity: '
                    || p_entity_name
                    || '. Add a WHEN branch in PKG_ETL_ORCHESTRATOR.dispatch_transform.');
        END CASE;
    END dispatch_transform;


    ---------------------------------------------------------------------------
    -- Core run procedure for a single named entity
    ---------------------------------------------------------------------------
    PROCEDURE run_entity(p_entity_name IN VARCHAR2) IS
        v_entity         GPC_DM.PKG_ETL_METADATA.r_entity;
        v_mappings       GPC_DM.PKG_ETL_METADATA.t_mapping_tab;
        v_entity_run_id  NUMBER;
        v_mapping_run_id NUMBER;
        v_from_wm        DATE;
        v_to_wm          DATE;
        v_reject_cnt     NUMBER;
        v_dup_cnt        NUMBER;
        v_step_id        NUMBER;
    BEGIN
        -- 1. Resolve entity and validate metadata
        v_entity  := GPC_DM.PKG_ETL_METADATA.get_entity(p_entity_name);
        v_mappings:= GPC_DM.PKG_ETL_METADATA.get_mappings(v_entity.ENTITY_ID);
        GPC_DM.PKG_ETL_METADATA.validate_metadata(v_entity.ENTITY_ID);

        -- 2. Check entity is runnable (IDLE or FAILED)
        IF NOT GPC_DM.PKG_ETL_CONTROL.is_runnable(v_entity.ENTITY_ID) THEN
            -- Silently return; caller can inspect ETL_CONTROL.STATUS
            RETURN;
        END IF;

        -- 3. Open entity-level run log entry (no mapping_id at this level)
        v_entity_run_id := GPC_DM.PKG_ETL_LOGGER.start_run(
            v_entity.ENTITY_ID, NULL);

        -- 4. Acquire run lock (prevents concurrent execution of same entity)
        GPC_DM.PKG_ETL_CONTROL.lock_entity(v_entity.ENTITY_ID, v_entity_run_id);
        COMMIT;  -- lock update must commit before long-running work begins

        BEGIN  -- entity-level error boundary

            -- 5. Determine watermark window
            v_from_wm := GPC_DM.PKG_ETL_CONTROL.get_watermark(v_entity.ENTITY_ID);
            v_to_wm   := GPC_DM.PKG_ETL_CONTROL.get_max_source_wm(v_entity.ENTITY_ID);

            IF v_to_wm IS NULL OR v_to_wm <= v_from_wm THEN
                -- Nothing new to process
                GPC_DM.PKG_ETL_LOGGER.end_run(
                    v_entity_run_id, 'SUCCESS',
                    p_error_message =>
                        'No new data. Source max watermark = '
                        || TO_CHAR(v_to_wm, 'YYYY-MM-DD')
                        || ', current watermark = '
                        || TO_CHAR(v_from_wm, 'YYYY-MM-DD')
                );
                GPC_DM.PKG_ETL_CONTROL.release_entity(
                    v_entity.ENTITY_ID, v_entity_run_id, 'SUCCESS');
                RETURN;
            END IF;

            -- 6. Run entity-specific transform (populates all staging tables)
            v_step_id := GPC_DM.PKG_ETL_LOGGER.log_step(
                v_entity_run_id, 'TRANSFORM', 'RUNNING');

            dispatch_transform(
                p_entity_name    => p_entity_name,
                p_run_id         => v_entity_run_id,
                p_entity_id      => v_entity.ENTITY_ID,
                p_from_watermark => v_from_wm,
                p_to_watermark   => v_to_wm
            );

            GPC_DM.PKG_ETL_LOGGER.end_step(v_step_id, 'SUCCESS');

            -- 7. Process each target mapping in LOAD_ORDER sequence
            FOR i IN 1..v_mappings.COUNT LOOP

                v_mapping_run_id := GPC_DM.PKG_ETL_LOGGER.start_run(
                    v_entity.ENTITY_ID, v_mappings(i).MAPPING_ID);

                BEGIN  -- mapping-level error boundary

                    -- a. Validate staging data for this mapping
                    v_reject_cnt := GPC_DM.PKG_ETL_VALIDATOR.validate_staging(
                        v_mapping_run_id,
                        v_mappings(i).MAPPING_ID,
                        p_entity_name
                    );

                    -- b. Load based on declared LOAD_TYPE
                    CASE v_mappings(i).LOAD_TYPE

                        WHEN 'SCD2' THEN
                            GPC_DM.PKG_ETL_SCD2_LOADER.load_scd2(
                                v_mapping_run_id,
                                v_mappings(i).MAPPING_ID,
                                v_to_wm
                            );
                            -- c. Post-load duplicate integrity check
                            v_dup_cnt := GPC_DM.PKG_ETL_VALIDATOR.check_scd2_duplicates(
                                v_mapping_run_id,
                                v_mappings(i).MAPPING_ID
                            );
                            IF v_dup_cnt > 0 THEN
                                GPC_DM.PKG_ETL_LOGGER.log_error(
                                    v_mapping_run_id,
                                    p_entity_name,
                                    v_mappings(i).TARGET_TABLE,
                                    'SCD2_POST_LOAD_DUP',
                                    v_dup_cnt || ' duplicate IS_CURRENT=Y key(s) '
                                    || 'after load. Data quality alert.'
                                );
                            END IF;

                        WHEN 'SCD1' THEN
                            -- SCD1 is a simpler MERGE (upsert, no history).
                            -- Reuses load_incremental which performs a full MERGE.
                            GPC_DM.PKG_ETL_SCD2_LOADER.load_incremental(
                                v_mapping_run_id,
                                v_mappings(i).MAPPING_ID,
                                v_to_wm
                            );

                        WHEN 'INCREMENTAL' THEN
                            GPC_DM.PKG_ETL_SCD2_LOADER.load_incremental(
                                v_mapping_run_id,
                                v_mappings(i).MAPPING_ID,
                                v_to_wm
                            );

                        ELSE
                            RAISE_APPLICATION_ERROR(-20051,
                                'Unsupported LOAD_TYPE: '
                                || v_mappings(i).LOAD_TYPE
                                || ' for mapping_id=' || v_mappings(i).MAPPING_ID);
                    END CASE;

                    COMMIT;

                    GPC_DM.PKG_ETL_LOGGER.end_run(
                        v_mapping_run_id, 'SUCCESS',
                        p_rows_rejected => v_reject_cnt
                    );

                EXCEPTION
                    WHEN OTHERS THEN
                        ROLLBACK;
                        GPC_DM.PKG_ETL_LOGGER.log_error(
                            v_entity_run_id,
                            p_entity_name,
                            v_mappings(i).TARGET_TABLE,
                            'ETL-' || SQLCODE,
                            SUBSTR(SQLERRM, 1, 4000)
                        );
                        GPC_DM.PKG_ETL_LOGGER.end_run(
                            v_mapping_run_id, 'FAILED',
                            p_error_message => SUBSTR(SQLERRM, 1, 4000)
                        );
                        RAISE;  -- propagate to entity-level handler
                END;  -- mapping-level error boundary

            END LOOP;

            -- 8. All mappings succeeded — advance the watermark
            GPC_DM.PKG_ETL_CONTROL.advance_watermark(
                v_entity.ENTITY_ID, v_to_wm, v_entity_run_id);
            COMMIT;

            -- 9. Close entity run as SUCCESS and release lock
            GPC_DM.PKG_ETL_LOGGER.end_run(v_entity_run_id, 'SUCCESS');
            GPC_DM.PKG_ETL_CONTROL.release_entity(
                v_entity.ENTITY_ID, v_entity_run_id, 'SUCCESS');

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                GPC_DM.PKG_ETL_LOGGER.end_run(
                    v_entity_run_id, 'FAILED',
                    p_error_message => SUBSTR(SQLERRM, 1, 4000)
                );
                GPC_DM.PKG_ETL_CONTROL.release_entity(
                    v_entity.ENTITY_ID, v_entity_run_id, 'FAILED');
                RAISE;
        END;  -- entity-level error boundary

    END run_entity;


    ---------------------------------------------------------------------------
    -- Run all active, runnable entities in entity_id sequence.
    -- A failure in one entity is caught and logged; others continue.
    ---------------------------------------------------------------------------
    PROCEDURE run_all IS
    BEGIN
        FOR e IN (
            SELECT e.ENTITY_NAME
            FROM   GPC_DM.ETL_ENTITY    e
            JOIN   GPC_DM.ETL_CONTROL   c ON c.ENTITY_ID = e.ENTITY_ID
            WHERE  e.IS_ACTIVE = 'Y'
            AND    c.STATUS   IN ('IDLE', 'FAILED')
            ORDER BY e.ENTITY_ID
        ) LOOP
            BEGIN
                run_entity(e.ENTITY_NAME);
            EXCEPTION
                WHEN OTHERS THEN
                    -- Error already logged by run_entity; continue with next
                    NULL;
            END;
        END LOOP;
    END run_all;

END PKG_ETL_ORCHESTRATOR;
/
