SET GLOBAL event_scheduler = ON;

CREATE EVENT IF NOT EXISTS events_mamba_etl
    ON SCHEDULE EVERY ? MINUTE
        STARTS CURRENT_TIMESTAMP
    DO
    BEGIN

        DECLARE etl_schedule_table_count INT DEFAULT 1;
        DECLARE incremental_mode TINYINT(1) DEFAULT 1;

        -- TODO: make etl table name dynamic
        SELECT COUNT(*)
        INTO etl_schedule_table_count
        FROM information_schema.tables
        WHERE table_schema = 'analysis_db'
          AND table_name = '_mamba_etl_schedule';

        SELECT DISTINCT(incremental_mode_switch)
        INTO incremental_mode
        FROM mamba_etl_user_settings;

        IF etl_schedule_table_count < 1 OR incremental_mode = 0 THEN
            CALL sp_mamba_etl_schedule_table_create();
            CALL sp_mamba_etl_schedule('sp_mamba_data_processing_flatten');
        ELSE
            CALL sp_mamba_etl_schedule('sp_mamba_data_processing_flatten_incremental');
        END IF;

    END;