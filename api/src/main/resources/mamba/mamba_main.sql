-- Enable the event etl_scheduler
SET GLOBAL event_scheduler = ON;

-- Drop/Create the Event responsible for firing up the ETL process
DROP EVENT IF EXISTS _mamba_etl_scheduler_event;

-- Setup ETL configurations
CALL sp_mamba_etl_setup(?, ?, ?, ?, ?);
-- pass them from the runtime properties file

CREATE EVENT IF NOT EXISTS _mamba_etl_scheduler_event
    ON SCHEDULE EVERY ? SECOND
        STARTS CURRENT_TIMESTAMP
    DO CALL sp_mamba_etl_schedule();

-- Setup a trigger that trims record off _mamba_etl_schedule to just leave 20 latest records.
       -- to avoid the table growing too big
CREATE TRIGGER trim_log_table_after_insert
    AFTER INSERT ON _mamba_etl_schedule
    FOR EACH ROW
BEGIN
    DELETE FROM _mamba_etl_schedule
    WHERE id NOT IN (
        SELECT id FROM (
                           SELECT id
                           FROM _mamba_etl_schedule
                           ORDER BY id DESC
                               LIMIT 20
                       ) AS recent_records
    );
END;