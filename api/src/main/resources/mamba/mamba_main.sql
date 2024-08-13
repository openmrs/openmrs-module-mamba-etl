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