-- Enable the event scheduler
SET GLOBAL event_scheduler = ON;

-- Create the event to call the stored procedure
DROP EVENT IF EXISTS _mamba_etl_scheduler_event;

CREATE EVENT IF NOT EXISTS _mamba_etl_scheduler_event
    ON SCHEDULE EVERY 1 MINUTE
        STARTS CURRENT_TIMESTAMP
    DO CALL sp_mamba_etl_scheduler_wrapper();
