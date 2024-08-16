DELIMITER //

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_etl;

CREATE PROCEDURE sp_mamba_data_processing_etl(IN etl_incremental_mode INT)

BEGIN
    -- add base folder SP here if any --

    -- Needed for now till incremental is full implemented. We will just drop all tables and recreate them
    -- CALL sp_mamba_drop_all_billing_tables();
    -- Call the ETL process
    CALL sp_mamba_data_processing_derived_hts();
    CALL sp_mamba_data_processing_derived_pmtct();
-- CALL sp_mamba_data_processing_derived_covid();

END //

DELIMITER ;