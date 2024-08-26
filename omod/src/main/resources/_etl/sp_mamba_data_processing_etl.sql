DELIMITER //

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_etl;

CREATE PROCEDURE sp_mamba_data_processing_etl(IN etl_incremental_mode INT)

BEGIN
    -- add base folder SP here if any --

    -- TODO: add incremental scripts for derived and remove this drop
    CALL sp_mamba_drop_all_derived_tables();

    -- Call the ETL process
    CALL sp_mamba_data_processing_derived_hts();
    CALL sp_mamba_data_processing_derived_pmtct();
-- CALL sp_mamba_data_processing_derived_covid();

END //

DELIMITER ;