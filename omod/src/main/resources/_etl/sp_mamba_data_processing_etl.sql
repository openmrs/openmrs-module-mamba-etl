-- $BEGIN
-- add base folder SP here --

-- Flatten the tables first
CALL sp_mamba_data_processing_flatten();

-- Call the ETL process
CALL sp_mamba_data_processing_derived_hts();
CALL sp_mamba_data_processing_derived_pmtct();
-- CALL sp_mamba_data_processing_derived_covid();

-- $END