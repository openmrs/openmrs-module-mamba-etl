-- $BEGIN
-- add base folder SP here --

-- Flatten the tables first
CALL sp_mamba_data_processing_flatten();

-- Call the ETL process
CALL sp_mamba_data_processing_derived_pmtct();

-- Create the reporting API definition Tables - TODO: we need to find a way of automating this so we dont have to add them
CALL sp_mamba_total_pregnant_women_columns_query();
Call sp_mamba_hiv_exposed_infants_columns_query();
Call sp_mamba_total_deliveries_columns_query();

-- $END