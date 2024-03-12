-- $BEGIN
-- add base folder SP here --

-- Flatten OpenMRS Observational Data
CALL sp_mamba_data_processing_flatten();

-- Add the implementation-specific ETL processes here

-- $END