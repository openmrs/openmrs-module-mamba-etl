-- sp_mamba_data_processing.sql
-- This SP is the overall SP that will be used to execute all other stored procedures

-- $BEGIN

CALL sp_mamba_data_processing();
CALL sp_derived_covid_data_processing();
CALL sp_derived_hts_data_processing();

-- $END






