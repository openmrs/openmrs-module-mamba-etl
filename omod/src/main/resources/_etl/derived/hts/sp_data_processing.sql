-- $BEGIN

-- Derived facts
CALL sp_dim_client_hiv_hts;
CALL sp_fact_encounter_hiv_hts;

-- SELECT 'Executing sp_derived_hts_fact_hts';
CALL sp_fact_hts;

-- $END