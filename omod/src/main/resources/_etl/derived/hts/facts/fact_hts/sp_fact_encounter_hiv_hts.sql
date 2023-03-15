-- $BEGIN

CALL sp_fact_encounter_hiv_hts_create();
CALL sp_fact_encounter_hiv_hts_insert();
CALL sp_fact_encounter_hiv_hts_update();

-- $END