USE analysis;

-- $BEGIN

CALL sp_fact_encounter_covid_create();
CALL sp_fact_encounter_covid_insert();
CALL sp_fact_encounter_covid_update();