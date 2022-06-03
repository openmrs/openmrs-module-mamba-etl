USE analysis;

-- $BEGIN
CALL sp_mamba_dim_encounter_create();
CALL sp_mamba_dim_encounter_insert();
-- $END
