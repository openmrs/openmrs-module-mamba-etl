USE analysis;


-- $BEGIN

CALL sp_mamba_dim_encounter_type_create();
CALL sp_mamba_dim_encounter_type_insert();

-- $END
