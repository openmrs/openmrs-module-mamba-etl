USE analysis;

-- $BEGIN

CALL sp_mamba_dim_person_create();
CALL sp_mamba_dim_person_insert();

-- $END
