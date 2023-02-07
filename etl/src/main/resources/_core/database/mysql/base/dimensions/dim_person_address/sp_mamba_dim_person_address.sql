USE analysis;

-- $BEGIN

CALL sp_mamba_dim_person_address_create();
CALL sp_mamba_dim_person_address_insert();

-- $END
