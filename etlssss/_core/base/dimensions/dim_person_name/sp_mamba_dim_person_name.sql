USE analysis;

-- $BEGIN
CALL sp_mamba_dim_person_name_create();
CALL sp_mamba_dim_person_name_insert();
-- $END