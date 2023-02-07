USE analysis;


-- $BEGIN

CALL sp_mamba_dim_concept_create();
CALL sp_mamba_dim_concept_insert();
CALL sp_mamba_dim_concept_update();

-- $END
