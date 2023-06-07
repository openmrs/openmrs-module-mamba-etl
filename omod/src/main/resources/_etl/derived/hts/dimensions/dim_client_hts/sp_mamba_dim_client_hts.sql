-- $BEGIN
CALL sp_mamba_dim_client_hts_create();
CALL sp_mamba_dim_client_hts_insert();
CALL sp_mamba_dim_client_hts_update();
-- $END