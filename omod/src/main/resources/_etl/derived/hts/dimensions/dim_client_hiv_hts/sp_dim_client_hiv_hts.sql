USE analysis;

-- $BEGIN

CALL sp_dim_client_hiv_hts_create();
CALL sp_dim_client_hiv_hts_insert();
CALL sp_dim_client_hiv_hts_update();