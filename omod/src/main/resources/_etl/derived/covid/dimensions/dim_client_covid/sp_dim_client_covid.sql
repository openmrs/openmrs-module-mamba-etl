USE analysis;

-- $BEGIN

CALL sp_dim_client_covid_create();
CALL sp_dim_client_covid_insert();
CALL sp_dim_client_covid_update();