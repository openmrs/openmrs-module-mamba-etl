USE analysis;

-- $BEGIN

CALL sp_dim_agegroup_create();
CALL sp_dim_agegroup_insert();
-- CALL sp_dim_agegroup_update();