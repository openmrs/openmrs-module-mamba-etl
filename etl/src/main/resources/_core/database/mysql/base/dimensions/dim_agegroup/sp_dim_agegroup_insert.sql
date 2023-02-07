
USE analysis;
-- $BEGIN

-- Enter unknown dimension value (in case a person's date of birth is unknown)
CALL sp_load_agegroup();
-- $END