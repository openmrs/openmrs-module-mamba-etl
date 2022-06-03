USE analysis;

-- Enter unknown dimension value (in case a person's date of birth is unknown)
-- $BEGIN
CALL sp_load_agegroup();
-- $END