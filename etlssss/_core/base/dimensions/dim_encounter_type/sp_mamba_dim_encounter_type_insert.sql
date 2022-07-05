USE analysis;

-- $BEGIN
SELECT encounter_type_id,
       name,
       uuid
INTO mamba_dim_encounter_type
FROM encounter_type
WHERE retired = 0;
-- $END
