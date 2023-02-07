USE analysis;

-- $BEGIN
INSERT INTO mamba_dim_encounter_type (encounter_type_id,
                                      name,
                                      uuid)
SELECT encounter_type_id,
       name,
       uuid
FROM encounter_type
WHERE retired = 0;
-- $END
