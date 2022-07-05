USE analysis;

-- $BEGIN
SELECT encounter_id,
       encounter_type,
       encounter_datetime,
       visit_id
INTO mamba_dim_encounter
FROM encounter;
-- $END
