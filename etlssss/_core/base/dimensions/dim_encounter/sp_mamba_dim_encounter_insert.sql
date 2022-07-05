USE analysis;

-- $BEGIN
INSERT INTO mamba_dim_encounter (encounter_id,
                                 encounter_type,
                                 encounter_datetime,
                                 visit_id)
SELECT encounter_id,
       encounter_type,
       encounter_datetime,
       visit_id
FROM encounter;
-- $END
