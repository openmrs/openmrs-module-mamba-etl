USE analysis;

-- $BEGIN
UPDATE mamba_dim_encounter e
    INNER JOIN mamba_dim_encounter_type et
    ON e.encounter_type = et.encounter_type_id
SET e.encounter_type_uuid = et.uuid
WHERE e.encounter_id > 0;
-- $END
