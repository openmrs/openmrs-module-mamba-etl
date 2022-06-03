USE analysis;

-- $BEGIN
UPDATE mamba_dim_encounter e
    INNER JOIN mamba_dim_encounter_type et
    ON e.external_encounter_type_id = et.external_encounter_type_id
SET e.encounter_type_uuid = et.encounter_type_uuid
WHERE e.encounter_id > 0;
-- $END
