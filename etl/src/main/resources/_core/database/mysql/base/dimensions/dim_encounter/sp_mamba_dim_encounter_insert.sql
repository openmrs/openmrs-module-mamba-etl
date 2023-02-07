USE analysis;

TRUNCATE TABLE mamba_dim_encounter;

-- $BEGIN

INSERT INTO mamba_dim_encounter (
     external_encounter_id,
     external_encounter_type_id
)
SELECT
    e.encounter_id AS external_encounter_id,
    e.encounter_type AS external_encounter_type_id
FROM
    openmrs_dev.encounter e;

-- $END
