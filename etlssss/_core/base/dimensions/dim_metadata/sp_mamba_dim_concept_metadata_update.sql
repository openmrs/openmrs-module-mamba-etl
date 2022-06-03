USE analysis;

-- $BEGIN
-- Update the Concept datatypes
UPDATE mamba_dim_concept_metadata md
    INNER JOIN mamba_dim_concept c
    ON md.concept_uuid = c.uuid
SET md.concept_datatype = c.datatype
WHERE md.concept_metadata_id > 0;

-- Update to True if this field is an obs answer to an obs Question
UPDATE mamba_dim_concept_metadata md
    INNER JOIN mamba_dim_concept c
    ON md.concept_uuid = c.uuid
    INNER JOIN mamba_dim_concept_answer ca
    ON ca.answer_concept = c.external_concept_id
SET md.concept_answer_obs = 1
WHERE md.concept_metadata_id > 0;
-- $END