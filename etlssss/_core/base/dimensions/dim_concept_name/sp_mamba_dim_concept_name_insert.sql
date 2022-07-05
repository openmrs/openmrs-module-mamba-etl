USE analysis;

-- $BEGIN
SELECT concept_name_id,
       concept_id,
       name
INTO mamba_dim_concept_name
FROM concept_name;
-- $END
