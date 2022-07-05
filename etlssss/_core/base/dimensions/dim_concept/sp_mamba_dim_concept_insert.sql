USE analysis;

-- $BEGIN
SELECT concept_id,
       datatype_id,
       datatype,
       uuid
INTO mamba_dim_concept
FROM concept
WHERE retired = 0;
-- $END
