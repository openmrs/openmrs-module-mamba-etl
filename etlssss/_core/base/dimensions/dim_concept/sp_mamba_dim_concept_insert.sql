USE analysis;

-- $BEGIN
INSERT INTO mamba_dim_concept (concept_id,
                               datatype_id,
                               datatype,
                               uuid)
SELECT concept_id,
       datatype_id,
       datatype,
       uuid
FROM concept
WHERE retired = 0;
-- $END
