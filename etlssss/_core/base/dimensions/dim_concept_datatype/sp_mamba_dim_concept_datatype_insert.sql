USE analysis;

-- $BEGIN
INSERT INTO mamba_dim_concept_datatype (concept_datatype_id,
                                        name)
SELECT concept_datatype_id,
       name
FROM concept_datatype
WHERE retired = 0;
-- $END
