USE analysis;

-- $BEGIN
INSERT INTO mamba_dim_concept_name (concept_name_id,
                                    concept_id,
                                    name)
SELECT concept_name_id,
       concept_id,
       name
FROM concept_name;
-- $END
