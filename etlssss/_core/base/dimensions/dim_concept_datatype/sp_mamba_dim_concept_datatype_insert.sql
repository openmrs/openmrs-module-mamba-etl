USE analysis;

-- $BEGIN
INSERT INTO mamba_dim_concept_datatype (concept_datatype_id, name)
SELECT dt.concept_datatype_id AS concept_datatype_id,
       dt.name                AS name
FROM concept_datatype dt
WHERE dt.retired = 0;
-- $END
