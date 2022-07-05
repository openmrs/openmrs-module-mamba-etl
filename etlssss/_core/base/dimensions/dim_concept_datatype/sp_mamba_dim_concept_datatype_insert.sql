USE analysis;

-- $BEGIN
SELECT concept_datatype_id,
       name
INTO mamba_dim_concept_datatype
FROM concept_datatype dt
WHERE dt.retired = 0;
-- $END
