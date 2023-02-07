USE analysis;

-- $BEGIN

UPDATE mamba_dim_concept c
    INNER JOIN mamba_dim_concept_datatype dt
        ON c.external_datatype_id = dt.external_datatype_id
SET c.datatype = dt.datatype_name
WHERE c.concept_id > 0;

-- $END
