USE analysis;

-- $BEGIN
UPDATE mamba_dim_concept c
    INNER JOIN mamba_dim_concept_datatype dt
    ON c.datatype_id = dt.concept_datatype_id
    INNER JOIN mamba_dim_concept_name cn
    ON c.concept_id = cn.concept_id
SET c.datatype = dt.name,
    c.name=cn.name
WHERE c.concept_id > 0;
-- $END
