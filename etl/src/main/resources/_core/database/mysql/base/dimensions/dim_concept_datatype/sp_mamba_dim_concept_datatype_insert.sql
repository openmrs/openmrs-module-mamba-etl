USE analysis;

TRUNCATE TABLE mamba_dim_concept_datatype;

-- $BEGIN

INSERT INTO mamba_dim_concept_datatype (
    external_datatype_id,
    datatype_name
)
SELECT
    dt.concept_datatype_id AS external_datatype_id,
    dt.name AS datatype_name
FROM
    openmrs_dev.concept_datatype dt
WHERE
    dt.retired = 0;

-- $END
