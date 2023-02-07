USE analysis;

TRUNCATE TABLE mamba_dim_concept_name;

-- $BEGIN

INSERT INTO mamba_dim_concept_name (
    external_concept_id,
    concept_name
)
SELECT
    cn.concept_id AS external_concept_id,
    cn.name AS concept_name
FROM
    openmrs_dev.concept_name cn
WHERE
    cn.locale = 'en'
    AND cn.locale_preferred = 1;

-- $END
