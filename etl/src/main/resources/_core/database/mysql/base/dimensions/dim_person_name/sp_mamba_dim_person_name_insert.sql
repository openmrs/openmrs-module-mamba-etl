USE analysis;

TRUNCATE TABLE mamba_dim_person_name;

-- $BEGIN

INSERT INTO mamba_dim_person_name (
    external_person_name_id,
    external_person_id,
    given_name
)
SELECT
    pn.person_name_id AS external_person_name_id,
    pn.person_id AS external_person_id,
    pn.given_name AS given_name
FROM
    openmrs_dev.person_name pn;

-- $END