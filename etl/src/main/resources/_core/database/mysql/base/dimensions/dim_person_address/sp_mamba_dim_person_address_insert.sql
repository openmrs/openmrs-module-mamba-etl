USE analysis;

TRUNCATE TABLE mamba_dim_person_address;

-- $BEGIN

INSERT INTO mamba_dim_person_address (
    external_person_address_id,
    external_person_id,
    city_village,
    county_district,
    address1,
    address2
)
SELECT
    pa.person_address_id AS external_person_address_id,
    pa.person_id AS external_person_id,
    pa.city_village AS city_village,
    pa.county_district AS county_district,
    pa.address1 AS address1,
    pa.address2 AS address2
FROM
    openmrs_dev.person_address pa;

-- $END