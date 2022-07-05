USE analysis;

-- $BEGIN
SELECT person_address_id,
       person_id,
       city_village,
       county_district,
       address1,
       address2
INTO mamba_dim_person_address
FROM person_address pa;
-- $END
