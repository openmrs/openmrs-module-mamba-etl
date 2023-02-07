USE analysis;

-- $BEGIN
INSERT INTO mamba_dim_person_address (person_address_id,
                                      person_id,
                                      city_village,
                                      county_district,
                                      address1,
                                      address2)
SELECT person_address_id,
       person_id,
       city_village,
       county_district,
       address1,
       address2
FROM person_address pa;
-- $END
