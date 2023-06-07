-- $BEGIN
INSERT INTO mamba_dim_client_hts (client_id,
                                  date_of_birth,
                                  age_at_test,
                                  sex,
                                  county,
                                  sub_county,
                                  ward)
SELECT p.person_id                                        as client_id,
       birthdate                                          as date_of_birth,
       hts.date_test_conducted as date_of_test,
       DATEDIFF(hts.date_test_conducted, birthdate) / 365 as age_at_test,
       (CASE `p`.`gender`
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE '_'
           END)                                           as sex,
       pa.county_district                                 as county,
       pa.city_village                                    as sub_county,
       pa.address1                                        as ward
FROM mamba_dim_person p
         INNER JOIN mamba_dim_person_address pa ON p.person_id = pa.person_id
         INNER JOIN mamba_flat_encounter_hts hts
                    ON p.person_id = hts.client_id;
-- $END
