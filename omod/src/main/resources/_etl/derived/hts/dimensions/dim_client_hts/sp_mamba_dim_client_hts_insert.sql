-- $BEGIN
INSERT INTO mamba_dim_client_hts
    (
        client_id,
        date_of_birth,
        age_at_test,
        sex,
        county,
        sub_county,
        ward
    )
    SELECT
        p.person_id AS client_id,
        birthdate AS date_of_birth,
        FLOOR(DATEDIFF(hts.date_test_conducted, birthdate) / 365) AS age_at_test,
        CASE `p`.`gender`
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE '_'
        END AS sex,
        pa.county_district AS county,
        pa.city_village AS sub_county,
        pa.address1 AS ward
    FROM
        mamba_dim_person p
    INNER JOIN
            mamba_flat_encounter_hts hts
                ON p.person_id = hts.client_id
    LEFT JOIN
            mamba_dim_person_address pa
                ON p.person_id = pa.person_id
;
-- $END
