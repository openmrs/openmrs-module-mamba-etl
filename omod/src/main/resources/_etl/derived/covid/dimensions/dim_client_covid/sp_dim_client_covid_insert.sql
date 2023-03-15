-- $BEGIN

INSERT INTO dim_client_covid (
        client_id,
        date_of_birth,
        ageattest,
        sex,
        county,
        sub_county,
        ward
)
SELECT
    c.client_id,
    date_of_birth,
    DATEDIFF(CAST(cd.order_date as date ),CAST(date_of_birth as DATE ))/365 as ageattest,
    sex,
    county,
    sub_county,
    ward
FROM dim_client c
INNER JOIN flat_encounter_covid cd
    ON c.client_id = cd.client_id;

-- $END
