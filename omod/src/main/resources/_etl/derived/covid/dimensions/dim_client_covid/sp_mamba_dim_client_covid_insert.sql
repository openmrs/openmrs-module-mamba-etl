-- $BEGIN
INSERT INTO dim_client_covid
    (
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
        FLOOR(DATEDIFF(CAST(cd.order_date AS DATE), CAST(date_of_birth as DATE)) / 365) AS ageattest,
        sex,
        county,
        sub_county,
        ward
    FROM
        mamba_dim_client c
    INNER JOIN
        mamba_flat_encounter_covid cd
            ON c.client_id = cd.client_id;
-- $END
