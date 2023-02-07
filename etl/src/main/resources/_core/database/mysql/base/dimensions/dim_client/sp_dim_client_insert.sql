USE analysis;

-- $BEGIN

INSERT INTO dim_client (
    client_id ,
    date_of_birth,
    age,
    sex,
    county,
    sub_county,
    ward
)
SELECT
       `psn`.`person_id`                                                                                              AS `client_id`,
       `psn`.`birthdate`                                                                                              AS `date_of_birth`,
       timestampdiff(YEAR, `psn`.`birthdate`, now())                                                                  AS `age`,
       (CASE `psn`.`gender`
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE '_'
        END)                                                                                                          AS `sex`,
       `pa`.`county_district`                                                                                         AS `county`,
       `pa`.`city_village`                                                                                            AS `sub_county`,
       `pa`.`address1`                                                                                                AS `ward`
from ((`mamba_dim_person` `psn`
left join `mamba_dim_person_name` `pn` on ((`psn`.`external_person_id` = `pn`.`external_person_id`)))
left join `mamba_dim_person_address` `pa` on ((`psn`.`external_person_id` = `pa`.`external_person_id`)));


-- $END