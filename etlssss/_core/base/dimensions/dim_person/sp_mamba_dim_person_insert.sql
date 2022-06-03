USE analysis;

-- $BEGIN
INSERT INTO mamba_dim_person (external_person_id,
                              birthdate,
                              gender)
SELECT psn.person_id AS external_person_id,
       psn.birthdate AS birthdate,
       psn.gender    AS gender
FROM openmrs_dev.person psn;
-- $END