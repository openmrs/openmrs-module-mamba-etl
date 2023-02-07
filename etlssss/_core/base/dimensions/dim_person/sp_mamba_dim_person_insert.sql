USE analysis;

-- $BEGIN
INSERT INTO mamba_dim_person (person_id,
                              gender,
                              birthdate,
                              dead,
                              death_date)
SELECT person_id,
       gender,
       birthdate,
       dead,
       death_date
FROM person psn;
-- $END
