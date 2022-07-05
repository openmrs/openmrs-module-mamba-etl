USE analysis;

-- $BEGIN
SELECT person_id,
       gender,
       birthdate,
       dead,
       death_date
INTO mamba_dim_person
FROM person psn;
-- $END
