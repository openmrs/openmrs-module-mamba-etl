USE analysis;

-- $BEGIN
SELECT person_name_id,
       person_id,
       given_name,
       middle_name
INTO mamba_dim_person_name
FROM person_name;
-- $END
