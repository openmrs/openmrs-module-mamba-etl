USE analysis;

-- $BEGIN
INSERT INTO mamba_dim_person_name (person_name_id,
                                   person_id,
                                   given_name,
                                   middle_name)
SELECT person_name_id,
       person_id,
       given_name,
       middle_name
FROM person_name;
-- $END
