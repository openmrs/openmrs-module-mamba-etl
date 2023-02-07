USE analysis;

DROP TABLE IF EXISTS mamba_dim_person;

-- $BEGIN

CREATE TABLE mamba_dim_person (
    person_id int NOT NULL AUTO_INCREMENT,
    external_person_id int,
    birthdate NVARCHAR(255) NULL,
    gender NVARCHAR(255) NULL,
    PRIMARY KEY (person_id)
);
create index mamba_dim_person_external_person_id_index
    on mamba_dim_person (external_person_id);

-- $END
