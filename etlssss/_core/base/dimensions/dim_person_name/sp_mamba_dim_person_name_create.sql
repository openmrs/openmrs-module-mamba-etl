USE analysis;

DROP TABLE IF EXISTS mamba_dim_person_name;

-- $BEGIN
CREATE TABLE mamba_dim_person_name
(
    person_name_id          int           NOT NULL AUTO_INCREMENT,
    external_person_name_id int,
    external_person_id      int,
    given_name              NVARCHAR(255) NULL,
    PRIMARY KEY (person_name_id)
);

create index mamba_dim_person_name_external_person_id_index
    on mamba_dim_person_name (external_person_id);
-- $END
