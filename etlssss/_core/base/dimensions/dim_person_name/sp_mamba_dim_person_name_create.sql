USE analysis;

DROP TABLE IF EXISTS mamba_dim_person_name;

-- $BEGIN
CREATE TABLE mamba_dim_person_name
(
    mamba_person_name_id INT          NOT NULL AUTO_INCREMENT,
    person_name_id       INT          NOT NULL,
    person_id            INT          NOT NULL,
    given_name           NVARCHAR(50) NULL,
    middle_name          NVARCHAR(50) NULL,
    PRIMARY KEY (mamba_person_name_id)
);

CREATE INDEX index_person_name_id
    ON mamba_dim_person_name (person_name_id);

CREATE INDEX index_person_id
    ON mamba_dim_person_name (person_id);
-- $END
