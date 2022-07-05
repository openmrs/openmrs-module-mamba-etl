USE analysis;

DROP TABLE IF EXISTS mamba_dim_person;

-- $BEGIN
CREATE TABLE mamba_dim_person
(
    mamba_person_id INT NOT NULL AUTO_INCREMENT,
    person_id       INT NOT NULL,
    gender          NVARCHAR(50),
    birthdate       DATE,
    dead            TINYINT,
    death_date      DATETIME,
    PRIMARY KEY (mamba_person_id)
);

CREATE INDEX index_person_id
    ON mamba_dim_person (person_id);
-- $END
