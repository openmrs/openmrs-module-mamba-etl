USE analysis;

DROP TABLE IF EXISTS  mamba_dim_encounter_type;

-- $BEGIN

CREATE TABLE mamba_dim_encounter_type (
    encounter_type_id int NOT NULL AUTO_INCREMENT,
    external_encounter_type_id int,
    encounter_type_uuid CHAR(38) NOT NULL,
    PRIMARY KEY (encounter_type_id)
);

-- $END
