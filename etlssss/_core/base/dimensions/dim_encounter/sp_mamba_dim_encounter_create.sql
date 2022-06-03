USE analysis;

DROP TABLE IF EXISTS mamba_dim_encounter;

-- $BEGIN
CREATE TABLE mamba_dim_encounter
(
    encounter_id               int      NOT NULL AUTO_INCREMENT,
    external_encounter_id      int,
    external_encounter_type_id int,
    encounter_type_uuid        CHAR(38) NULL,
    PRIMARY KEY (encounter_id)
);
-- $END
