USE analysis;

DROP TABLE IF EXISTS  mamba_dim_concept;

-- $BEGIN
CREATE TABLE mamba_dim_concept (
    concept_id int NOT NULL AUTO_INCREMENT,
    uuid CHAR(38) NOT NULL,
    external_concept_id int,
    external_datatype_id int, -- make it a FK
    datatype NVARCHAR(255) NULL,
    PRIMARY KEY (concept_id)
);

create index mamba_dim_concept_external_concept_id_index
    on mamba_dim_concept (external_concept_id);

create index mamba_dim_concept_external_datatype_id_index
    on mamba_dim_concept (external_datatype_id);
-- $END
