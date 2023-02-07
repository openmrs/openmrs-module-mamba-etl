USE analysis;
DROP TABLE IF EXISTS mamba_dim_concept;

-- $BEGIN
CREATE TABLE mamba_dim_concept
(
    mamba_concept_id INT           NOT NULL AUTO_INCREMENT,
    concept_id       INT           NOT NULL,
    datatype_id      INT           NOT NULL, -- make it a FK
    datatype         NVARCHAR(255) NULL,
    name             NVARCHAR(255) NULL,
    uuid             CHAR(38)      NOT NULL,
    PRIMARY KEY (mamba_concept_id)
);

CREATE INDEX index_concept_id
    ON mamba_dim_concept (concept_id);

CREATE INDEX index_datatype_id
    ON mamba_dim_concept (datatype_id);

CREATE INDEX index_uuid
    ON mamba_dim_concept (uuid);
-- $END
