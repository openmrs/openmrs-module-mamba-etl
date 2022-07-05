USE analysis;
DROP TABLE IF EXISTS mamba_dim_concept_datatype;

-- $BEGIN
CREATE TABLE mamba_dim_concept_datatype
(
    mamba_concept_datatype_id INT           NOT NULL AUTO_INCREMENT,
    concept_datatype_id       INT           NOT NULL,
    name             NVARCHAR(255) NULL,
    PRIMARY KEY (mamba_concept_datatype_id)
);

CREATE INDEX index_concept_datatype_id
    ON mamba_dim_concept_datatype (concept_datatype_id);
-- $END
