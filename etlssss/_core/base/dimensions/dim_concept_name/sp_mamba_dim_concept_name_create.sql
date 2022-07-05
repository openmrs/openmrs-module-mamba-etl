USE analysis;
DROP TABLE IF EXISTS mamba_dim_concept_name;

-- $BEGIN
CREATE TABLE mamba_dim_concept_name
(
    mamba_concept_name_id INT           NOT NULL AUTO_INCREMENT,
    concept_name_id       INT           NOT NULL,
    concept_id            INT           NOT NULL,
    name                  NVARCHAR(255) NOT NULL,
    PRIMARY KEY (mamba_concept_name_id)
);

CREATE INDEX index_concept_name_id
    ON mamba_dim_concept_name (concept_name_id);

CREATE INDEX index_concept_id
    ON mamba_dim_concept_name (concept_id);
-- $END
