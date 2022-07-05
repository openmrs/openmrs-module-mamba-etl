USE analysis;
DROP TABLE IF EXISTS mamba_dim_concept_answer;

-- $BEGIN
CREATE TABLE mamba_dim_concept_answer
(
    mamba_concept_answer_id INT NOT NULL AUTO_INCREMENT,
    concept_answer_id       INT NOT NULL,
    concept_id              INT NOT NULL,
    answer_concept          INT,
    answer_drug             INT,
    PRIMARY KEY (mamba_concept_answer_id)
);

CREATE INDEX index_concept_answer_id
    ON mamba_dim_concept_answer (concept_answer_id);

CREATE INDEX index_concept_id
    ON mamba_dim_concept_answer (concept_id);
-- $END
