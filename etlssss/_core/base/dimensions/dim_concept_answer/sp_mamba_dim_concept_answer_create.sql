USE analysis;

DROP TABLE IF EXISTS mamba_dim_concept_answer;

-- $BEGIN
CREATE TABLE mamba_dim_concept_answer
(
    concept_answer_id INT NOT NULL AUTO_INCREMENT,
    concept_id        INT,
    answer_concept    INT,
    answer_drug       INT,
    PRIMARY KEY (concept_answer_id)
);
-- $END