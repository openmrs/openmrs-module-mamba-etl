USE analysis;

-- $BEGIN
INSERT INTO mamba_dim_concept_answer (concept_answer_id,
                                      concept_id,
                                      answer_concept,
                                      answer_drug)
SELECT concept_answer_id,
       concept_id,
       answer_concept,
       answer_drug
FROM concept_answer;
-- $END
