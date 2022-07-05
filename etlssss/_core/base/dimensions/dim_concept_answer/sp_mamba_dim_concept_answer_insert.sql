USE analysis;

-- $BEGIN
SELECT concept_answer_id,
       concept_id,
       answer_concept,
       answer_drug
INTO mamba_dim_concept_answer
FROM [source_db].concept_answer;
-- $END
