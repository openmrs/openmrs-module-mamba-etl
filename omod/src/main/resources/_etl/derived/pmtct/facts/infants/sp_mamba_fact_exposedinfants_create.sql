-- $BEGIN
CREATE TABLE mamba_fact_pmtct_exposedinfants
(

    encounter_id                              INT          NOT NULL,
    infant_client_id                          INT          NOT NULL,
    encounter_datetime                        DATE         NOT NULL,
    mother_client_id                          INT          NULL,
    visit_type                                VARCHAR(100) NULL,
    arv_adherence                             VARCHAR(100) NULL,
    linked_to_art                             VARCHAR(100) NULL,
    infant_hiv_test                           VARCHAR(100) NULL,
    hiv_test_performed                        VARCHAR(100) NULL,
    result_of_hiv_test                        VARCHAR(100) NULL,
    viral_load_results                        VARCHAR(100) NULL,
    hiv_exposure_status                       VARCHAR(100) NULL,
    viral_load_test_done                      VARCHAR(100) NULL,
    art_initiation_status                     VARCHAR(100) NULL,
    arv_prophylaxis_status                    VARCHAR(100) NULL,
    ctx_prophylaxis_status                    VARCHAR(100) NULL,
    patient_outcome_status                    VARCHAR(100) NULL,
    cotrimoxazole_adherence                   VARCHAR(100) NULL,
    child_hiv_dna_pcr_test_result             VARCHAR(100) NULL,
    unique_antiretroviral_therapy_number      VARCHAR(100) NULL,
    confirmatory_test_performed_on_this_vist  VARCHAR(100) NULL,
    rapid_hiv_antibody_test_result_at_18_mths VARCHAR(100) NULL,

    PRIMARY KEY (encounter_id)
);
-- $END