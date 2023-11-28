-- $BEGIN
create table mamba_fact_pmtct_pregnant_women
(
    encounter_id                         INT          NOT NULL,
    client_id                            INT          NOT NULL,
    encounter_datetime                   datetime     NOT NULL,
    parity                               VARCHAR(100) NULL,
    gravida                              VARCHAR(100) NULL,
    missing                              VARCHAR(100) NULL,
    visit_type                           VARCHAR(100) NULL,
    ptracker_id                          VARCHAR(100) NULL,
    new_anc_visit                        VARCHAR(100) NULL,
    hiv_test_result                      VARCHAR(100) NULL,
    return_anc_visit                     VARCHAR(100) NULL,
    return_visit_date                    VARCHAR(100) NULL,
    hiv_test_performed                   VARCHAR(100) NULL,
    partner_hiv_tested                   VARCHAR(100) NULL,
    hiv_test_result_negative             VARCHAR(100) NULL,
    hiv_test_result_positive             VARCHAR(100) NULL,
    previously_known_positive            VARCHAR(100) NULL,
    estimated_date_of_delivery           VARCHAR(100) NULL,
    facility_of_next_appointment         VARCHAR(100) NULL,
    date_of_last_menstrual_period        VARCHAR(100) NULL,
    hiv_test_result_indeterminate        VARCHAR(100) NULL,
    tested_for_hiv_during_this_visit     VARCHAR(100) NULL,
    not_tested_for_hiv_during_this_visit VARCHAR(100) NULL
);

CREATE INDEX mamba_fact_pmtct_pregnant_women_client_id_index
    ON mamba_fact_pmtct_pregnant_women (client_id);

CREATE INDEX mamba_fact_pmtct_pregnant_women_encounter_datetime_index
    ON mamba_fact_pmtct_pregnant_women (encounter_datetime);

-- $END