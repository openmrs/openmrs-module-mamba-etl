-- $BEGIN
INSERT INTO mamba_fact_pmtct_exposedinfants
(
    encounter_id,
    infant_client_id ,
    encounter_datetime,
    mother_client_id,
    visit_type,
    arv_adherence,
    linked_to_art,
    infant_hiv_test,
    hiv_test_performed,
    result_of_hiv_test,
    viral_load_results,
    hiv_exposure_status,
    viral_load_test_done,
    art_initiation_status,
    arv_prophylaxis_status,
    ctx_prophylaxis_status,
    patient_outcome_status,
    cotrimoxazole_adherence,
    child_hiv_dna_pcr_test_result,
    unique_antiretroviral_therapy_number,
    confirmatory_test_performed_on_this_vist,
    rapid_hiv_antibody_test_result_at_18_mths
)
    SELECT
        DISTINCT encounter_id,
        client_id ,
        encounter_datetime,
        a.person_a mother_person_id,
        visit_type,
        arv_adherence,
        linked_to_art,
        infant_hiv_test,
        hiv_test_performed,
        result_of_hiv_test,
        viral_load_results,
        hiv_exposure_status,
        viral_load_test_done,
        art_initiation_status,
        arv_prophylaxis_status,
        ctx_prophylaxis_status,
        patient_outcome_status,
        cotrimoxazole_adherence,
        child_hiv_dna_pcr_test_result,
        unique_antiretroviral_therapy_number,
        confirmatory_test_performed_on_this_vist,
        rapid_hiv_antibody_test_result_at_18_mths

    FROM mamba_flat_encounter_pmtct_infant_postnatal ip
        INNER JOIN mamba_dim_person p
            ON ip.client_id = p.person_id
    LEFT JOIN mamba_dim_relationship a ON  ip.client_id = a.person_b
    WHERE   (ip.client_id in (SELECT person_b FROM mamba_dim_relationship a
                INNER JOIN mamba_flat_encounter_pmtct_anc anc
                    ON a.person_a = anc.client_id
                WHERE (anc.hiv_test_result ='HIV Positive'
                           OR anc.hiv_test_performed = 'Previously known positive'))
            OR ip.client_id in (SELECT person_b FROM mamba_dim_relationship a
                INNER JOIN mamba_flat_encounter_pmtct_labor_delivery ld
                    ON a.person_a = ld.client_id
                where (ld.result_of_hiv_test ='HIV Positive'
                           OR ld.hiv_test_performed = 'Previously known positive'
                           OR ld.anc_hiv_status_first_visit like '%Positive%'))
            OR ip.client_id in (SELECT person_b FROM mamba_dim_relationship a
                INNER JOIN mamba_flat_encounter_mother_postnatal mp
                    ON a.person_a = mp.client_id
                where (mp.result_of_hiv_test like '%Positive%'
                           OR mp.hiv_test_performed = 'Previously known positive')))
;
-- $END