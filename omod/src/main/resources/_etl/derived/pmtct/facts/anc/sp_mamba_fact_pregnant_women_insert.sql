-- $BEGIN
INSERT INTO mamba_fact_pmtct_pregnant_women
(
    encounter_id,
    client_id,
    encounter_datetime,
    parity,
    gravida,
    missing,
    visit_type,
    ptracker_id,
    new_anc_visit,
    hiv_test_result,
    return_anc_visit,
    return_visit_date,
    hiv_test_performed,
    partner_hiv_tested,
    hiv_test_result_negative,
    hiv_test_result_positive,
    previously_known_positive,
    estimated_date_of_delivery,
    facility_of_next_appointment,
    date_of_last_menstrual_period,
    hiv_test_result_indeterminate,
    tested_for_hiv_during_this_visit,
    not_tested_for_hiv_during_this_visit
)
    SELECT
        anc.encounter_id,
        client_id,
        encounter_datetime,
        parity,
        gravida,
        missing,
        visit_type,
        ptracker_id,
        new_anc_visit,
        hiv_test_result,
        return_anc_visit,
        return_visit_date,
        hiv_test_performed,
        partner_hiv_tested,
        hiv_test_result_negative,
        hiv_test_result_positive,
        previously_known_positive,
        estimated_date_of_delivery,
        facility_of_next_appointment,
        date_of_last_menstrual_period,
        hiv_test_result_indeterminate,
        tested_for_hiv_during_this_visit,
        not_tested_for_hiv_during_this_visit
FROM mamba_flat_encounter_pmtct_anc anc
    INNER JOIN mamba_dim_person  p
        ON anc.client_id = p.person_id
WHERE visit_type = 'New ANC Visit'
    AND (anc.client_id NOT in (SELECT anc.client_id
                               FROM mamba_flat_encounter_pmtct_anc anc
                                        LEFT JOIN mamba_flat_encounter_pmtct_labor_delivery ld
                                                  ON ld.client_id = anc.client_id
                               WHERE ld.encounter_datetime >
                                     DATE_ADD(date_of_last_menstrual_period, INTERVAL 40 WEEK))
    OR anc.client_id NOT in (SELECT anc.client_id
                             FROM mamba_flat_encounter_pmtct_anc anc
                                      LEFT JOIN mamba_flat_encounter_pmtct_mother_postnatal mp
                                                ON mp.client_id = anc.client_id
                             WHERE mp.encounter_datetime >
                                   DATE_ADD(date_of_last_menstrual_period, INTERVAL 40 WEEK))
    )

;
-- $END