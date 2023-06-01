-- $BEGIN
INSERT INTO fact_encounter_hiv_hts (encounter_id,
                                    client_id,
                                    date_tested,
                                    consent,
                                    community_service_point,
                                    pop_type,
                                    keypop_category,
                                    priority_pop,
                                    test_setting,
                                    facility_service_point,
                                    hts_approach,
                                    pretest_counselling,
                                    type_pretest_counselling,
                                    reason_for_test,
                                    ever_tested_hiv,
                                    duration_since_last_test,
                                    couple_result,
                                    result_received_couple,
                                    test_conducted,
                                    initial_kit_name,
                                    initial_test_result,
                                    confirmatory_kit_name,
                                    last_test_result,
                                    final_test_result,
                                    given_result,
                                    date_given_result,
                                    tiebreaker_kit_name,
                                    tiebreaker_test_result,
                                    sti_last_6mo,
                                    sexually_active,
                                    syphilis_test_result,
                                    unprotected_sex_last_12mo,
                                    recency_consent,
                                    recency_test_done,
                                    recency_test_type,
                                    recency_vl_result,
                                    recency_rtri_result)
SELECT hts.encounter_id,
       `hts`.`client_id`                    AS `client_id`,
       CAST(date_test_conducted as DATE)    AS date_tested,
       CASE consent_provided
           WHEN 'True' THEN 'Yes'
           WHEN 'False' THEN 'No'
           ELSE NULL END                    AS consent,
       CASE community_service_point
           WHEN 'mobile voluntary counseling and testing program' THEN 'Mobile VCT'
           WHEN 'Home based HIV testing program' THEN 'Homebased'
           WHEN 'Outreach Program' THEN 'Outreach'
           WHEN 'Voluntary counseling and testing center' THEN 'VCT'

           ELSE community_service_point END as community_service_point,
       pop_type,
       CASE
           WHEN (`hts`.`key_pop_msm` = 1) THEN 'MSM'
           WHEN (`hts`.`key_pop_fsw` = 1) THEN 'FSW'
           WHEN (`hts`.`key_pop_transgender` = 1) THEN 'TRANS'
           WHEN (`hts`.`key_pop_pwid` = 1) THEN 'PWID'
           WHEN (`hts`.`key_pop_prisoners` = 1) THEN 'Prisoner'
           ELSE NULL END                    AS `keypop_category`,
       CASE
           WHEN (key_pop_AGYW = 1) THEN 'AGYW'
           WHEN (key_pop_fisher_folk = 1) THEN 'Fisher_folk'
           WHEN (key_pop_migrant_worker = 1) THEN 'Migrant_worker'
           WHEN (key_pop_refugees = 1) THEN 'Refugees'
           WHEN (key_pop_truck_driver = 1) THEN 'Truck_driver'
           WHEN (key_pop_uniformed_forces = 1) THEN 'Uniformed_forces'
           ELSE NULL END                    AS `priority_pop`,
       test_setting,
       CASE facility_service_point
           WHEN 'Post Natal Program' THEN 'PNC'
           WHEN 'Family Planning Clinic' THEN 'FP Clinic'
           WHEN 'Antenatal program' THEN 'ANC'
           WHEN 'Sexually transmitted infection program/clinic' THEN 'STI Clinic'
           WHEN 'Tuberculosis treatment program' THEN 'TB Clinic'
           WHEN 'Labor and delivery unit' THEN 'L&D'
           WHEN 'Other' THEN 'Other Clinics'
           ELSE facility_service_point END  as facility_service_point,
       CASE hts_approach
           WHEN 'Client Initiated Testing and Counselling' THEN 'CITC'
           WHEN 'Provider-initiated HIV testing and counseling' THEN 'PITC'
           ELSE hts_approach END            AS hts_approach,
       pretest_counselling,
       type_pretest_counselling,
       reason_for_test,
       CASE ever_tested_hiv
           WHEN 'True' THEN 'Yes'
           WHEN 'False' THEN 'No'
           ELSE NULL END                    AS ever_tested_hiv,
       duration_since_last_test,
       couple_result,
       result_received_couple,
       test_conducted,
       initial_kit_name,
       initial_test_result,
       confirmatory_kit_name,
       last_test_result,
       final_test_result,
       CASE
           WHEN given_result IN ('True', 'Yes') THEN 'Yes'
           WHEN given_result IN ('No', 'False') THEN 'No'
           WHEN given_result = 'Unknown' THEN 'Unknown'
           ELSE NULL END                    as given_result,
       CAST(date_given_result as DATE)      AS date_given_result,
       tiebreaker_kit_name,
       tiebreaker_test_result,
       sti_last_6mo,
       sexually_active,
       syphilis_test_result,
       unprotected_sex_last_12mo,
       recency_consent,
       recency_test_done,
       recency_test_type,
       recency_vl_result,
       recency_rtri_result
FROM `mamba_flat_encounter_hts` `hts`;
-- $END
