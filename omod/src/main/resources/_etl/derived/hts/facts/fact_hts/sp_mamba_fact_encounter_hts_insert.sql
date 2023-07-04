-- $BEGIN
INSERT INTO mamba_fact_encounter_hts
    (
        encounter_id,
        client_id,
        encounter_date,
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
        recency_rtri_result
    )
    SELECT
        encounter_id,
        client_id,
        encounter_datetime AS encounter_date,
        CAST(date_test_conducted AS DATE) AS date_tested,
        CASE consent_provided
           WHEN 'True' THEN 'Yes'
           WHEN 'False' THEN 'No'
           ELSE NULL END AS consent,
        CASE community_service_point
           WHEN 'mobile voluntary counseling and testing program' THEN 'Mobile VCT'
           WHEN 'Home based HIV testing program' THEN 'Homebased'
           WHEN 'Outreach Program' THEN 'Outreach'
           WHEN 'Voluntary counseling and testing center' THEN 'VCT'
           ELSE community_service_point
        END AS community_service_point,
        pop_type,
        CASE
           WHEN (key_pop_msm = 'Male who has sex with men') THEN 'MSM'
           WHEN (key_pop_fsw = 'Sex worker') THEN 'FSW'
           WHEN (key_pop_transgender = 'Transgender Persons') THEN 'TRANS'
           WHEN (key_pop_pwid = 'People Who Inject Drugs') THEN 'PWID'
           WHEN (key_pop_prisoners = 'Prisoners') THEN 'Prisoner'
           ELSE NULL
        END AS `keypop_category`,
        CASE
           WHEN (key_pop_AGYW = 'Adolescent Girls & Young Women') THEN 'AGYW'
           WHEN (key_pop_fisher_folk = 'Fisher Folk') THEN 'Fisher_folk'
           WHEN (key_pop_migrant_worker = 'Migrant Workers') THEN 'Migrant_worker'
           WHEN (key_pop_refugees = 'Refugees') THEN 'Refugees'
           WHEN (key_pop_truck_driver = 'Long distance truck driver') THEN 'Truck_driver'
           WHEN (key_pop_uniformed_forces = 'Uniformed Forces') THEN 'Uniformed_forces'
           ELSE NULL
        END AS `priority_pop`,
        test_setting,
        CASE facility_service_point
           WHEN 'Post Natal Program' THEN 'PNC'
           WHEN 'Family Planning Clinic' THEN 'FP Clinic'
           WHEN 'Antenatal program' THEN 'ANC'
           WHEN 'Sexually transmitted infection program/clinic' THEN 'STI Clinic'
           WHEN 'Tuberculosis treatment program' THEN 'TB Clinic'
           WHEN 'Labor and delivery unit' THEN 'L&D'
           WHEN 'Other' THEN 'Other Clinics'
           ELSE facility_service_point
        END  AS facility_service_point,
        CASE hts_approach
           WHEN 'Client Initiated Testing and Counselling' THEN 'CITC'
           WHEN 'Provider-initiated HIV testing and counseling' THEN 'PITC'
           ELSE hts_approach
        END AS hts_approach,
        pretest_counselling,
        type_pretest_counselling,
        reason_for_test,
        CASE ever_tested_hiv
           WHEN 'True' THEN 'Yes'
           WHEN 'False' THEN 'No'
           ELSE ever_tested_hiv
        END AS ever_tested_hiv,
        duration_since_last_test,
        couple_result,
        result_received_couple,
        test_conducted,
        initial_kit_name,
        initial_test_result,
        confirmatory_kit_name,
        last_test_result,
        CASE
           WHEN final_test_result IN ('+', 'POS','Positive') THEN 'Positive'
           WHEN final_test_result IN ('-', 'NEG','Negative') THEN 'Negative'
           WHEN final_test_result IN  ('Indeterminate','Inconclusive') THEN 'Indeterminate'
           ELSE final_test_result
        END AS final_test_result,
        CASE
           WHEN given_result IN ('True', 'Yes') THEN 'Yes'
           WHEN given_result IN ('No', 'False') THEN 'No'
           WHEN given_result = 'Unknown' THEN 'Unknown'
           ELSE given_result
        END AS given_result,
        CAST(date_given_result AS DATE) AS date_given_result,
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
    FROM
        `mamba_flat_encounter_hts` `hts`
;
-- $END
