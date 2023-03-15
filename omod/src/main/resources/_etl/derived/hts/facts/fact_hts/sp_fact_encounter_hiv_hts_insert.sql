# ---------INSERT into table
USE analysis;

-- $BEGIN

INSERT INTO fact_encounter_hiv_hts (
    encounter_id,
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
    recency_rtri_result
)
select hts.encounter_id,`hts`.`client_id`   AS `client_id`,
    cast(date_test_conducted as date) AS date_tested,
    case consent_provided
        when 'True' then 'Yes'
        when 'False' then 'No'
    else NULL end AS consent,
    case community_service_point
        when 'mobile voluntary counseling and testing program' then 'Mobile VCT'
        when 'Home based HIV testing program' then 'Homebased'
        when 'Outreach Program' then 'Outreach'
        when 'Voluntary counseling and testing center' then 'VCT'

    else community_service_point end as community_service_point,
    pop_type,
    case
        when (`hts`.`key_pop_msm` = 1) then 'MSM'
        when (`hts`.`key_pop_fsw` = 1) then 'FSW'
        when (`hts`.`key_pop_transgender` = 1) then 'TRANS'
        when (`hts`.`key_pop_pwid` = 1) then 'PWID'
        when (`hts`.`key_pop_prisoners` = 1) then 'Prisoner'
    else NULL end  AS `keypop_category`,
    case
        when (key_pop_AGYW = 1) then 'AGYW'
        when (key_pop_fisher_folk = 1) then 'Fisher_folk'
        when (key_pop_migrant_worker = 1) then 'Migrant_worker'
        when (key_pop_refugees = 1) then 'Refugees'
        when (key_pop_truck_driver = 1) then 'Truck_driver'
        when (key_pop_uniformed_forces = 1) then 'Uniformed_forces'
    else NULL end  AS `priority_pop`,
    test_setting,
    case facility_service_point
        when 'Post Natal Program' then 'PNC'
        when 'Family Planning Clinic' then 'FP Clinic'
        when 'Antenatal program' then 'ANC'
        when 'Sexually transmitted infection program/clinic' then 'STI Clinic'
        when 'Tuberculosis treatment program' then 'TB Clinic'
        when 'Labor and delivery unit' then 'L&D'
        when 'Other' then 'Other Clinics'
    else facility_service_point end as facility_service_point,
    case hts_approach
        when 'Client Initiated Testing and Counselling' then 'CITC'
        when 'Provider-initiated HIV testing and counseling' then 'PITC'
    else hts_approach end AS hts_approach,
    pretest_counselling,
    type_pretest_counselling,
    reason_for_test,
    case ever_tested_hiv
        when 'True' then 'Yes'
        when 'False' then 'No'
    else NULL end AS ever_tested_hiv,
    duration_since_last_test,
    couple_result,
    result_received_couple,
    test_conducted,
    initial_kit_name,
    initial_test_result,
    confirmatory_kit_name,
    last_test_result,
    final_test_result,
    case
        when given_result in ('True','Yes') then 'Yes'
        when given_result in ('No', 'False') then 'No'
        when given_result ='Unknown' then 'Unknown'
        else NULL end as given_result,
    cast(date_given_result as date) AS date_given_result,
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
from `analysis`.`flat_encounter_hts` `hts`;


-- $END
