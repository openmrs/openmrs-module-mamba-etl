USE analysis;

-- $BEGIN
SET @report_data = '
{
  "flat_report_metadata": [
    {
      "report_name": "UgandaEMR ART Card encounter",
      "flat_table_name": "flat_encounter_art",
      "encounter_type_uuid": "8d5b2be0-c2cc-11de-8d13-0010c6dffd0f",
      "table_columns": {
        "current_regimen": "dd2b0b4d-30ab-102d-86b0-7a5022ba4115",
        "latest_viral_load_date": "0b434cfa-b11c-4d14-aaa2-9aed6ca2da88",
        "viral_load_copies": "dc8d83e3-30ab-102d-86b0-7a5022ba4115",
        "return_date": "dcac04cf-30ab-102d-86b0-7a5022ba4115",
        "tpt_initiation_date": "483939c7-79ba-4ca4-8c3e-346488c97fc7",
        "tpt_status": "37d4ac43-b3b4-4445-b63b-e3acf47c8910",
        "pregnant": "dcda5179-30ab-102d-86b0-7a5022ba4115",
        "tb_status": "dce02aa1-30ab-102d-86b0-7a5022ba4115",
        "tb_end_date": "813e21e7-4ccb-4fe9-aaab-3c0e40b6e356"
      }
    }
  ]
}
';

CALL sp_extract_report_metadata(@report_data, 'mamba_dim_concept_metadata');
-- $END