DELIMITER //

-- Needed for now till incremental is full implemented. We will just drop all tables and recreate them

DROP PROCEDURE IF EXISTS sp_mamba_drop_all_derived_tables;

CREATE PROCEDURE sp_mamba_drop_all_derived_tables()

BEGIN

    DROP TABLE IF EXISTS fact_encounter_covid;
    DROP TABLE IF EXISTS dim_client_covid;
    DROP TABLE IF EXISTS mamba_fact_encounter_hts;
    DROP TABLE IF EXISTS mamba_dim_client_hts;
    DROP TABLE IF EXISTS mamba_fact_txcurr;
    DROP TABLE IF EXISTS mamba_fact_pmtct_pregnant_women;
    DROP TABLE IF EXISTS mamba_fact_pmtct_exposedinfants;

END //

DELIMITER ;