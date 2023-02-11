-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_datatype_create
--

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_create;
/
CREATE PROCEDURE sp_mamba_dim_concept_datatype_create()
BEGIN
    -- $BEGIN

    CREATE TABLE mamba_dim_concept_datatype
    (
        concept_datatype_id  int           NOT NULL AUTO_INCREMENT,
        external_datatype_id int,
        datatype_name        NVARCHAR(255) NULL,
        PRIMARY KEY (concept_datatype_id)
    );

    create index mamba_dim_concept_datatype_external_datatype_id_index
        on mamba_dim_concept_datatype (external_datatype_id);


-- $END
END
/



-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_datatype_insert
--

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_insert;
/
CREATE PROCEDURE sp_mamba_dim_concept_datatype_insert()
BEGIN
    -- $BEGIN

    INSERT INTO mamba_dim_concept_datatype (external_datatype_id,
                                            datatype_name)
    SELECT dt.concept_datatype_id AS external_datatype_id,
           dt.name                AS datatype_name
    FROM openmrs_dev.concept_datatype dt
    WHERE dt.retired = 0;

-- $END
END
/



-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_datatype
--

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype;
/
CREATE PROCEDURE sp_mamba_dim_concept_datatype()
BEGIN
    -- $BEGIN

    CALL sp_mamba_dim_concept_datatype_create();
    CALL sp_mamba_dim_concept_datatype_insert();

-- $END
END
/



-- ---------------------------------------------------------------------------------------------
-- sp_data_processing
--

DROP PROCEDURE IF EXISTS sp_data_processing;
/
CREATE PROCEDURE sp_data_processing()
BEGIN
    -- $BEGIN

-- Base dimensions
-- SELECT 'Executing sp_mamba_dim_concept_datatype';
    CALL sp_mamba_dim_concept_datatype;

-- $END
END
/

