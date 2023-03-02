



        

-- ---------------------------------------------------------------------------------------------
-- sp_xf_system_drop_all_functions_in_schema
--


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_functions_in_schema;

/
CREATE PROCEDURE sp_xf_system_drop_all_stored_functions_in_schema(
    IN database_name NVARCHAR(255)
)
BEGIN
    DELETE FROM `mysql`.`proc` WHERE `type` = 'FUNCTION' AND `db` = database_name; -- works in mysql before v.8

END
/



        

-- ---------------------------------------------------------------------------------------------
-- sp_xf_system_drop_all_stored_procedures_in_schema
--


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_procedures_in_schema;

/
CREATE PROCEDURE sp_xf_system_drop_all_stored_procedures_in_schema(
    IN database_name NVARCHAR(255)
)
BEGIN

    DELETE FROM `mysql`.`proc` WHERE `type` = 'PROCEDURE' AND `db` = database_name; -- works in mysql before v.8

END
/



        

-- ---------------------------------------------------------------------------------------------
-- sp_xf_system_drop_all_objects_in_schema
--


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_objects_in_schema;

/
CREATE PROCEDURE sp_xf_system_drop_all_objects_in_schema(
    IN database_name NVARCHAR(255)
)
BEGIN

    CALL sp_xf_system_drop_all_stored_functions_in_schema(database_name);
    CALL sp_xf_system_drop_all_stored_procedures_in_schema (database_name);
    CALL sp_xf_system_drop_all_tables_in_schema(database_name);
    # CALL sp_xf_system_drop_all_views_in_schema (database_name);

END
/



        

-- ---------------------------------------------------------------------------------------------
-- sp_xf_system_drop_all_tables_in_schema
--


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_tables_in_schema;

/
CREATE PROCEDURE sp_xf_system_drop_all_tables_in_schema(
    IN database_name NVARCHAR(255)
)
BEGIN

    DECLARE tables_count INT;

    SELECT COUNT(1)
    INTO tables_count
    FROM information_schema.tables
    WHERE TABLE_TYPE = 'BASE TABLE'
      AND TABLE_SCHEMA = database_name;

    IF tables_count > 0 THEN

        SET session group_concat_max_len = 20000;

        SET @tbls = (SELECT GROUP_CONCAT(database_name, '.', TABLE_NAME SEPARATOR ', ')
                     FROM information_schema.tables
                     WHERE TABLE_TYPE = 'BASE TABLE'
                       AND TABLE_SCHEMA = database_name
                       AND TABLE_NAME REGEXP '^(mamba_|dim_|fact_|flat_)');

        IF (@tbls IS NOT NULL) THEN

            SET @drop_tables = CONCAT('DROP TABLE IF EXISTS ', @tbls);

            SET foreign_key_checks = 0; -- Remove check, so we don't have to drop tables in the correct order, or care if they exist or not.
            PREPARE drop_tbls FROM @drop_tables;
            EXECUTE drop_tbls;
            DEALLOCATE PREPARE drop_tbls;
            SET foreign_key_checks = 1;

        END IF;

    END IF;

END
/



        

-- ---------------------------------------------------------------------------------------------
-- sp_xf_system_execute_etl
--


DROP PROCEDURE IF EXISTS sp_xf_system_execute_etl;

/
CREATE PROCEDURE sp_xf_system_execute_etl()
BEGIN

    DECLARE start_time bigint;
    DECLARE end_time bigint;

    -- Fix start time in microseconds
    SET @start_time = (UNIX_TIMESTAMP(NOW()) * 1000000 + MICROSECOND(NOW(6)));

    call sp_data_processing();

    -- Fix end time in microseconds
    SET @end_time = (UNIX_TIMESTAMP(NOW()) * 1000000 + MICROSECOND(NOW(6)));

    -- Result
    select (@end_time - @start_time) / 1000;

END
/



        

-- ---------------------------------------------------------------------------------------------
-- sp_flat_encounter_table_create
--


DROP PROCEDURE IF EXISTS sp_flat_encounter_table_create;

/
CREATE PROCEDURE sp_flat_encounter_table_create(
    IN flat_encounter_table_name NVARCHAR(255)
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @column_labels := NULL;

    SET @drop_table = CONCAT('DROP TABLE IF EXISTS `', flat_encounter_table_name, '`');

    SELECT GROUP_CONCAT(column_label SEPARATOR ' TEXT, ') INTO @column_labels
                     FROM mamba_dim_concept_metadata
                     WHERE flat_table_name = flat_encounter_table_name;

    SET @create_table = CONCAT(
            'CREATE TABLE `', flat_encounter_table_name ,'` (encounter_id INT, client_id INT, ', @column_labels, ' TEXT);');

    PREPARE deletetb FROM @drop_table;
    PREPARE createtb FROM @create_table;

    EXECUTE deletetb;
    EXECUTE createtb;

    DEALLOCATE PREPARE deletetb;
    DEALLOCATE PREPARE createtb;

END
/



        

-- ---------------------------------------------------------------------------------------------
-- sp_flat_encounter_table_create_all
--

-- Flatten all Encounters given in Config folder

DROP PROCEDURE IF EXISTS sp_flat_encounter_table_create_all;

/
CREATE PROCEDURE sp_flat_encounter_table_create_all()
BEGIN

    DECLARE tbl_name NVARCHAR(50);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name) FROM mamba_dim_concept_metadata;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_flat_encounter_table_create(tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END
/



        

-- ---------------------------------------------------------------------------------------------
-- sp_flat_encounter_table_insert
--


DROP PROCEDURE IF EXISTS sp_flat_encounter_table_insert;

/
CREATE PROCEDURE sp_flat_encounter_table_insert(
    IN flat_encounter_table_name NVARCHAR(255)
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @tbl_name = flat_encounter_table_name;

    SET @old_sql = (SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
                          FROM INFORMATION_SCHEMA.COLUMNS
                          WHERE TABLE_NAME = @tbl_name
                            AND TABLE_SCHEMA = Database());

    SELECT
      GROUP_CONCAT(DISTINCT
        CONCAT(' MAX(CASE WHEN column_label = ''', column_label, ''' THEN ', fn_get_obs_value_column(concept_datatype), ' END) ', column_label)
      ORDER BY concept_metadata_id ASC)
    INTO @column_labels
    FROM mamba_dim_concept_metadata
         WHERE flat_table_name = @tbl_name;

    SET @insert_stmt = CONCAT(
            'INSERT INTO `', @tbl_name ,'` SELECT eo.encounter_id, eo.person_id, ', @column_labels, '
            FROM mamba_z_encounter_obs eo
                INNER JOIN mamba_dim_concept_metadata cm
                ON IF(cm.concept_answer_obs=1, cm.concept_uuid=eo.obs_value_coded_uuid, cm.concept_uuid=eo.obs_question_uuid)
            WHERE cm.flat_table_name = ''', @tbl_name, '''
            AND eo.encounter_type_uuid = cm.encounter_type_uuid
            GROUP BY eo.encounter_id;');

    PREPARE inserttbl FROM @insert_stmt;
    EXECUTE inserttbl;
    DEALLOCATE PREPARE inserttbl;

END
/



        

-- ---------------------------------------------------------------------------------------------
-- sp_flat_encounter_table_insert_all
--

-- Flatten all Encounters given in Config folder

DROP PROCEDURE IF EXISTS sp_flat_encounter_table_insert_all;

/
CREATE PROCEDURE sp_flat_encounter_table_insert_all()
BEGIN

    DECLARE tbl_name NVARCHAR(50);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name) FROM mamba_dim_concept_metadata;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_flat_encounter_table_insert(tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END
/



        

-- ---------------------------------------------------------------------------------------------
-- sp_multiselect_values_update
--


DROP PROCEDURE IF EXISTS `sp_multiselect_values_update`;

/
CREATE PROCEDURE `sp_multiselect_values_update`(
        IN table_to_update NVARCHAR(100),
        IN column_names NVARCHAR(20000),
        IN value_yes NVARCHAR(100),
        IN value_no NVARCHAR(100)
)
BEGIN

    SET @table_columns = column_names;
    SET @start_pos = 1;
    SET @comma_pos = locate(',', @table_columns);
    SET @end_loop = 0;

    SET @column_label = '';

    REPEAT
        IF @comma_pos > 0 THEN
            SET @column_label = substring(@table_columns, @start_pos, @comma_pos - @start_pos);
            SET @end_loop = 0;
        ELSE
            SET @column_label = substring(@table_columns, @start_pos);
            SET @end_loop = 1;
        END IF;

        -- UPDATE fact_hts SET @column_label=IF(@column_label IS NULL OR '', new_value_if_false, new_value_if_true);

         SET @update_sql = CONCAT(
             'UPDATE ', table_to_update ,' SET ', @column_label ,'= IF(', @column_label ,' IS NOT NULL, ''',value_yes,''', ''',value_no,''');');
        PREPARE stmt FROM @update_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        IF @end_loop = 0 THEN
            SET @table_columns = substring(@table_columns, @comma_pos + 1);
            SET @comma_pos = locate(',', @table_columns);
        END IF;
        UNTIL @end_loop = 1

    END REPEAT;

END
/



        

-- ---------------------------------------------------------------------------------------------
-- sp_extract_report_metadata
--


DROP PROCEDURE IF EXISTS sp_extract_report_metadata;

/
CREATE PROCEDURE sp_extract_report_metadata(
    IN report_data MEDIUMTEXT,
    IN metadata_table NVARCHAR(255)
)
BEGIN

    SET session group_concat_max_len = 20000;

    SELECT JSON_EXTRACT(report_data, '$.flat_report_metadata') INTO @report_array;
    SELECT JSON_LENGTH(@report_array) INTO @report_array_len;

    SET @report_count = 0;
    WHILE @report_count < @report_array_len
        DO

            SELECT JSON_EXTRACT(@report_array, CONCAT('$[', @report_count, ']')) INTO @report;
            SELECT JSON_EXTRACT(@report, '$.report_name') INTO @report_name;
            SELECT JSON_EXTRACT(@report, '$.flat_table_name') INTO @flat_table_name;
            SELECT JSON_EXTRACT(@report, '$.encounter_type_uuid') INTO @encounter_type;
            SELECT JSON_EXTRACT(@report, '$.table_columns') INTO @column_array;

            SELECT JSON_KEYS(@column_array) INTO @column_keys_array;
            SELECT JSON_LENGTH(@column_keys_array) INTO @column_keys_array_len;
            SET @col_count = 0;
            WHILE @col_count < @column_keys_array_len
                DO
                    SELECT JSON_EXTRACT(@column_keys_array, CONCAT('$[', @col_count, ']')) INTO @field_name;
                    SELECT JSON_EXTRACT(@column_array, CONCAT('$.', @field_name)) INTO @concept_uuid;

                    SET @tbl_name = '';
                    INSERT INTO mamba_dim_concept_metadata(report_name,
                                                            flat_table_name,
                                                            encounter_type_uuid,
                                                            column_label,
                                                            concept_uuid)
                    VALUES (JSON_UNQUOTE(@report_name),
                            JSON_UNQUOTE(@flat_table_name),
                            JSON_UNQUOTE(@encounter_type),
                            JSON_UNQUOTE(@field_name),
                            JSON_UNQUOTE(@concept_uuid));

                    SET @col_count = @col_count + 1;
                END WHILE;

            SET @report_count = @report_count + 1;
        END WHILE;

END
/



        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_datatype_create
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_create;

/
CREATE PROCEDURE sp_mamba_dim_concept_datatype_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_datatype (
    concept_datatype_id int NOT NULL AUTO_INCREMENT,
    external_datatype_id int,
    datatype_name NVARCHAR(255) NULL,
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

INSERT INTO mamba_dim_concept_datatype (
    external_datatype_id,
    datatype_name
)
SELECT
    dt.concept_datatype_id AS external_datatype_id,
    dt.name AS datatype_name
FROM
    openmrs_dev.concept_datatype dt
WHERE
    dt.retired = 0;

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
-- sp_mamba_dim_concept_create
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_create;

/
CREATE PROCEDURE sp_mamba_dim_concept_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept (
    concept_id int NOT NULL AUTO_INCREMENT,
    uuid CHAR(38) NOT NULL,
    external_concept_id int,
    external_datatype_id int, -- make it a FK
    datatype NVARCHAR(255) NULL,
    PRIMARY KEY (concept_id)
);

# Create indexes
create index mamba_dim_concept_external_concept_id_index
    on mamba_dim_concept (external_concept_id);

create index mamba_dim_concept_external_datatype_id_index
    on mamba_dim_concept (external_datatype_id);


-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_insert
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_insert;

/
CREATE PROCEDURE sp_mamba_dim_concept_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_concept (
     uuid,
     external_concept_id,
     external_datatype_id
)
SELECT
    c.uuid AS uuid,
    c.concept_id AS external_concept_id,
    c.datatype_id AS external_datatype_id
FROM
    openmrs_dev.concept c
WHERE
    c.retired = 0;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_update
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_update;

/
CREATE PROCEDURE sp_mamba_dim_concept_update()
BEGIN
-- $BEGIN

UPDATE mamba_dim_concept c
    INNER JOIN mamba_dim_concept_datatype dt
        ON c.external_datatype_id = dt.external_datatype_id
SET c.datatype = dt.datatype_name
WHERE c.concept_id > 0;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept;

/
CREATE PROCEDURE sp_mamba_dim_concept()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_create();
CALL sp_mamba_dim_concept_insert();
CALL sp_mamba_dim_concept_update();

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_answer_create
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_create;

/
CREATE PROCEDURE sp_mamba_dim_concept_answer_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_answer (
    concept_answer_id INT NOT NULL AUTO_INCREMENT,
    concept_id INT,
    answer_concept INT,
    answer_drug INT,
    PRIMARY KEY (concept_answer_id)
);

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_answer_insert
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_insert;

/
CREATE PROCEDURE sp_mamba_dim_concept_answer_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_concept_answer (
    concept_id,
    answer_concept,
    answer_drug
)
SELECT
    ca.concept_id AS concept_id,
    ca.answer_concept AS answer_concept,
    ca.answer_drug AS answer_drug
FROM
    openmrs_dev.concept_answer ca;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_answer
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer;

/
CREATE PROCEDURE sp_mamba_dim_concept_answer()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_answer_create();
CALL sp_mamba_dim_concept_answer_insert();

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_name_create
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_create;

/
CREATE PROCEDURE sp_mamba_dim_concept_name_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_name (
    concept_name_id int NOT NULL AUTO_INCREMENT,
    external_concept_id int,
    concept_name NVARCHAR(255) NULL,
    PRIMARY KEY (concept_name_id)
);

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_name_insert
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_insert;

/
CREATE PROCEDURE sp_mamba_dim_concept_name_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_concept_name (
    external_concept_id,
    concept_name
)
SELECT
    cn.concept_id AS external_concept_id,
    cn.name AS concept_name
FROM
    openmrs_dev.concept_name cn
WHERE
    cn.locale = 'en'
    AND cn.locale_preferred = 1;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_name
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name;

/
CREATE PROCEDURE sp_mamba_dim_concept_name()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_name_create();
CALL sp_mamba_dim_concept_name_insert();

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_encounter_type_create
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_create;

/
CREATE PROCEDURE sp_mamba_dim_encounter_type_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_encounter_type (
    encounter_type_id int NOT NULL AUTO_INCREMENT,
    external_encounter_type_id int,
    encounter_type_uuid CHAR(38) NOT NULL,
    PRIMARY KEY (encounter_type_id)
);

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_encounter_type_insert
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_insert;

/
CREATE PROCEDURE sp_mamba_dim_encounter_type_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_encounter_type (
     external_encounter_type_id,
     encounter_type_uuid
)
SELECT
    et.encounter_type_id AS external_encounter_type_id,
    et.uuid AS encounter_type_uuid
FROM
    openmrs_dev.encounter_type et
WHERE
    et.retired = 0;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_encounter_type
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type;

/
CREATE PROCEDURE sp_mamba_dim_encounter_type()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_encounter_type_create();
CALL sp_mamba_dim_encounter_type_insert();

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_encounter_create
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_create;

/
CREATE PROCEDURE sp_mamba_dim_encounter_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_encounter (
    encounter_id int NOT NULL AUTO_INCREMENT,
    external_encounter_id int,
    external_encounter_type_id int,
    encounter_type_uuid CHAR(38) NULL,
    PRIMARY KEY (encounter_id)
);

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_encounter_insert
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_insert;

/
CREATE PROCEDURE sp_mamba_dim_encounter_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_encounter (
     external_encounter_id,
     external_encounter_type_id
)
SELECT
    e.encounter_id AS external_encounter_id,
    e.encounter_type AS external_encounter_type_id
FROM
    openmrs_dev.encounter e;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_encounter_update
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_update;

/
CREATE PROCEDURE sp_mamba_dim_encounter_update()
BEGIN
-- $BEGIN

UPDATE mamba_dim_encounter e
    INNER JOIN mamba_dim_encounter_type et
        ON e.external_encounter_type_id = et.external_encounter_type_id
SET e.encounter_type_uuid = et.encounter_type_uuid
WHERE e.encounter_id > 0;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_encounter
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter;

/
CREATE PROCEDURE sp_mamba_dim_encounter()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_encounter_create();
CALL sp_mamba_dim_encounter_insert();
CALL sp_mamba_dim_encounter_update();

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_metadata_create
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata_create;

/
CREATE PROCEDURE sp_mamba_dim_concept_metadata_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_metadata
(
    concept_metadata_id INT           NOT NULL AUTO_INCREMENT,
    column_number       INT,
    column_label        NVARCHAR(50)  NOT NULL,
    concept_uuid        CHAR(38)      NOT NULL,
    concept_datatype    NVARCHAR(255) NULL,
    concept_answer_obs  TINYINT(1)    NOT NULL DEFAULT 0,
    report_name         NVARCHAR(255) NOT NULL,
    flat_table_name     NVARCHAR(255) NULL,
    encounter_type_uuid CHAR(38)      NOT NULL,

    PRIMARY KEY (concept_metadata_id)
);

-- ALTER TABLE `mamba_dim_concept_metadata`
--     ADD COLUMN `encounter_type_id` INT NULL AFTER `output_table_name`,
--     ADD CONSTRAINT `fk_encounter_type_id`
--         FOREIGN KEY (`encounter_type_id`) REFERENCES `mamba_dim_encounter_type` (`encounter_type_id`);

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_metadata_insert
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata_insert;

/
CREATE PROCEDURE sp_mamba_dim_concept_metadata_insert()
BEGIN
  -- $BEGIN

  SET @report_data = '{"flat_report_metadata":[
  {
  "report_name": "CT ART Therapy",
  "flat_table_name": "flat_encounter_arttherapy",
  "encounter_type_uuid": "74bf4fe6-8fdb-4228-be39-680a93a9cf6d",
  "table_columns": {
    "art_plan": "7557d77c-172b-4673-9335-67a38657dd01",
    "artstart_date": "159599AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "regimen": "dfbe256e-30ba-4033-837a-2e8477f2e7cd",
    "regimen_line": "164515AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "regimenline_switched_date": "164516AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "regimen_substituted_date": "164431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "art_stop_reason": "1252AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "art_stop_date": "160739AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "art_restart_date": "160738AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "notes": "165095AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
  }
},
  {
  "report_name": "CT Patient Enrolment",
  "flat_table_name": "flat_encounter_patientenrolment",
  "encounter_type_uuid": "7e54cd64-f9c3-11eb-8e6a-57478ce139b0",
  "table_columns": {
    "patient_type": "83e40f2c-c316-43e6-a12e-20a338100281",
    "pop_type": "166432AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "reenrolment_date": "20efadf9-86d3-4498-b3ab-7da4dad9c429",
    "reenrolment_reason": "14ae2dc9-5964-425a-87e8-9ca525cf055e",
    "date_enrolled": "160555AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "transferring_facility": "160535AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "artstart_date": "159599AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "current_regimen": "1257AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "transferin_documentation": "7962d0ed-0fb5-4580-8e46-6fd318091154",
    "date_confirmed_hiv_positive": "160554AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "test_type": "ca4953af-9ad4-4514-b54a-6832acd7cae9",
    "previous_haart_pep": "8bfdc328-1970-446c-9d7b-97d62703801b",
    "date_pep_last_used": "fbe937b6-a4ad-4ce5-9c43-002222fbabfb",
    "previous_haart_prep": "5d397775-0155-4033-95dc-edcec98e8190",
    "date_prep_last_used": "5af829e9-2427-4ed7-bb55-de4381610364",
    "previous_haart_hepatitis": "906ed69c-949b-47b5-b469-2205f0da473a",
    "date_hepatitis_last_used": "6a6cbda5-b155-4144-9ff9-ec3d1d1cd509",
    "nnrtis": "9064043b-5b18-4228-97ff-f0e20aaf9448",
    "nrtis": "54e7ff9b-4d93-41ba-ad0b-cb5f565785f2",
    "PIs": "77eed025-0f5c-4173-bf45-36e05a175aaf",
    "other_hivdrugs": "5424AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "treatmentsupporter_name": "160638AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "phone_number": "159635AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "treatmentsupporter_relationship": "160642AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "notes": "165095AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
  }
}]}';

  CALL sp_extract_report_metadata(@report_data, 'mamba_dim_concept_metadata');

  -- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_metadata_update
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata_update;

/
CREATE PROCEDURE sp_mamba_dim_concept_metadata_update()
BEGIN
-- $BEGIN

-- Update the Concept datatypes
UPDATE mamba_dim_concept_metadata md
    INNER JOIN mamba_dim_concept c
        ON md.concept_uuid = c.uuid
SET md.concept_datatype = c.datatype
WHERE md.concept_metadata_id > 0;

-- Update to True if this field is an obs answer to an obs Question
UPDATE mamba_dim_concept_metadata md
    INNER JOIN mamba_dim_concept c
        ON md.concept_uuid = c.uuid
    INNER JOIN mamba_dim_concept_answer ca
        ON ca.answer_concept = c.external_concept_id
SET md.concept_answer_obs = 1
WHERE md.concept_metadata_id > 0;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_concept_metadata
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata;

/
CREATE PROCEDURE sp_mamba_dim_concept_metadata()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_metadata_create();
CALL sp_mamba_dim_concept_metadata_insert();
CALL sp_mamba_dim_concept_metadata_update();

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_person_create
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_create;

/
CREATE PROCEDURE sp_mamba_dim_person_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_person (
    person_id int NOT NULL AUTO_INCREMENT,
    external_person_id int,
    birthdate NVARCHAR(255) NULL,
    gender NVARCHAR(255) NULL,
    PRIMARY KEY (person_id)
);
create index mamba_dim_person_external_person_id_index
    on mamba_dim_person (external_person_id);

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_person_insert
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_insert;

/
CREATE PROCEDURE sp_mamba_dim_person_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_person (
    external_person_id,
    birthdate,
    gender
)
SELECT
    psn.person_id AS external_person_id,
    psn.birthdate AS birthdate,
    psn.gender AS gender
FROM
    openmrs_dev.person psn;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_person
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_person;

/
CREATE PROCEDURE sp_mamba_dim_person()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_person_create();
CALL sp_mamba_dim_person_insert();

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_person_name_create
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_create;

/
CREATE PROCEDURE sp_mamba_dim_person_name_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_person_name (
    person_name_id int NOT NULL AUTO_INCREMENT,
    external_person_name_id int,
    external_person_id int,
    given_name NVARCHAR(255) NULL,
    PRIMARY KEY (person_name_id)
);
create index mamba_dim_person_name_external_person_id_index
    on mamba_dim_person_name (external_person_id);
-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_person_name_insert
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_insert;

/
CREATE PROCEDURE sp_mamba_dim_person_name_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_person_name (
    external_person_name_id,
    external_person_id,
    given_name
)
SELECT
    pn.person_name_id AS external_person_name_id,
    pn.person_id AS external_person_id,
    pn.given_name AS given_name
FROM
    openmrs_dev.person_name pn;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_person_name
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name;

/
CREATE PROCEDURE sp_mamba_dim_person_name()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_person_name_create();
CALL sp_mamba_dim_person_name_insert();

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_person_address_create
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_create;

/
CREATE PROCEDURE sp_mamba_dim_person_address_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_person_address (
    person_address_id int NOT NULL AUTO_INCREMENT,
    external_person_address_id int,
    external_person_id int,
    city_village NVARCHAR(255) NULL,
    county_district NVARCHAR(255) NULL,
    address1 NVARCHAR(255) NULL,
    address2 NVARCHAR(255) NULL,
    PRIMARY KEY (person_address_id)
);
create index mamba_dim_person_address_external_person_id_index
    on mamba_dim_person_address (external_person_id);

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_person_address_insert
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_insert;

/
CREATE PROCEDURE sp_mamba_dim_person_address_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_person_address (
    external_person_address_id,
    external_person_id,
    city_village,
    county_district,
    address1,
    address2
)
SELECT
    pa.person_address_id AS external_person_address_id,
    pa.person_id AS external_person_id,
    pa.city_village AS city_village,
    pa.county_district AS county_district,
    pa.address1 AS address1,
    pa.address2 AS address2
FROM
    openmrs_dev.person_address pa;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_dim_person_address
--


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address;

/
CREATE PROCEDURE sp_mamba_dim_person_address()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_person_address_create();
CALL sp_mamba_dim_person_address_insert();

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_dim_client_create
--


DROP PROCEDURE IF EXISTS sp_dim_client_create;

/
CREATE PROCEDURE sp_dim_client_create()
BEGIN
-- $BEGIN
CREATE TABLE dim_client (
    id INT NOT NULL AUTO_INCREMENT,
    client_id INT,
    date_of_birth DATE NULL,
    age INT,
    sex NVARCHAR(255) NULL,
    county NVARCHAR(255) NULL,
    sub_county NVARCHAR(255) NULL,
    ward NVARCHAR(255) NULL,
    PRIMARY KEY (id)
);
-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_dim_client_insert
--


DROP PROCEDURE IF EXISTS sp_dim_client_insert;

/
CREATE PROCEDURE sp_dim_client_insert()
BEGIN
-- $BEGIN

INSERT INTO dim_client (
    client_id ,
    date_of_birth,
    age,
    sex,
    county,
    sub_county,
    ward
)
SELECT
       `psn`.`person_id`                                                                                              AS `client_id`,
       `psn`.`birthdate`                                                                                              AS `date_of_birth`,
       timestampdiff(YEAR, `psn`.`birthdate`, now())                                                                  AS `age`,
       (CASE `psn`.`gender`
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE '_'
        END)                                                                                                          AS `sex`,
       `pa`.`county_district`                                                                                         AS `county`,
       `pa`.`city_village`                                                                                            AS `sub_county`,
       `pa`.`address1`                                                                                                AS `ward`
from ((`mamba_dim_person` `psn`
left join `mamba_dim_person_name` `pn` on ((`psn`.`external_person_id` = `pn`.`external_person_id`)))
left join `mamba_dim_person_address` `pa` on ((`psn`.`external_person_id` = `pa`.`external_person_id`)));


-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_dim_client_update
--


DROP PROCEDURE IF EXISTS sp_dim_client_update;

/
CREATE PROCEDURE sp_dim_client_update()
BEGIN
-- $BEGIN



-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_dim_client
--


DROP PROCEDURE IF EXISTS sp_dim_client;

/
CREATE PROCEDURE sp_dim_client()
BEGIN
-- $BEGIN

CALL sp_dim_client_create();
CALL sp_dim_client_insert();
CALL sp_dim_client_update();

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_z_encounter_obs
--


DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs;

/
CREATE PROCEDURE sp_mamba_z_encounter_obs()
BEGIN
-- $BEGIN

CREATE TABLE mamba_z_encounter_obs
(
    obs_question_uuid    CHAR(38),
    obs_answer_uuid      CHAR(38),
    obs_value_coded_uuid CHAR(38),
    encounter_type_uuid  CHAR(38)
)
SELECT o.encounter_id         AS encounter_id,
       o.person_id            AS person_id,
       o.obs_datetime         AS obs_datetime,
       o.concept_id           AS obs_question_concept_id,
       o.value_text           AS obs_value_text,
       o.value_numeric        AS obs_value_numeric,
       o.value_coded          AS obs_value_coded,
       o.value_datetime       AS obs_value_datetime,
       o.value_complex        AS obs_value_complex,
       o.value_drug           AS obs_value_drug,
       et.encounter_type_uuid AS encounter_type_uuid,
       NULL                   AS obs_question_uuid,
       NULL                   AS obs_answer_uuid,
       NULL                   AS obs_value_coded_uuid
FROM openmrs_dev.obs o
         INNER JOIN mamba_dim_encounter e
                    ON o.encounter_id = e.external_encounter_id
         INNER JOIN mamba_dim_encounter_type et
                    ON e.external_encounter_type_id = et.external_encounter_type_id
WHERE et.encounter_type_uuid
          IN (SELECT DISTINCT(md.encounter_type_uuid)
              FROM mamba_dim_concept_metadata md); -- only select obs for given encounter types

create index mamba_z_encounter_obs_encounter_id_type_uuid_person_id_index
    on mamba_z_encounter_obs (encounter_id, encounter_type_uuid, person_id);

create index mamba_z_encounter_obs_encounter_type_uuid_index
    on mamba_z_encounter_obs (encounter_type_uuid);

-- update obs question UUIDs
UPDATE mamba_z_encounter_obs z
    INNER JOIN mamba_dim_concept c
    ON z.obs_question_concept_id = c.external_concept_id
SET z.obs_question_uuid = c.uuid
WHERE TRUE;

-- update obs_value_coded (UUIDs & values)
UPDATE mamba_z_encounter_obs z
    INNER JOIN mamba_dim_concept_name cn
    ON z.obs_value_coded = cn.external_concept_id
    INNER JOIN mamba_dim_concept c
    ON z.obs_value_coded = c.external_concept_id
SET z.obs_value_text       = cn.concept_name,
    z.obs_value_coded_uuid = c.uuid
WHERE z.obs_value_coded IS NOT NULL;


-- update obs answer UUIDs
-- UPDATE mamba_z_encounter_obs z
-- INNER JOIN mamba_dim_concept c
-- -- ON z.obs_question_concept_id = c.external_concept_id
-- INNER JOIN mamba_dim_concept_datatype dt
-- ON dt.external_datatype_id = c.external_datatype_id
-- SET z.obs_answer_uuid = (IF(dt.datatype_name = 'Coded',
-- (SELECT c.uuid
-- FROM mamba_dim_concept c
--  where c.external_concept_id = z.obs_value_coded AND z.obs_value_coded IS NOT NULL),
-- c.uuid))
-- WHERE TRUE;

-- $END
END
/


        

-- ---------------------------------------------------------------------------------------------
-- sp_mamba_z_tables
--


DROP PROCEDURE IF EXISTS sp_mamba_z_tables;

/
CREATE PROCEDURE sp_mamba_z_tables()
BEGIN
-- $BEGIN

CALL sp_mamba_z_encounter_obs;

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

CALL sp_xf_system_drop_all_tables_in_schema('openmrs_working');

CALL sp_mamba_dim_concept_datatype;

CALL sp_mamba_dim_concept_answer;

CALL sp_mamba_dim_concept_name;

CALL sp_mamba_dim_concept;

CALL sp_mamba_dim_encounter_type;

CALL sp_mamba_dim_encounter;

CALL sp_mamba_dim_concept_metadata;

CALL sp_mamba_dim_person;

CALL sp_mamba_dim_person_name;

CALL sp_mamba_dim_person_address;

CALL sp_dim_client;

CALL sp_mamba_z_tables;

CALL sp_flat_encounter_table_create_all;

CALL sp_flat_encounter_table_insert_all;

-- $END
END
/


