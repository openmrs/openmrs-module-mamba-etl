
        
    
        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_calculate_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS fn_mamba_calculate_agegroup;

~
CREATE FUNCTION fn_mamba_calculate_agegroup(age INT) RETURNS VARCHAR(15)
    DETERMINISTIC
BEGIN
    DECLARE agegroup VARCHAR(15);
    IF (age < 1) THEN
        SET agegroup = '<1';
    ELSEIF age between 1 and 4 THEN
        SET agegroup = '1-4';
    ELSEIF age between 5 and 9 THEN
        SET agegroup = '5-9';
    ELSEIF age between 10 and 14 THEN
        SET agegroup = '10-14';
    ELSEIF age between 15 and 19 THEN
        SET agegroup = '15-19';
    ELSEIF age between 20 and 24 THEN
        SET agegroup = '20-24';
    ELSEIF age between 25 and 29 THEN
        SET agegroup = '25-29';
    ELSEIF age between 30 and 34 THEN
        SET agegroup = '30-34';
    ELSEIF age between 35 and 39 THEN
        SET agegroup = '35-39';
    ELSEIF age between 40 and 44 THEN
        SET agegroup = '40-44';
    ELSEIF age between 45 and 49 THEN
        SET agegroup = '45-49';
    ELSEIF age between 50 and 54 THEN
        SET agegroup = '50-54';
    ELSEIF age between 55 and 59 THEN
        SET agegroup = '55-59';
    ELSEIF age between 60 and 64 THEN
        SET agegroup = '60-64';
    ELSE
        SET agegroup = '65+';
    END IF;

    RETURN (agegroup);
END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_get_obs_value_column  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS fn_mamba_get_obs_value_column;

~
CREATE FUNCTION fn_mamba_get_obs_value_column(conceptDatatype VARCHAR(20)) RETURNS VARCHAR(20)
    DETERMINISTIC
BEGIN
    DECLARE obsValueColumn VARCHAR(20);
    IF (conceptDatatype = 'Text' OR conceptDatatype = 'Coded' OR conceptDatatype = 'N/A' OR
        conceptDatatype = 'Boolean') THEN
        SET obsValueColumn = 'obs_value_text';
    ELSEIF conceptDatatype = 'Date' OR conceptDatatype = 'Datetime' THEN
        SET obsValueColumn = 'obs_value_datetime';
    ELSEIF conceptDatatype = 'Numeric' THEN
        SET obsValueColumn = 'obs_value_numeric';
    END IF;

    RETURN (obsValueColumn);
END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_age_calculator  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS fn_mamba_age_calculator;

~
CREATE FUNCTION fn_mamba_age_calculator (birthdate DATE,deathDate DATE) RETURNS  Integer
    DETERMINISTIC
BEGIN
    DECLARE onDate DATE;
    DECLARE today DATE;
    DECLARE bday DATE;
    DECLARE age INT;
    DECLARE todaysMonth INT;
    DECLARE bdayMonth INT;
    DECLARE todaysDay INT;
    DECLARE bdayDay INT;

    SET onDate = NULL ;

    IF birthdate IS NULL THEN
        RETURN NULL;
    ELSE
        SET today = CURDATE();

        IF onDate IS NOT NULL THEN
            SET today = onDate;
        END IF;

        IF deathDate IS NOT NULL AND today > deathDate THEN
            SET today = deathDate;
        END IF;

        SET bday = birthdate;
        SET age = YEAR(today) - YEAR(bday);
        SET todaysMonth = MONTH(today);
        SET bdayMonth = MONTH(bday);
        SET todaysDay = DAY(today);
        SET bdayDay = DAY(bday);

        IF todaysMonth < bdayMonth THEN
            SET age = age - 1;
        ELSEIF todaysMonth = bdayMonth AND todaysDay < bdayDay THEN
            SET age = age - 1;
        END IF;

        RETURN age;
    END IF;
END;




        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_functions_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_functions_in_schema;

~
CREATE PROCEDURE sp_xf_system_drop_all_stored_functions_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN
    DELETE FROM `mysql`.`proc` WHERE `type` = 'FUNCTION' AND `db` = database_name; -- works in mysql before v.8

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_stored_procedures_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_procedures_in_schema;

~
CREATE PROCEDURE sp_xf_system_drop_all_stored_procedures_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN

    DELETE FROM `mysql`.`proc` WHERE `type` = 'PROCEDURE' AND `db` = database_name; -- works in mysql before v.8

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_objects_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_objects_in_schema;

~
CREATE PROCEDURE sp_xf_system_drop_all_objects_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN

    CALL sp_xf_system_drop_all_stored_functions_in_schema(database_name);
    CALL sp_xf_system_drop_all_stored_procedures_in_schema(database_name);
    CALL sp_xf_system_drop_all_tables_in_schema(database_name);
    # CALL sp_xf_system_drop_all_views_in_schema (database_name);

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_tables_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_tables_in_schema;

-- CREATE PROCEDURE sp_xf_system_drop_all_tables_in_schema(IN database_name CHAR(255) CHARACTER SET UTF8MB4)
~
CREATE PROCEDURE sp_xf_system_drop_all_tables_in_schema()
BEGIN

    DECLARE tables_count INT;

    SET @database_name = (SELECT DATABASE());

    SELECT COUNT(1)
    INTO tables_count
    FROM information_schema.tables
    WHERE TABLE_TYPE = 'BASE TABLE'
      AND TABLE_SCHEMA = @database_name;

    IF tables_count > 0 THEN

        SET session group_concat_max_len = 20000;

        SET @tbls = (SELECT GROUP_CONCAT(@database_name, '.', TABLE_NAME SEPARATOR ', ')
                     FROM information_schema.tables
                     WHERE TABLE_TYPE = 'BASE TABLE'
                       AND TABLE_SCHEMA = @database_name
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

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_execute  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_etl_execute;

~
CREATE PROCEDURE sp_mamba_etl_execute()
BEGIN
    DECLARE error_message VARCHAR(255) DEFAULT 'OK';
    DECLARE error_code CHAR(5) DEFAULT '00000';

    DECLARE start_time bigint;
    DECLARE end_time bigint;
    DECLARE start_date_time DATETIME;
    DECLARE end_date_time DATETIME;

    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            GET DIAGNOSTICS CONDITION 1
                error_code = RETURNED_SQLSTATE,
                error_message = MESSAGE_TEXT;

            -- SET @sql = CONCAT('SIGNAL SQLSTATE ''', error_code, ''' SET MESSAGE_TEXT = ''', error_message, '''');
            -- SET @sql = CONCAT('SET @signal = ''', @sql, '''');

            -- SET @sql = CONCAT('SIGNAL SQLSTATE ''', error_code, ''' SET MESSAGE_TEXT = ''', error_message, '''');
            -- PREPARE stmt FROM @sql;
            -- EXECUTE stmt;
            -- DEALLOCATE PREPARE stmt;

            INSERT INTO zzmamba_etl_tracker (initial_run_date,
                                             start_date,
                                             end_date,
                                             time_taken_microsec,
                                             completion_status,
                                             success_or_error_message,
                                             next_run_date)
            SELECT NOW(),
                   start_date_time,
                   NOW(),
                   (((UNIX_TIMESTAMP(NOW()) * 1000000 + MICROSECOND(NOW(6))) - @start_time) / 1000),
                   'ERROR',
                   (CONCAT(error_code, ' : ', error_message)),
                   NOW() + 5;
        END;

    -- Fix start time in microseconds
    SET start_date_time = NOW();
    SET @start_time = (UNIX_TIMESTAMP(NOW()) * 1000000 + MICROSECOND(NOW(6)));

    CALL sp_mamba_data_processing_etl();

    -- Fix end time in microseconds
    SET end_date_time = NOW();
    SET @end_time = (UNIX_TIMESTAMP(NOW()) * 1000000 + MICROSECOND(NOW(6)));

    -- Result
    SET @time_taken = (@end_time - @start_time) / 1000;
    SELECT @time_taken;


    INSERT INTO zzmamba_etl_tracker (initial_run_date,
                                     start_date,
                                     end_date,
                                     time_taken_microsec,
                                     completion_status,
                                     success_or_error_message,
                                     next_run_date)
    SELECT NOW(), start_date_time, end_date_time, @time_taken, 'SUCCESS', 'OK', NOW() + 5;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_create;

~
CREATE PROCEDURE sp_mamba_flat_encounter_table_create(
    IN flat_encounter_table_name VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @column_labels := NULL;

    SET @drop_table = CONCAT('DROP TABLE IF EXISTS `', flat_encounter_table_name, '`');

    SELECT GROUP_CONCAT(column_label SEPARATOR ' TEXT, ')
    INTO @column_labels
    FROM mamba_dim_concept_metadata
    WHERE flat_table_name = flat_encounter_table_name
      AND concept_datatype IS NOT NULL;

    IF @column_labels IS NULL THEN
        SET @create_table = CONCAT(
                'CREATE TABLE `', flat_encounter_table_name, '` (encounter_id INT NOT NULL, client_id INT NOT NULL, encounter_datetime DATETIME NOT NULL);');
    ELSE
        SET @create_table = CONCAT(
                'CREATE TABLE `', flat_encounter_table_name, '` (encounter_id INT NOT NULL, client_id INT NOT NULL, encounter_datetime DATETIME NOT NULL, ', @column_labels,
                ' TEXT);');
    END IF;


    PREPARE deletetb FROM @drop_table;
    PREPARE createtb FROM @create_table;

    EXECUTE deletetb;
    EXECUTE createtb;

    DEALLOCATE PREPARE deletetb;
    DEALLOCATE PREPARE createtb;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_create_all;

~
CREATE PROCEDURE sp_mamba_flat_encounter_table_create_all()
BEGIN

    DECLARE tbl_name CHAR(50) CHARACTER SET UTF8MB4;

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

        CALL sp_mamba_flat_encounter_table_create(tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_insert;

~
CREATE PROCEDURE sp_mamba_flat_encounter_table_insert(
    IN flat_encounter_table_name CHAR(255) CHARACTER SET UTF8MB4
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
            CONCAT(' MAX(CASE WHEN column_label = ''', column_label, ''' THEN ',
                fn_mamba_get_obs_value_column(concept_datatype), ' END) ', column_label)
            ORDER BY id ASC)
    INTO @column_labels
    FROM mamba_dim_concept_metadata
    WHERE flat_table_name = @tbl_name;

    SET @insert_stmt = CONCAT(
            'INSERT INTO `', @tbl_name, '` SELECT eo.encounter_id, eo.person_id, eo.encounter_datetime, ',
            @column_labels, '
            FROM mamba_z_encounter_obs eo
                INNER JOIN mamba_dim_concept_metadata cm
                ON cm.concept_uuid=eo.obs_question_uuid
            WHERE cm.flat_table_name = ''', @tbl_name, '''
            AND eo.encounter_type_uuid = cm.encounter_type_uuid
            AND eo.row_num = cm.row_num
            GROUP BY eo.encounter_id, eo.person_id, eo.encounter_datetime;');

    PREPARE inserttbl FROM @insert_stmt;
    EXECUTE inserttbl;
    DEALLOCATE PREPARE inserttbl;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_insert_all;

~
CREATE PROCEDURE sp_mamba_flat_encounter_table_insert_all()
BEGIN

    DECLARE tbl_name CHAR(50) CHARACTER SET UTF8MB4;

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

        CALL sp_mamba_flat_encounter_table_insert(tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_multiselect_values_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS `sp_mamba_multiselect_values_update`;

~
CREATE PROCEDURE `sp_mamba_multiselect_values_update`(
    IN table_to_update CHAR(100) CHARACTER SET UTF8MB4,
    IN column_names TEXT CHARACTER SET UTF8MB4,
    IN value_yes CHAR(100) CHARACTER SET UTF8MB4,
    IN value_no CHAR(100) CHARACTER SET UTF8MB4
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
                'UPDATE ', table_to_update, ' SET ', @column_label, '= IF(', @column_label, ' IS NOT NULL, ''',
                value_yes, ''', ''', value_no, ''');');
        PREPARE stmt FROM @update_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        IF @end_loop = 0 THEN
            SET @table_columns = substring(@table_columns, @comma_pos + 1);
            SET @comma_pos = locate(',', @table_columns);
        END IF;
    UNTIL @end_loop = 1
        END REPEAT;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_extract_report_metadata  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_extract_report_metadata;

~
CREATE PROCEDURE sp_mamba_extract_report_metadata(
    IN report_data MEDIUMTEXT CHARACTER SET UTF8MB4,
    IN metadata_table VARCHAR(255) CHARSET UTF8MB4
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
            SELECT JSON_EXTRACT(@report, '$.concepts_locale') INTO @concepts_locale;
            SELECT JSON_EXTRACT(@report, '$.table_columns') INTO @column_array;

            SELECT JSON_KEYS(@column_array) INTO @column_keys_array;
            SELECT JSON_LENGTH(@column_keys_array) INTO @column_keys_array_len;
            SET @col_count = 0;
            WHILE @col_count < @column_keys_array_len
                DO
                    SELECT JSON_EXTRACT(@column_keys_array, CONCAT('$[', @col_count, ']')) INTO @field_name;
                    SELECT JSON_EXTRACT(@column_array, CONCAT('$.', @field_name)) INTO @concept_uuid;

                    SET @tbl_name = '';
                    INSERT INTO mamba_dim_concept_metadata
                        (
                            report_name,
                            flat_table_name,
                            encounter_type_uuid,
                            column_label,
                            concept_uuid,
                            concepts_locale
                        )
                    VALUES (JSON_UNQUOTE(@report_name),
                            JSON_UNQUOTE(@flat_table_name),
                            JSON_UNQUOTE(@encounter_type),
                            JSON_UNQUOTE(@field_name),
                            JSON_UNQUOTE(@concept_uuid),
                            JSON_UNQUOTE(@concepts_locale));

                    SET @col_count = @col_count + 1;
                END WHILE;

            SET @report_count = @report_count + 1;
        END WHILE;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_load_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_load_agegroup;

~
CREATE PROCEDURE sp_mamba_load_agegroup()
BEGIN
    DECLARE age INT DEFAULT 0;
    WHILE age <= 120
        DO
            INSERT INTO mamba_dim_agegroup(age, datim_agegroup, normal_agegroup)
            VALUES (age, fn_mamba_calculate_agegroup(age), IF(age < 15, '<15', '15+'));
            SET age = age + 1;
        END WHILE;
END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_location_create;

~
CREATE PROCEDURE sp_mamba_dim_location_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_location
(
    id              INT          NOT NULL AUTO_INCREMENT,
    location_id     INT          NOT NULL,
    name            VARCHAR(255) NOT NULL,
    description     VARCHAR(255) NULL,
    city_village    VARCHAR(255) NULL,
    state_province  VARCHAR(255) NULL,
    postal_code     VARCHAR(50)  NULL,
    country         VARCHAR(50)  NULL,
    latitude        VARCHAR(50)  NULL,
    longitude       VARCHAR(50)  NULL,
    county_district VARCHAR(255) NULL,
    address1        VARCHAR(255) NULL,
    address2        VARCHAR(255) NULL,
    address3        VARCHAR(255) NULL,
    address4        VARCHAR(255) NULL,
    address5        VARCHAR(255) NULL,
    address6        VARCHAR(255) NULL,
    address7        VARCHAR(255) NULL,
    address8        VARCHAR(255) NULL,
    address9        VARCHAR(255) NULL,
    address10       VARCHAR(255) NULL,
    address11       VARCHAR(255) NULL,
    address12       VARCHAR(255) NULL,
    address13       VARCHAR(255) NULL,
    address14       VARCHAR(255) NULL,
    address15       VARCHAR(255) NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_location_location_id_index
    ON mamba_dim_location (location_id);

CREATE INDEX mamba_dim_location_name_index
    ON mamba_dim_location (name);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_location_insert;

~
CREATE PROCEDURE sp_mamba_dim_location_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_location (location_id,
                                name,
                                description,
                                city_village,
                                state_province,
                                postal_code,
                                country,
                                latitude,
                                longitude,
                                county_district,
                                address1,
                                address2,
                                address3,
                                address4,
                                address5,
                                address6,
                                address7,
                                address8,
                                address9,
                                address10,
                                address11,
                                address12,
                                address13,
                                address14,
                                address15)
SELECT location_id,
       name,
       description,
       city_village,
       state_province,
       postal_code,
       country,
       latitude,
       longitude,
       county_district,
       address1,
       address2,
       address3,
       address4,
       address5,
       address6,
       address7,
       address8,
       address9,
       address10,
       address11,
       address12,
       address13,
       address14,
       address15
FROM location;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_location_update;

~
CREATE PROCEDURE sp_mamba_dim_location_update()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_location;

~
CREATE PROCEDURE sp_mamba_dim_location()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_location_create();
CALL sp_mamba_dim_location_insert();
CALL sp_mamba_dim_location_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_create;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_patient_identifier_type
(
    id                         INT         NOT NULL AUTO_INCREMENT,
    patient_identifier_type_id INT         NOT NULL,
    name                       VARCHAR(50) NOT NULL,
    description                TEXT        NULL,
    uuid                       CHAR(38)    NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_patient_identifier_type_id_index
    ON mamba_dim_patient_identifier_type (patient_identifier_type_id);

CREATE INDEX mamba_dim_patient_identifier_type_name_index
    ON mamba_dim_patient_identifier_type (name);

CREATE INDEX mamba_dim_patient_identifier_type_uuid_index
    ON mamba_dim_patient_identifier_type (uuid);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_insert;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_patient_identifier_type (patient_identifier_type_id,
                                               name,
                                               description,
                                               uuid)
SELECT patient_identifier_type_id,
       name,
       description,
       uuid
FROM patient_identifier_type;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_update;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_update()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_patient_identifier_type_create();
CALL sp_mamba_dim_patient_identifier_type_insert();
CALL sp_mamba_dim_patient_identifier_type_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_create;

~
CREATE PROCEDURE sp_mamba_dim_concept_datatype_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_datatype
(
    id                  INT          NOT NULL AUTO_INCREMENT,
    concept_datatype_id INT          NOT NULL,
    datatype_name       VARCHAR(255) NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_concept_datatype_concept_datatype_id_index
    ON mamba_dim_concept_datatype (concept_datatype_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_insert;

~
CREATE PROCEDURE sp_mamba_dim_concept_datatype_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_concept_datatype (concept_datatype_id,
                                        datatype_name)
SELECT dt.concept_datatype_id AS concept_datatype_id,
       dt.name                AS datatype_name
FROM concept_datatype dt;
-- WHERE dt.retired = 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype;

~
CREATE PROCEDURE sp_mamba_dim_concept_datatype()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_datatype_create();
CALL sp_mamba_dim_concept_datatype_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_create;

~
CREATE PROCEDURE sp_mamba_dim_concept_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept
(
    id          INT          NOT NULL AUTO_INCREMENT,
    concept_id  INT          NOT NULL,
    uuid        CHAR(38)     NOT NULL,
    datatype_id INT NOT NULL, -- make it a FK
    datatype    VARCHAR(100) NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_concept_concept_id_index
    ON mamba_dim_concept (concept_id);

CREATE INDEX mamba_dim_concept_uuid_index
    ON mamba_dim_concept (uuid);

CREATE INDEX mamba_dim_concept_datatype_id_index
    ON mamba_dim_concept (datatype_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_insert;

~
CREATE PROCEDURE sp_mamba_dim_concept_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_concept (uuid,
                               concept_id,
                               datatype_id)
SELECT c.uuid        AS uuid,
       c.concept_id  AS concept_id,
       c.datatype_id AS datatype_id
FROM concept c;
-- WHERE c.retired = 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_update;

~
CREATE PROCEDURE sp_mamba_dim_concept_update()
BEGIN
-- $BEGIN

UPDATE mamba_dim_concept c
    INNER JOIN mamba_dim_concept_datatype dt
    ON c.datatype_id = dt.concept_datatype_id
SET c.datatype = dt.datatype_name
WHERE c.id > 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept;

~
CREATE PROCEDURE sp_mamba_dim_concept()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_create();
CALL sp_mamba_dim_concept_insert();
CALL sp_mamba_dim_concept_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_create;

~
CREATE PROCEDURE sp_mamba_dim_concept_answer_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_answer
(
    id                INT NOT NULL AUTO_INCREMENT,
    concept_answer_id INT NOT NULL,
    concept_id        INT NOT NULL,
    answer_concept    INT,
    answer_drug       INT,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_concept_answer_concept_answer_id_index
    ON mamba_dim_concept_answer (concept_answer_id);

CREATE INDEX mamba_dim_concept_answer_concept_id_index
    ON mamba_dim_concept_answer (concept_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_insert;

~
CREATE PROCEDURE sp_mamba_dim_concept_answer_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_concept_answer (concept_answer_id,
                                      concept_id,
                                      answer_concept,
                                      answer_drug)
SELECT ca.concept_answer_id AS concept_answer_id,
       ca.concept_id        AS concept_id,
       ca.answer_concept    AS answer_concept,
       ca.answer_drug       AS answer_drug
FROM concept_answer ca;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer;

~
CREATE PROCEDURE sp_mamba_dim_concept_answer()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_answer_create();
CALL sp_mamba_dim_concept_answer_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_create;

~
CREATE PROCEDURE sp_mamba_dim_concept_name_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_name
(
    id                INT          NOT NULL AUTO_INCREMENT,
    concept_name_id   INT          NOT NULL,
    concept_id        INT,
    name              VARCHAR(255) NOT NULL,
    locale            VARCHAR(50)  NOT NULL,
    locale_preferred  TINYINT,
    concept_name_type VARCHAR(255),

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_concept_name_concept_name_id_index
    ON mamba_dim_concept_name (concept_name_id);

CREATE INDEX mamba_dim_concept_name_concept_id_index
    ON mamba_dim_concept_name (concept_id);

CREATE INDEX mamba_dim_concept_name_concept_name_type_index
    ON mamba_dim_concept_name (concept_name_type);

CREATE INDEX mamba_dim_concept_name_locale_index
    ON mamba_dim_concept_name (locale);

CREATE INDEX mamba_dim_concept_name_locale_preferred_index
    ON mamba_dim_concept_name (locale_preferred);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_insert;

~
CREATE PROCEDURE sp_mamba_dim_concept_name_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_concept_name (concept_name_id,
                                    concept_id,
                                    name,
                                    locale,
                                    locale_preferred,
                                    concept_name_type)
SELECT cn.concept_name_id,
       cn.concept_id,
       cn.name,
       cn.locale,
       cn.locale_preferred,
       cn.concept_name_type
FROM concept_name cn
 WHERE cn.locale = 'en'
  AND cn.locale_preferred = 1
    AND cn.voided = 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name;

~
CREATE PROCEDURE sp_mamba_dim_concept_name()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_name_create();
CALL sp_mamba_dim_concept_name_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_create;

~
CREATE PROCEDURE sp_mamba_dim_encounter_type_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_encounter_type
(
    id                INT      NOT NULL AUTO_INCREMENT,
    encounter_type_id INT      NOT NULL,
    uuid              CHAR(38) NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_encounter_type_encounter_type_id_index
    ON mamba_dim_encounter_type (encounter_type_id);

CREATE INDEX mamba_dim_encounter_type_uuid_index
    ON mamba_dim_encounter_type (uuid);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_insert;

~
CREATE PROCEDURE sp_mamba_dim_encounter_type_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_encounter_type (encounter_type_id,
                                      uuid)
SELECT et.encounter_type_id,
       et.uuid
FROM encounter_type et;
-- WHERE et.retired = 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type;

~
CREATE PROCEDURE sp_mamba_dim_encounter_type()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_encounter_type_create();
CALL sp_mamba_dim_encounter_type_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_create;

~
CREATE PROCEDURE sp_mamba_dim_encounter_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_encounter
(
    id                  INT        NOT NULL AUTO_INCREMENT,
    encounter_id        INT        NOT NULL,
    uuid                CHAR(38)   NOT NULL,
    encounter_type      INT        NOT NULL,
    encounter_type_uuid CHAR(38)   NULL,
    patient_id          INT        NOT NULL,
    encounter_datetime  DATETIME   NOT NULL,
    date_created        DATETIME   NOT NULL,
    voided              TINYINT NOT NULL,
    visit_id            INT        NULL,

    CONSTRAINT encounter_encounter_id_index
        UNIQUE (encounter_id),

    CONSTRAINT encounter_uuid_index
        UNIQUE (uuid),

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_encounter_encounter_id_index
    ON mamba_dim_encounter (encounter_id);

CREATE INDEX mamba_dim_encounter_encounter_type_index
    ON mamba_dim_encounter (encounter_type);

CREATE INDEX mamba_dim_encounter_uuid_index
    ON mamba_dim_encounter (uuid);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_insert;

~
CREATE PROCEDURE sp_mamba_dim_encounter_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_encounter (encounter_id,
                                 uuid,
                                 encounter_type,
                                 encounter_type_uuid,
                                 patient_id,
                                 encounter_datetime,
                                 date_created,
                                 voided,
                                 visit_id)
SELECT e.encounter_id,
       e.uuid,
       e.encounter_type,
       et.uuid,
       e.patient_id,
       e.encounter_datetime,
       e.date_created,
       e.voided,
       e.visit_id
FROM encounter e
         INNER JOIN mamba_dim_encounter_type et
                    ON e.encounter_type = et.encounter_type_id
WHERE et.uuid
          IN (SELECT DISTINCT(md.encounter_type_uuid)
              FROM mamba_dim_concept_metadata md);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_update;

~
CREATE PROCEDURE sp_mamba_dim_encounter_update()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter;

~
CREATE PROCEDURE sp_mamba_dim_encounter()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_encounter_create();
CALL sp_mamba_dim_encounter_insert();
CALL sp_mamba_dim_encounter_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_metadata_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata_create;

~
CREATE PROCEDURE sp_mamba_dim_concept_metadata_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_metadata
(
    id                  INT          NOT NULL AUTO_INCREMENT,
    concept_id          INT          NULL,
    concept_uuid        CHAR(38)     NOT NULL,
    concept_name        VARCHAR(255) NULL,
    concepts_locale     VARCHAR(20)  NOT NULL,
    column_number       INT,
    column_label        VARCHAR(50)  NOT NULL,
    concept_datatype    VARCHAR(255) NULL,
    concept_answer_obs  TINYINT      NOT NULL DEFAULT 0,
    report_name         VARCHAR(255) NOT NULL,
    flat_table_name     VARCHAR(255) NULL,
    encounter_type_uuid CHAR(38)     NOT NULL,
    row_num             INT          NULL DEFAULT 1,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_concept_metadata_concept_id_index
    ON mamba_dim_concept_metadata (concept_id);

CREATE INDEX mamba_dim_concept_metadata_concept_uuid_index
    ON mamba_dim_concept_metadata (concept_uuid);

CREATE INDEX mamba_dim_concept_metadata_encounter_type_uuid_index
    ON mamba_dim_concept_metadata (encounter_type_uuid);

CREATE INDEX mamba_dim_concept_metadata_concepts_locale_index
    ON mamba_dim_concept_metadata (concepts_locale);

-- ALTER TABLE `mamba_dim_concept_metadata`
--     ADD COLUMN `encounter_type_id` INT NULL AFTER `output_table_name`,
--     ADD CONSTRAINT `fk_encounter_type_id`
--         FOREIGN KEY (`encounter_type_id`) REFERENCES `mamba_dim_encounter_type` (`encounter_type_id`);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_metadata_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata_insert;

~
CREATE PROCEDURE sp_mamba_dim_concept_metadata_insert()
BEGIN
  -- $BEGIN

  SET @report_data = '{"flat_report_metadata":[
  {
  "report_name": "PMTCT Infant Postnatal visit",
  "flat_table_name": "mamba_flat_encounter_pmtct_infant_postnatal",
  "encounter_type_uuid": "af1f1b24-d2e8-4282-b308-0bf79b365584",
  "concepts_locale": "en",
  "table_columns": {
        "arv_prophylaxis_status": "1148AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "viral_load_results": "1305AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "arv_adherence": "1658AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "result_of_hiv_test": "159427AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "patient_outcome_status": "160433AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "cotrimoxazole_adherence": "161652AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "viral_load_test_done": "163310AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "visit_type": "164181AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "hiv_test_performed": "164401AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "unique_antiretroviral_therapy_number": "164402AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "child_hiv_dna_pcr_test_result": "164461AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "rapid_hiv_antibody_test_result_at_18_mths": "164860AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "art_initiation_status": "6e62bf7e-2107-4d09-b485-6e60cbbb2d08",
        "hiv_exposure_status": "6027869c-5d7e-4a82-b22f-6d9c57d61a4d",
        "ctx_prophylaxis_status": "f3de6eb3-5d4a-43ca-8648-74649271238c",
        "infant_hiv_test": "ee8c0292-47f8-4c01-8b60-8ba13a560e1a",
        "confirmatory_test_performed_on_this_vist": "8c2b3506-5b77-4916-a5c8-677a37a65007",
        "linked_to_art": "a40d8bc4-56b8-4f28-a1dd-412da5cf20ed",
        "missing_art_number": "43cb14fe-6f06-4b40-81f0-a712b805a74d"
      }
},
  {
  "report_name": "PMTCT ANC visit",
  "flat_table_name": "mamba_flat_encounter_pmtct_anc",
  "encounter_type_uuid": "2549af50-75c8-4aeb-87ca-4bb2cef6c69a",
  "concepts_locale": "en",
  "table_columns": {
    "hiv_test_result": "159427AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hiv_test_result_negative": "664AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hiv_test_result_positive": "703AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hiv_test_result_indeterminate": "1138AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "parity": "1053AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_last_menstrual_period": "1427AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "return_visit_date": "5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "estimated_date_of_delivery": "5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "gravida": "5624AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "return_anc_visit": "160530AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "partner_hiv_tested": "161557AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "new_anc_visit": "164180AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "visit_type": "164181AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hiv_test_performed": "164401AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "previously_known_positive": "8b8951a8-e8d6-40ca-ad70-89e8f8f71fa8",
    "tested_for_hiv_during_this_visit": "6f041992-f0fd-4ec7-b7b6-c06b0f60bf3f",
    "not_tested_for_hiv_during_this_visit": "d18fa331-f158-47d0-b344-cf147c7125a4",
    "facility_of_next_appointment": "efc87cd5-2fd8-411c-ba52-b0d858f541e7",
    "missing": "54b96458-6585-4c4c-a5b1-b3ca7f1be351",
    "ptracker_id": "6c45421e-2566-47cb-bbb3-07586fffbfe2"
  }
},
  {
  "report_name": "LaborandDelivery_Register",
  "flat_table_name": "mamba_flat_encounter_pmtct_labor_delivery",
  "encounter_type_uuid": "2678423c-0523-4d76-b0da-18177b439eed" ,
  "concepts_locale": "en",
  "table_columns": {
          "arv_prophylaxis_status": "1148AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "infant_feeding_method": "1151AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "viral_load_results": "1305AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "partners_hiv_status": "1436AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "number_of_births_from_current_pregnancy": "1568AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "child_gender": "1587AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "antenatal_card_present": "1719AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "mothers_health_status": "1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "labor_delivery_child_1": "8fe7ad7a-494d-4799-bf72-9f58fbdae221",
          "labor_delivery_child_2": "8fe7ad7a-494d-4799-bf72-9f58fbdae222",
          "delivery_outcome": "125872AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "result_of_hiv_test": "159427AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "art_start_date": "159599AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "reason_for_declining_hiv_test": "159803AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "qualitative_birth_outcome": "159917AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "partner_hiv_tested": "161557AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "viral_load_test_done": "163310AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "hiv_test_performed": "164401AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "infants_date_of_birth": "164802AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "infants_medical_record_number": "164803AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "art_initiation_status": "6e62bf7e-2107-4d09-b485-6e60cbbb2d08",
          "facility_of_next_appointment": "efc87cd5-2fd8-411c-ba52-b0d858f541e7",
          "missing_art_number": "43cb14fe-6f06-4b40-81f0-a712b805a74d",
          "anc_hiv_status_first_visit": "c5f74c86-62cd-4d22-9260-4238f1e45fe0",
          "child_two_gender": "1587AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "child_two_delivery_outcome": "125872AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
  }
},
  {
  "report_name": "MotherPostnatal_Register",
  "flat_table_name": "mamba_flat_encounter_pmtct_mother_postnatal",
  "encounter_type_uuid": "af1f1b24-d2e8-4282-b308-0bf79b365584" ,
  "concepts_locale": "en",
  "table_columns": {
        "viral_load_results": "1305AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "partners_hiv_status": "1436AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "return_visit_date": "5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "result_of_hiv_test": "159427AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "transferred_out_to": "159495AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "partner_hiv_tested": "161557AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "viral_load_test_done": "163310AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "hiv_test_performed": "164401AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "art_initiation_status": "6e62bf7e-2107-4d09-b485-6e60cbbb2d08",
        "facility_of_next_appointment": "efc87cd5-2fd8-411c-ba52-b0d858f541e7",
        "missing_reason_for_refusing_art_initiation": "0117ec63-6fc8-4b37-99e9-7f6d99652852"
    }
}]}';

  CALL sp_mamba_extract_report_metadata(@report_data, 'mamba_dim_concept_metadata');

  -- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_metadata_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata_update;

~
CREATE PROCEDURE sp_mamba_dim_concept_metadata_update()
BEGIN
-- $BEGIN

-- Update the Concept datatypes, concept_name and concept_id based on given locale
UPDATE mamba_dim_concept_metadata md
    INNER JOIN mamba_dim_concept c
    ON md.concept_uuid = c.uuid
    INNER JOIN mamba_dim_concept_name cn
    ON c.concept_id = cn.concept_id
SET md.concept_datatype = c.datatype,
    md.concept_id       = c.concept_id,
    md.concept_name     = cn.name
WHERE md.id > 0
  AND cn.locale = md.concepts_locale
  AND IF(cn.locale_preferred = 1, cn.locale_preferred = 1, cn.concept_name_type = 'FULLY_SPECIFIED');

-- Use locale preferred or Fully specified name

-- Update to True if this field is an obs answer to an obs Question
UPDATE mamba_dim_concept_metadata md
    INNER JOIN mamba_dim_concept_answer ca
    ON md.concept_id = ca.answer_concept
SET md.concept_answer_obs = 1
WHERE md.id > 0;

-- Update row number
UPDATE mamba_dim_concept_metadata md
INNER JOIN (
    SELECT
        id,
        ROW_NUMBER()  OVER (PARTITION BY  flat_table_name,concept_id ORDER BY id ASC) num
    FROM mamba_dim_concept_metadata)m
ON md.id = m.id
SET md.row_num = num
WHERE md.id > 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_metadata  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata;

~
CREATE PROCEDURE sp_mamba_dim_concept_metadata()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_metadata_create();
CALL sp_mamba_dim_concept_metadata_insert();
CALL sp_mamba_dim_concept_metadata_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_create;

~
CREATE PROCEDURE sp_mamba_dim_person_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_person
(
    id                  INT      NOT NULL AUTO_INCREMENT,
    person_id           INT      NOT NULL,
    birthdate           DATE NULL,
    birthdate_estimated TINYINT  NOT NULL,
    age                 INT NULL,
    dead                TINYINT  NOT NULL,
    death_date          DATETIME NULL,
    deathdate_estimated TINYINT  NOT NULL,
    gender              VARCHAR(255) NULL,
    date_created        DATETIME NOT NULL,
    person_name_short   VARCHAR(255) NULL,
    person_name_long    TEXT NULL,
    uuid                CHAR(38) NOT NULL,
    voided              TINYINT  NOT NULL,

    PRIMARY KEY (id)
) CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_person_person_id_index
    ON mamba_dim_person (person_id);

CREATE INDEX mamba_dim_person_uuid_index
    ON mamba_dim_person (uuid);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_insert;

~
CREATE PROCEDURE sp_mamba_dim_person_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_person
    (
        person_id,
        birthdate,
        birthdate_estimated,
        age,
        dead,
        death_date,
        deathdate_estimated,
        gender,
        date_created,
        uuid,
        voided
    )

    SELECT psn.person_id,
           psn.birthdate,
           psn.birthdate_estimated,
           fn_mamba_age_calculator(birthdate,death_date) AS age,
           psn.dead,
           psn.death_date,
           psn.deathdate_estimated,
           psn.gender,
           psn.date_created,
           psn.uuid,
           psn.voided
    FROM person psn;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_update;

~
CREATE PROCEDURE sp_mamba_dim_person_update()
BEGIN
-- $BEGIN
UPDATE mamba_dim_person dp
    INNER JOIN person psn  on psn.person_id = dp.person_id
    INNER JOIN  person_name pn on psn.person_id = pn.person_id
    SET   person_name_short = CONCAT_WS(' ',prefix,given_name,middle_name,family_name),
        person_name_long = CONCAT_WS(' ',prefix,given_name, middle_name,family_name_prefix, family_name,family_name2,family_name_suffix, degree)
WHERE  pn.preferred=1
;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person;

~
CREATE PROCEDURE sp_mamba_dim_person()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_person_create();
CALL sp_mamba_dim_person_insert();
CALL sp_mamba_dim_person_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_create;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_patient_identifier
(
    id                    INT         NOT NULL AUTO_INCREMENT,
    patient_identifier_id INT,
    patient_id            INT         NOT NULL,
    identifier            VARCHAR(50) NOT NULL,
    identifier_type       INT         NOT NULL,
    preferred             TINYINT     NOT NULL,
    location_id           INT         NULL,
    date_created          DATETIME    NOT NULL,
    uuid                  CHAR(38)    NOT NULL,
    voided                TINYINT     NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_patient_identifier_patient_identifier_id_index
    ON mamba_dim_patient_identifier (patient_identifier_id);

CREATE INDEX mamba_dim_patient_identifier_patient_id_index
    ON mamba_dim_patient_identifier (patient_id);

CREATE INDEX mamba_dim_patient_identifier_identifier_index
    ON mamba_dim_patient_identifier (identifier);

CREATE INDEX mamba_dim_patient_identifier_identifier_type_index
    ON mamba_dim_patient_identifier (identifier_type);

CREATE INDEX mamba_dim_patient_identifier_uuid_index
    ON mamba_dim_patient_identifier (uuid);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_insert;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_patient_identifier (patient_id,
                                          identifier,
                                          identifier_type,
                                          preferred,
                                          location_id,
                                          date_created,
                                          uuid,
                                          voided)
SELECT patient_id,
       identifier,
       identifier_type,
       preferred,
       location_id,
       date_created,
       uuid,
       voided
FROM patient_identifier;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_update;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_update()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_patient_identifier_create();
CALL sp_mamba_dim_patient_identifier_insert();
CALL sp_mamba_dim_patient_identifier_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_create;

~
CREATE PROCEDURE sp_mamba_dim_person_name_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_person_name
(
    id                 INT         NOT NULL AUTO_INCREMENT,
    person_name_id     INT         NOT NULL,
    person_id          INT         NOT NULL,
    preferred          TINYINT  NOT NULL,
    prefix             VARCHAR(50) NULL,
    given_name         VARCHAR(50) NULL,
    middle_name        VARCHAR(50) NULL,
    family_name_prefix VARCHAR(50) NULL,
    family_name        VARCHAR(50) NULL,
    family_name2       VARCHAR(50) NULL,
    family_name_suffix VARCHAR(50) NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_person_name_person_name_id_index
    ON mamba_dim_person_name (person_name_id);

CREATE INDEX mamba_dim_person_name_person_id_index
    ON mamba_dim_person_name (person_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_insert;

~
CREATE PROCEDURE sp_mamba_dim_person_name_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_person_name
    (
        person_name_id,
        person_id,
        preferred,
        prefix,
        given_name,
        middle_name,
        family_name_prefix,
        family_name,
        family_name2,
        family_name_suffix
    )
    SELECT
        pn.person_name_id,
        pn.person_id,
        pn.preferred,
        pn.prefix,
        pn.given_name,
        pn.middle_name,
        pn.family_name_prefix,
        pn.family_name,
        pn.family_name2,
        pn.family_name_suffix
    FROM
        person_name pn;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name;

~
CREATE PROCEDURE sp_mamba_dim_person_name()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_person_name_create();
CALL sp_mamba_dim_person_name_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_create;

~
CREATE PROCEDURE sp_mamba_dim_person_address_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_person_address
(
    id                INT          NOT NULL AUTO_INCREMENT,
    person_address_id INT          NOT NULL,
    person_id         INT          NULL,
    preferred         TINYINT      NOT NULL,
    address1          VARCHAR(255) NULL,
    address2          VARCHAR(255) NULL,
    address3          VARCHAR(255) NULL,
    address4          VARCHAR(255) NULL,
    address5          VARCHAR(255) NULL,
    address6          VARCHAR(255) NULL,
    city_village      VARCHAR(255) NULL,
    county_district   VARCHAR(255) NULL,
    state_province    VARCHAR(255) NULL,
    postal_code       VARCHAR(50)  NULL,
    country           VARCHAR(50)  NULL,
    latitude          VARCHAR(50)  NULL,
    longitude         VARCHAR(50)  NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_person_address_person_address_id_index
    ON mamba_dim_person_address (person_address_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_insert;

~
CREATE PROCEDURE sp_mamba_dim_person_address_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_person_address (person_address_id,
                                      person_id,
                                      preferred,
                                      address1,
                                      address2,
                                      address3,
                                      address4,
                                      address5,
                                      address6,
                                      city_village,
                                      county_district,
                                      state_province,
                                      postal_code,
                                      country,
                                      latitude,
                                      longitude)
SELECT person_address_id,
       person_id,
       preferred,
       address1,
       address2,
       address3,
       address4,
       address5,
       address6,
       city_village,
       county_district,
       state_province,
       postal_code,
       country,
       latitude,
       longitude
FROM person_address;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address;

~
CREATE PROCEDURE sp_mamba_dim_person_address()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_person_address_create();
CALL sp_mamba_dim_person_address_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_user_create;

~
CREATE PROCEDURE sp_mamba_dim_user_create()
BEGIN
-- $BEGIN
    CREATE TABLE mamba_dim_users
    (
        id            INT          NOT NULL AUTO_INCREMENT,
        user_id       INT          NOT NULL,
        system_id     VARCHAR(50)  NOT NULL,
        username      VARCHAR(50)  NULL,
        creator       INT          NOT NULL,
        date_created  DATETIME     NOT NULL,
        changed_by    INT          NULL,
        date_changed  DATETIME     NULL,
        person_id     INT          NOT NULL,
        retired       TINYINT(1)   NOT NULL,
        retired_by    INT          NULL,
        date_retired  DATETIME     NULL,
        retire_reason VARCHAR(255) NULL,
        uuid          CHAR(38)     NOT NULL,
        email         VARCHAR(255) NULL,

        PRIMARY KEY (id)
    )
        CHARSET = UTF8MB4;

    CREATE INDEX mamba_dim_users_user_id_index
        ON mamba_dim_users (user_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_user_insert;

~
CREATE PROCEDURE sp_mamba_dim_user_insert()
BEGIN
-- $BEGIN
    INSERT INTO mamba_dim_users
        (
            user_id,
            system_id,
            username,
            creator,
            date_created,
            changed_by,
            date_changed,
            person_id,
            retired,
            retired_by,
            date_retired,
            retire_reason,
            uuid,
            email
        )
        SELECT
            user_id,
            system_id,
            username,
            creator,
            date_created,
            changed_by,
            date_changed,
            person_id,
            retired,
            retired_by,
            date_retired,
            retire_reason,
            uuid,
            email
        FROM users c;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_user_update;

~
CREATE PROCEDURE sp_mamba_dim_user_update()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_user;

~
CREATE PROCEDURE sp_mamba_dim_user()
BEGIN
-- $BEGIN
    CALL sp_mamba_dim_user_create();
    CALL sp_mamba_dim_user_insert();
    CALL sp_mamba_dim_user_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_create;

~
CREATE PROCEDURE sp_mamba_dim_relationship_create()
BEGIN
-- $BEGIN
CREATE TABLE mamba_dim_relationship
(
    relationship_id INT                  NOT NULL AUTO_INCREMENT,
    person_a        INT                  NOT NULL,
    relationship    INT                  NOT NULL,
    person_b        INT                  NOT NULL,
    start_date      DATETIME             NULL,
    end_date        DATETIME             NULL,
    creator         INT                  NOT NULL,
    date_created    DATETIME             NOT NULL,
    date_changed    DATETIME             NULL,
    changed_by      INT                  NULL,
    voided          TINYINT(1)           NOT NULL ,
    voided_by       INT                  NULL,
    date_voided     DATETIME             NULL,
    void_reason     VARCHAR(255)         NULL,
    uuid            CHAR(38)             NOT NULL,

    PRIMARY KEY (relationship_id)
)

    CHARSET = UTF8MB3;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_insert;

~
CREATE PROCEDURE sp_mamba_dim_relationship_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_relationship
    (
        relationship_id,
        person_a,
        relationship,
        person_b,
        start_date,
        end_date,
        creator,
        date_created,
        date_changed,
        changed_by,
        voided,
        voided_by,
        date_voided,
        void_reason,
        uuid
    )
SELECT
    relationship_id,
    person_a,
    relationship,
    person_b,
    start_date,
    end_date,
    creator,
    date_created,
    date_changed,
    changed_by,
    voided,
    voided_by,
    date_voided,
    void_reason,
    uuid
FROM relationship;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_update;

~
CREATE PROCEDURE sp_mamba_dim_relationship_update()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship;

~
CREATE PROCEDURE sp_mamba_dim_relationship()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_relationship_create();
CALL sp_mamba_dim_relationship_insert();
CALL sp_mamba_dim_relationship_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_create;

~
CREATE PROCEDURE sp_mamba_dim_agegroup_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_agegroup
(
    id              INT         NOT NULL AUTO_INCREMENT,
    age             INT         NULL,
    datim_agegroup  VARCHAR(50) NULL,
    datim_age_val   INT         NULL,
    normal_agegroup VARCHAR(50) NULL,
    normal_age_val   INT        NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_insert;

~
CREATE PROCEDURE sp_mamba_dim_agegroup_insert()
BEGIN
-- $BEGIN

-- Enter unknown dimension value (in case a person's date of birth is unknown)
CALL sp_mamba_load_agegroup();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_update;

~
CREATE PROCEDURE sp_mamba_dim_agegroup_update()
BEGIN
-- $BEGIN

-- update age_value b
UPDATE mamba_dim_agegroup a
SET datim_age_val =
    CASE
        WHEN a.datim_agegroup = '<1' THEN 1
        WHEN a.datim_agegroup = '1-4' THEN 2
        WHEN a.datim_agegroup = '5-9' THEN 3
        WHEN a.datim_agegroup = '10-14' THEN 4
        WHEN a.datim_agegroup = '15-19' THEN 5
        WHEN a.datim_agegroup = '20-24' THEN 6
        WHEN a.datim_agegroup = '25-29' THEN 7
        WHEN a.datim_agegroup = '30-34' THEN 8
        WHEN a.datim_agegroup = '35-39' THEN 9
        WHEN a.datim_agegroup = '40-44' THEN 10
        WHEN a.datim_agegroup = '45-49' THEN 11
        WHEN a.datim_agegroup = '50-54' THEN 12
        WHEN a.datim_agegroup = '55-59' THEN 13
        WHEN a.datim_agegroup = '60-64' THEN 14
        WHEN a.datim_agegroup = '65+' THEN 15
    END
WHERE a.datim_agegroup IS NOT NULL;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup;

~
CREATE PROCEDURE sp_mamba_dim_agegroup()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_agegroup_create();
CALL sp_mamba_dim_agegroup_insert();
CALL sp_mamba_dim_agegroup_update();
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_create;

~
CREATE PROCEDURE sp_mamba_z_encounter_obs_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_z_encounter_obs
(
    id                      INT           NOT NULL AUTO_INCREMENT,
    encounter_id            INT           NULL,
    person_id               INT           NOT NULL,
    encounter_datetime      DATETIME      NOT NULL,
    obs_datetime            DATETIME      NOT NULL,
    obs_question_concept_id INT DEFAULT 0 NOT NULL,
    obs_value_text          TEXT          NULL,
    obs_value_numeric       DOUBLE        NULL,
    obs_value_coded         INT           NULL,
    obs_value_datetime      DATETIME      NULL,
    obs_value_complex       VARCHAR(1000) NULL,
    obs_value_drug          INT           NULL,
    obs_question_uuid       CHAR(38),
    obs_answer_uuid         CHAR(38),
    obs_value_coded_uuid    CHAR(38),
    encounter_type_uuid     CHAR(38),
    status                  VARCHAR(16)   NOT NULL,
    voided                  TINYINT       NOT NULL,
    row_num                 INT           NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_z_encounter_obs_encounter_id_type_uuid_person_id_index
    ON mamba_z_encounter_obs (encounter_id, encounter_type_uuid, person_id);

CREATE INDEX mamba_z_encounter_obs_encounter_type_uuid_index
    ON mamba_z_encounter_obs (encounter_type_uuid);

CREATE INDEX mamba_z_encounter_obs_question_concept_id_index
    ON mamba_z_encounter_obs (obs_question_concept_id);

CREATE INDEX mamba_z_encounter_obs_value_coded_index
    ON mamba_z_encounter_obs (obs_value_coded);

CREATE INDEX mamba_z_encounter_obs_value_coded_uuid_index
    ON mamba_z_encounter_obs (obs_value_coded_uuid);

CREATE INDEX mamba_z_encounter_obs_question_uuid_index
    ON mamba_z_encounter_obs (obs_question_uuid);

CREATE INDEX mamba_z_encounter_obs_status_index
    ON mamba_z_encounter_obs (status);

CREATE INDEX mamba_z_encounter_obs_voided_index
    ON mamba_z_encounter_obs (voided);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_insert;

~
CREATE PROCEDURE sp_mamba_z_encounter_obs_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_z_encounter_obs
    (
        encounter_id,
        person_id,
        obs_datetime,
        encounter_datetime,
        encounter_type_uuid,
        obs_question_concept_id,
        obs_value_text,
        obs_value_numeric,
        obs_value_coded,
        obs_value_datetime,
        obs_value_complex,
        obs_value_drug,
        obs_question_uuid,
        obs_answer_uuid,
        obs_value_coded_uuid,
        status,
        voided,
        row_num
    )
    SELECT o.encounter_id,
           o.person_id,
           o.obs_datetime,
           e.encounter_datetime,
           e.encounter_type_uuid,
           o.concept_id     AS obs_question_concept_id,
           o.value_text     AS obs_value_text,
           o.value_numeric  AS obs_value_numeric,
           o.value_coded    AS obs_value_coded,
           o.value_datetime AS obs_value_datetime,
           o.value_complex  AS obs_value_complex,
           o.value_drug     AS obs_value_drug,
           NULL             AS obs_question_uuid,
           NULL             AS obs_answer_uuid,
           NULL             AS obs_value_coded_uuid,
           o.status,
           o.voided,
           ROW_NUMBER()OVER(PARTITION BY person_id,encounter_id,concept_id)
    FROM obs o
             INNER JOIN mamba_dim_encounter e
                        ON o.encounter_id = e.encounter_id
    WHERE o.encounter_id IS NOT NULL;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_update;

~
CREATE PROCEDURE sp_mamba_z_encounter_obs_update()
BEGIN
-- $BEGIN

-- update obs question UUIDs
UPDATE mamba_z_encounter_obs z
    INNER JOIN mamba_dim_concept_metadata md
    ON z.obs_question_concept_id = md.concept_id
SET z.obs_question_uuid = md.concept_uuid
WHERE TRUE;

-- update obs_value_coded (UUIDs & Concept value names)
UPDATE mamba_z_encounter_obs z
    INNER JOIN mamba_dim_concept_name md
    ON z.obs_value_coded = md.concept_id
    INNER JOIN mamba_dim_concept c
    ON c.concept_id = md.concept_id
SET z.obs_value_text       = md.name,
    z.obs_value_coded_uuid = c.uuid
WHERE z.obs_value_coded IS NOT NULL;


-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs;

~
CREATE PROCEDURE sp_mamba_z_encounter_obs()
BEGIN
-- $BEGIN

CALL sp_mamba_z_encounter_obs_create();
CALL sp_mamba_z_encounter_obs_insert();
CALL sp_mamba_z_encounter_obs_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_flatten  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_data_processing_flatten;

~
CREATE PROCEDURE sp_mamba_data_processing_flatten()
BEGIN
-- $BEGIN
-- CALL sp_xf_system_drop_all_tables_in_schema($target_database);
CALL sp_xf_system_drop_all_tables_in_schema();

CALL sp_mamba_dim_location;

CALL sp_mamba_dim_patient_identifier_type;

CALL sp_mamba_dim_concept_datatype;

CALL sp_mamba_dim_concept_answer;

CALL sp_mamba_dim_concept_name;

CALL sp_mamba_dim_concept;

CALL sp_mamba_dim_concept_metadata;

CALL sp_mamba_dim_encounter_type;

CALL sp_mamba_dim_encounter;

CALL sp_mamba_dim_person;

CALL sp_mamba_dim_person_name;

CALL sp_mamba_dim_person_address;

CALL sp_mamba_dim_user;

CALL sp_mamba_dim_relationship;

CALL sp_mamba_dim_patient_identifier;

CALL sp_mamba_dim_agegroup;

CALL sp_mamba_z_encounter_obs;

CALL sp_mamba_flat_encounter_table_create_all;

CALL sp_mamba_flat_encounter_table_insert_all;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_pmtct  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_pmtct;

~
CREATE PROCEDURE sp_mamba_data_processing_derived_pmtct()
BEGIN
-- $BEGIN
CALL sp_mamba_fact_exposedinfants;
CALL sp_mamba_fact_pregnant_women;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_etl  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_data_processing_etl;

~
CREATE PROCEDURE sp_mamba_data_processing_etl()
BEGIN
-- $BEGIN
-- add base folder SP here --

-- Flatten the tables first
CALL sp_mamba_data_processing_flatten();

-- Call the ETL process
CALL sp_mamba_data_processing_derived_pmtct();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_covid_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_client_covid_create;

~
CREATE PROCEDURE sp_mamba_dim_client_covid_create()
BEGIN
-- $BEGIN
CREATE TABLE dim_client_covid
(
    id            INT auto_increment,
    client_id     INT           NULL,
    date_of_birth DATE          NULL,
    ageattest     INT           NULL,
    sex           NVARCHAR(50)  NULL,
    county        NVARCHAR(255) NULL,
    sub_county    NVARCHAR(255) NULL,
    ward          NVARCHAR(255) NULL,
    PRIMARY KEY (id)
);
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_covid_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_client_covid_insert;

~
CREATE PROCEDURE sp_mamba_dim_client_covid_insert()
BEGIN
-- $BEGIN
INSERT INTO dim_client_covid
    (
        client_id,
        date_of_birth,
        ageattest,
        sex,
        county,
        sub_county,
        ward
    )
    SELECT
        c.client_id,
        date_of_birth,
        FLOOR(DATEDIFF(CAST(cd.order_date AS DATE), CAST(date_of_birth as DATE)) / 365) AS ageattest,
        sex,
        county,
        sub_county,
        ward
    FROM
        mamba_dim_client c
    INNER JOIN
        mamba_flat_encounter_covid cd
            ON c.client_id = cd.client_id;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_covid_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_client_covid_update;

~
CREATE PROCEDURE sp_mamba_dim_client_covid_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_covid  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_client_covid;

~
CREATE PROCEDURE sp_mamba_dim_client_covid()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_client_covid_create();
CALL sp_mamba_dim_client_covid_insert();
CALL sp_mamba_dim_client_covid_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_covid_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_covid_create;

~
CREATE PROCEDURE sp_mamba_fact_encounter_covid_create()
BEGIN
-- $BEGIN
CREATE TABLE IF NOT EXISTS fact_encounter_covid
(
    encounter_id                      INT           NULL,
    client_id                         INT           NULL,
    covid_test                        NVARCHAR(255) NULL,
    order_date                        DATE          NULL,
    result_date                       DATE          NULL,
    date_assessment                   DATE          NULL,
    assessment_presentation           NVARCHAR(255) NULL,
    assessment_contact_case           INT           NULL,
    assessment_entry_country          INT           NULL,
    assessment_travel_out_country     INT           NULL,
    assessment_follow_up              INT           NULL,
    assessment_voluntary              INT           NULL,
    assessment_quarantine             INT           NULL,
    assessment_symptomatic            INT           NULL,
    assessment_surveillance           INT           NULL,
    assessment_health_worker          INT           NULL,
    assessment_frontline_worker       INT           NULL,
    assessment_rdt_confirmatory       INT           NULL,
    assessment_post_mortem            INT           NULL,
    assessment_other                  INT           NULL,
    date_onset_symptoms               DATE          NULL,
    symptom_cough                     INT           NULL,
    symptom_headache                  INT           NULL,
    symptom_red_eyes                  INT           NULL,
    symptom_sneezing                  INT           NULL,
    symptom_diarrhoea                 INT           NULL,
    symptom_sore_throat               INT           NULL,
    symptom_tiredness                 INT           NULL,
    symptom_chest_pain                INT           NULL,
    symptom_joint_pain                INT           NULL,
    symptom_loss_smell                INT           NULL,
    symptom_loss_taste                INT           NULL,
    symptom_runny_nose                INT           NULL,
    symptom_fever_chills              INT           NULL,
    symptom_muscular_pain             INT           NULL,
    symptom_general_weakness          INT           NULL,
    symptom_shortness_breath          INT           NULL,
    symptom_nausea_vomiting           INT           NULL,
    symptom_abdominal_pain            INT           NULL,
    symptom_irritability_confusion    INT           NULL,
    symptom_disturbance_consciousness INT           NULL,
    symptom_other                     INT           NULL,
    comorbidity_present               INT           NULL,
    comorbidity_tb                    INT           NULL,
    comorbidity_liver                 INT           NULL,
    comorbidity_renal                 INT           NULL,
    comorbidity_diabetes              INT           NULL,
    comorbidity_hiv_aids              INT           NULL,
    comorbidity_malignancy            INT           NULL,
    comorbidity_chronic_lung          INT           NULL,
    comorbidity_hypertension          INT           NULL,
    comorbidity_former_smoker         INT           NULL,
    comorbidity_cardiovascular        INT           NULL,
    comorbidity_current_smoker        INT           NULL,
    comorbidity_immunodeficiency      INT           NULL,
    comorbidity_chronic_neurological  INT           NULL,
    comorbidity_other                 INT           NULL,
    diagnostic_pcr_test               NVARCHAR(255) NULL,
    diagnostic_pcr_result             NVARCHAR(255) NULL,
    rapid_antigen_test                NVARCHAR(255) NULL,
    rapid_antigen_result              NVARCHAR(255) NULL,
    long_covid_description            NVARCHAR(255) NULL,
    patient_outcome                   NVARCHAR(255) NULL,
    date_recovered                    DATE          NULL,
    date_died                         DATE          NULL
);
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_covid_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_covid_insert;

~
CREATE PROCEDURE sp_mamba_fact_encounter_covid_insert()
BEGIN
-- $BEGIN
INSERT INTO fact_encounter_covid (encounter_id,
                                  client_id,
                                  covid_test,
                                  order_date,
                                  result_date,
                                  date_assessment,
                                  assessment_presentation,
                                  assessment_contact_case,
                                  assessment_entry_country,
                                  assessment_travel_out_country,
                                  assessment_follow_up,
                                  assessment_voluntary,
                                  assessment_quarantine,
                                  assessment_symptomatic,
                                  assessment_surveillance,
                                  assessment_health_worker,
                                  assessment_frontline_worker,
                                  assessment_rdt_confirmatory,
                                  assessment_post_mortem,
                                  assessment_other,
                                  date_onset_symptoms,
                                  symptom_cough,
                                  symptom_headache,
                                  symptom_red_eyes,
                                  symptom_sneezing,
                                  symptom_diarrhoea,
                                  symptom_sore_throat,
                                  symptom_tiredness,
                                  symptom_chest_pain,
                                  symptom_joint_pain,
                                  symptom_loss_smell,
                                  symptom_loss_taste,
                                  symptom_runny_nose,
                                  symptom_fever_chills,
                                  symptom_muscular_pain,
                                  symptom_general_weakness,
                                  symptom_shortness_breath,
                                  symptom_nausea_vomiting,
                                  symptom_abdominal_pain,
                                  symptom_irritability_confusion,
                                  symptom_disturbance_consciousness,
                                  symptom_other,
                                  comorbidity_present,
                                  comorbidity_tb,
                                  comorbidity_liver,
                                  comorbidity_renal,
                                  comorbidity_diabetes,
                                  comorbidity_hiv_aids,
                                  comorbidity_malignancy,
                                  comorbidity_chronic_lung,
                                  comorbidity_hypertension,
                                  comorbidity_former_smoker,
                                  comorbidity_cardiovascular,
                                  comorbidity_current_smoker,
                                  comorbidity_immunodeficiency,
                                  comorbidity_chronic_neurological,
                                  comorbidity_other,
                                  diagnostic_pcr_test,
                                  diagnostic_pcr_result,
                                  rapid_antigen_test,
                                  rapid_antigen_result,
                                  long_covid_description,
                                  patient_outcome,
                                  date_recovered,
                                  date_died)
SELECT encounter_id,
       client_id,
       covid_test,
       cast(order_date AS DATE)          order_date,
       cast(result_date AS DATE)         result_date,
       cast(date_assessment AS DATE)     date_assessment,
       assessment_presentation,
       assessment_contact_case,
       assessment_entry_country,
       assessment_travel_out_country,
       assessment_follow_up,
       assessment_voluntary,
       assessment_quarantine,
       assessment_symptomatic,
       assessment_surveillance,
       assessment_health_worker,
       assessment_frontline_worker,
       assessment_rdt_confirmatory,
       assessment_post_mortem,
       assessment_other,
       cast(date_onset_symptoms AS DATE) date_onset_symptoms,
       symptom_cough,
       symptom_headache,
       symptom_red_eyes,
       symptom_sneezing,
       symptom_diarrhoea,
       symptom_sore_throat,
       symptom_tiredness,
       symptom_chest_pain,
       symptom_joint_pain,
       symptom_loss_smell,
       symptom_loss_taste,
       symptom_runny_nose,
       symptom_fever_chills,
       symptom_muscular_pain,
       symptom_general_weakness,
       symptom_shortness_breath,
       symptom_nausea_vomiting,
       symptom_abdominal_pain,
       symptom_irritability_confusion,
       symptom_disturbance_consciousness,
       symptom_other,
       CASE
           WHEN comorbidity_present IN ('Yes', 'True') THEN 1
           WHEN comorbidity_present IN ('False', 'No') THEN 0
           END AS                        comorbidity_present,
       comorbidity_tb,
       comorbidity_liver,
       comorbidity_renal,
       comorbidity_diabetes,
       comorbidity_hiv_aids,
       comorbidity_malignancy,
       comorbidity_chronic_lung,
       comorbidity_hypertension,
       comorbidity_former_smoker,
       comorbidity_cardiovascular,
       comorbidity_current_smoker,
       comorbidity_immunodeficiency,
       comorbidity_chronic_neurological,
       comorbidity_other,
       diagnostic_pcr_test,
       diagnostic_pcr_result,
       rapid_antigen_test,
       rapid_antigen_result,
       long_covid_description,
       patient_outcome,
       cast(date_recovered AS DATE)      date_recovered,
       cast(date_died AS DATE)           date_died
FROM flat_encounter_covid;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_covid_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_covid_update;

~
CREATE PROCEDURE sp_mamba_fact_encounter_covid_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_covid  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_covid;

~
CREATE PROCEDURE sp_mamba_fact_encounter_covid()
BEGIN
-- $BEGIN
CALL sp_mamba_fact_encounter_covid_create();
CALL sp_mamba_fact_encounter_covid_insert();
CALL sp_mamba_fact_encounter_covid_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_covid  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_covid;

~
CREATE PROCEDURE sp_mamba_data_processing_derived_covid()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_client_covid;
CALL sp_mamba_fact_encounter_covid;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_hts_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_client_hts_create;

~
CREATE PROCEDURE sp_mamba_dim_client_hts_create()
BEGIN
-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_dim_client_hts
(
    id            INT           NOT NULL AUTO_INCREMENT,
    client_id     INT           NOT NULL,
    date_of_birth DATE          NULL,
    age_at_test   INT           NULL,
    sex           NVARCHAR(25)  NULL,
    county        NVARCHAR(255) NULL,
    sub_county    NVARCHAR(255) NULL,
    ward          NVARCHAR(255) NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_hts_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_client_hts_insert;

~
CREATE PROCEDURE sp_mamba_dim_client_hts_insert()
BEGIN
-- $BEGIN
INSERT INTO mamba_dim_client_hts
    (
        client_id,
        date_of_birth,
        age_at_test,
        sex,
        county,
        sub_county,
        ward
    )
    SELECT
        p.person_id AS client_id,
        birthdate AS date_of_birth,
        FLOOR(DATEDIFF(hts.date_test_conducted, birthdate) / 365) AS age_at_test,
        CASE `p`.`gender`
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE '_'
        END AS sex,
        pa.county_district AS county,
        pa.city_village AS sub_county,
        pa.address1 AS ward
    FROM
        mamba_dim_person p
    INNER JOIN
            mamba_flat_encounter_hts hts
                ON p.person_id = hts.client_id
    LEFT JOIN
            mamba_dim_person_address pa
                ON p.person_id = pa.person_id
;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_hts_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_client_hts_update;

~
CREATE PROCEDURE sp_mamba_dim_client_hts_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_hts  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_client_hts;

~
CREATE PROCEDURE sp_mamba_dim_client_hts()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_client_hts_create();
CALL sp_mamba_dim_client_hts_insert();
CALL sp_mamba_dim_client_hts_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_hts_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_hts_create;

~
CREATE PROCEDURE sp_mamba_fact_encounter_hts_create()
BEGIN
-- $BEGIN
CREATE TABLE mamba_fact_encounter_hts
(
    id                        INT AUTO_INCREMENT,
    encounter_id              INT           NULL,
    client_id                 INT           NULL,
    encounter_date            DATETIME          NULL,

    date_tested               DATE          NULL,
    consent                   NVARCHAR(7)   NULL,
    community_service_point   NVARCHAR(255) NULL,
    pop_type                  NVARCHAR(50)  NULL,
    keypop_category           NVARCHAR(50)  NULL,
    priority_pop              NVARCHAR(16)  NULL,
    test_setting              NVARCHAR(255) NULL,
    facility_service_point    NVARCHAR(255) NULL,
    hts_approach              NVARCHAR(255) NULL,
    pretest_counselling       NVARCHAR(255) NULL,
    type_pretest_counselling  NVARCHAR(255) NULL,
    reason_for_test           NVARCHAR(255) NULL,
    ever_tested_hiv           VARCHAR(7)    NULL,
    duration_since_last_test  NVARCHAR(255) NULL,
    couple_result             NVARCHAR(50)  NULL,
    result_received_couple    NVARCHAR(255) NULL,
    test_conducted            NVARCHAR(255) NULL,
    initial_kit_name          NVARCHAR(255) NULL,
    initial_test_result       NVARCHAR(50)  NULL,
    confirmatory_kit_name     NVARCHAR(255) NULL,
    last_test_result          NVARCHAR(50)  NULL,
    final_test_result         NVARCHAR(50)  NULL,
    given_result              VARCHAR(7)    NULL,
    date_given_result         DATE          NULL,
    tiebreaker_kit_name       NVARCHAR(255) NULL,
    tiebreaker_test_result    NVARCHAR(50)  NULL,
    sti_last_6mo              NVARCHAR(7)   NULL,
    sexually_active           NVARCHAR(255) NULL,
    syphilis_test_result      NVARCHAR(50)  NULL,
    unprotected_sex_last_12mo NVARCHAR(255) NULL,
    recency_consent           NVARCHAR(7)   NULL,
    recency_test_done         NVARCHAR(7)   NULL,
    recency_test_type         NVARCHAR(255) NULL,
    recency_vl_result         NVARCHAR(50)  NULL,
    recency_rtri_result       NVARCHAR(50)  NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_hts_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_hts_insert;

~
CREATE PROCEDURE sp_mamba_fact_encounter_hts_insert()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_hts_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_hts_update;

~
CREATE PROCEDURE sp_mamba_fact_encounter_hts_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_hts  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_hts;

~
CREATE PROCEDURE sp_mamba_fact_encounter_hts()
BEGIN
-- $BEGIN
CALL sp_mamba_fact_encounter_hts_create();
CALL sp_mamba_fact_encounter_hts_insert();
CALL sp_mamba_fact_encounter_hts_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_txcurr_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_txcurr_create;

~
CREATE PROCEDURE sp_mamba_fact_txcurr_create()
BEGIN
-- $BEGIN
CREATE TABLE mamba_fact_txcurr
(
    id                        INT AUTO_INCREMENT,
    encounter_id              INT           NULL,
    client_id                 INT           NULL,
    date_tested               DATE          NULL,
    consent                   NVARCHAR(7)   NULL,
    community_service_point   NVARCHAR(255) NULL,
    pop_type                  NVARCHAR(50)  NULL,
    keypop_category           NVARCHAR(50)  NULL,
    priority_pop              NVARCHAR(16)  NULL,
    test_setting              NVARCHAR(255) NULL,
    facility_service_point    NVARCHAR(255) NULL,
    hts_approach              NVARCHAR(255) NULL,
    pretest_counselling       NVARCHAR(255) NULL,
    type_pretest_counselling  NVARCHAR(255) NULL,
    reason_for_test           NVARCHAR(255) NULL,
    ever_tested_hiv           VARCHAR(7)    NULL,
    duration_since_last_test  NVARCHAR(255) NULL,
    couple_result             NVARCHAR(50)  NULL,
    result_received_couple    NVARCHAR(255) NULL,
    test_conducted            NVARCHAR(255) NULL,
    initial_kit_name          NVARCHAR(255) NULL,
    initial_test_result       NVARCHAR(50)  NULL,
    confirmatory_kit_name     NVARCHAR(255) NULL,
    last_test_result          NVARCHAR(50)  NULL,
    final_test_result         NVARCHAR(50)  NULL,
    given_result              VARCHAR(7)    NULL,
    date_given_result         DATE          NULL,
    tiebreaker_kit_name       NVARCHAR(255) NULL,
    tiebreaker_test_result    NVARCHAR(50)  NULL,
    sti_last_6mo              NVARCHAR(7)   NULL,
    sexually_active           NVARCHAR(255) NULL,
    syphilis_test_result      NVARCHAR(50)  NULL,
    unprotected_sex_last_12mo NVARCHAR(255) NULL,
    recency_consent           NVARCHAR(7)   NULL,
    recency_test_done         NVARCHAR(7)   NULL,
    recency_test_type         NVARCHAR(255) NULL,
    recency_vl_result         NVARCHAR(50)  NULL,
    recency_rtri_result       NVARCHAR(50)  NULL,
    PRIMARY KEY (id)
);
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_txcurr_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_txcurr_insert;

~
CREATE PROCEDURE sp_mamba_fact_txcurr_insert()
BEGIN
-- $BEGIN
INSERT INTO mamba_fact_txcurr (encounter_id,
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
FROM `flat_encounter_hts` `hts`;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_txcurr_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_txcurr_update;

~
CREATE PROCEDURE sp_mamba_fact_txcurr_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_txcurr  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_txcurr;

~
CREATE PROCEDURE sp_mamba_fact_txcurr()
BEGIN
-- $BEGIN
CALL sp_mamba_fact_txcurr_create();
CALL sp_mamba_fact_txcurr_insert();
CALL sp_mamba_fact_txcurr_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_txcurr_query  ----------------------------
-- ---------------------------------------------------------------------------------------------




        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_hts  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_hts;

~
CREATE PROCEDURE sp_mamba_data_processing_derived_hts()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_client_hts;
CALL sp_mamba_fact_encounter_hts;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_exposedinfants_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_exposedinfants_create;

~
CREATE PROCEDURE sp_mamba_fact_exposedinfants_create()
BEGIN
-- $BEGIN
CREATE TABLE mamba_fact_pmtct_exposedinfants
(

    encounter_id                              INT          NOT NULL,
    infant_client_id                          INT          NOT NULL,
    encounter_datetime                        DATE         NOT NULL,
    mother_client_id                          INT          NULL,
    visit_type                                VARCHAR(100) NULL,
    arv_adherence                             VARCHAR(100) NULL,
    linked_to_art                             VARCHAR(100) NULL,
    infant_hiv_test                           VARCHAR(100) NULL,
    hiv_test_performed                        VARCHAR(100) NULL,
    missing_art_number                        VARCHAR(100) NULL,
    result_of_hiv_test                        VARCHAR(100) NULL,
    viral_load_results                        VARCHAR(100) NULL,
    hiv_exposure_status                       VARCHAR(100) NULL,
    viral_load_test_done                      VARCHAR(100) NULL,
    art_initiation_status                     VARCHAR(100) NULL,
    arv_prophylaxis_status                    VARCHAR(100) NULL,
    ctx_prophylaxis_status                    VARCHAR(100) NULL,
    patient_outcome_status                    VARCHAR(100) NULL,
    cotrimoxazole_adherence                   VARCHAR(100) NULL,
    child_hiv_dna_pcr_test_result             VARCHAR(100) NULL,
    unique_antiretroviral_therapy_number      VARCHAR(100) NULL,
    confirmatory_test_performed_on_this_vist  VARCHAR(100) NULL,
    rapid_hiv_antibody_test_result_at_18_mths VARCHAR(100) NULL,

    PRIMARY KEY (encounter_id)
);
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_exposedinfants_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_exposedinfants_insert;

~
CREATE PROCEDURE sp_mamba_fact_exposedinfants_insert()
BEGIN
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
    missing_art_number,
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
        missing_art_number,
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
    LEFT JOIN relationship a ON  ip.client_id = a.person_b
    WHERE   (ip.client_id in (SELECT person_b FROM relationship a
                INNER JOIN mamba_flat_encounter_pmtct_anc anc
                    ON a.person_a = anc.client_id
                WHERE (anc.hiv_test_result ='HIV Positive'
                           OR anc.hiv_test_performed = 'Previously known positive'))
            OR ip.client_id in (SELECT person_b FROM relationship a
                INNER JOIN mamba_flat_encounter_pmtct_labor_delivery ld
                    ON a.person_a = ld.client_id
                where (ld.result_of_hiv_test ='HIV Positive'
                           OR ld.hiv_test_performed = 'Previously known positive'))
            OR ip.client_id in (SELECT person_b FROM relationship a
                INNER JOIN mamba_flat_encounter_pmtct_mother_postnatal mp
                    ON a.person_a = mp.client_id
                where (mp.result_of_hiv_test like '%Positive%'
                           OR mp.hiv_test_performed = 'Previously known positive')))
;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_exposedinfants_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_exposedinfants_update;

~
CREATE PROCEDURE sp_mamba_fact_exposedinfants_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_exposedinfants_query  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_exposedinfants_query;

~
CREATE PROCEDURE sp_mamba_fact_exposedinfants_query()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_exposedinfants  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_exposedinfants;

~
CREATE PROCEDURE sp_mamba_fact_exposedinfants()
BEGIN
-- $BEGIN
CALL sp_mamba_fact_exposedinfants_create();
CALL sp_mamba_fact_exposedinfants_insert();
CALL sp_mamba_fact_exposedinfants_update();
CALL sp_mamba_fact_exposedinfants_query();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_pregnant_women_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_pregnant_women_create;

~
CREATE PROCEDURE sp_mamba_fact_pregnant_women_create()
BEGIN
-- $BEGIN
create table mamba_fact_pmtct_pregnant_women
(
    encounter_id                         INT      NOT NULL,
    client_id                            INT      NOT NULL,
    encounter_datetime                   datetime NOT NULL,
    parity                               VARCHAR(100)     NULL,
    gravida                              VARCHAR(100)     NULL,
    missing                              VARCHAR(100)     NULL,
    visit_type                           VARCHAR(100)     NULL,
    ptracker_id                          VARCHAR(100)     NULL,
    new_anc_visit                        VARCHAR(100)     NULL,
    hiv_test_result                      VARCHAR(100)     NULL,
    return_anc_visit                     VARCHAR(100)     NULL,
    return_visit_date                    VARCHAR(100)     NULL,
    hiv_test_performed                   VARCHAR(100)     NULL,
    partner_hiv_tested                   VARCHAR(100)     NULL,
    hiv_test_result_negative             VARCHAR(100)     NULL,
    hiv_test_result_positive             VARCHAR(100)     NULL,
    previously_known_positive            VARCHAR(100)     NULL,
    estimated_date_of_delivery           VARCHAR(100)     NULL,
    facility_of_next_appointment         VARCHAR(100)     NULL,
    date_of_last_menstrual_period        VARCHAR(100)     NULL,
    hiv_test_result_indeterminate        VARCHAR(100)     NULL,
    tested_for_hiv_during_this_visit     VARCHAR(100)     NULL,
    not_tested_for_hiv_during_this_visit VARCHAR(100)     NULL
);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_pregnant_women_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_pregnant_women_insert;

~
CREATE PROCEDURE sp_mamba_fact_pregnant_women_insert()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_pregnant_women_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_pregnant_women_update;

~
CREATE PROCEDURE sp_mamba_fact_pregnant_women_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_pregnant_women_query  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_pregnant_women_query;

~
CREATE PROCEDURE sp_mamba_fact_pregnant_women_query()
BEGIN
-- $BEGIN


-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_pregnant_women  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_pregnant_women;

~
CREATE PROCEDURE sp_mamba_fact_pregnant_women()
BEGIN
-- $BEGIN
CALL sp_mamba_fact_pregnant_women_create();
CALL sp_mamba_fact_pregnant_women_insert();
CALL sp_mamba_fact_pregnant_women_update();
CALL sp_mamba_fact_pregnant_women_query();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_pmtct  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_pmtct;

~
CREATE PROCEDURE sp_mamba_data_processing_derived_pmtct()
BEGIN
-- $BEGIN
CALL sp_mamba_fact_exposedinfants;
CALL sp_mamba_fact_pregnant_women;
-- $END
END~


