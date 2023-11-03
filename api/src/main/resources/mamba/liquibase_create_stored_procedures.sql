USE analysis_db;

~

        
    
        
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
CREATE FUNCTION fn_mamba_age_calculator(birthdate DATE, deathDate DATE) RETURNS Integer
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
    DECLARE birthdateCheck VARCHAR(255) DEFAULT NULL;

    SET onDate = NULL;

    -- Check if birthdate is not null and not an empty string
    IF birthdate IS NULL OR TRIM(birthdate) = '' THEN
        RETURN NULL;
    ELSE
        SET today = CURDATE();

        -- Check if birthdate is a valid date using STR_TO_DATE &  -- Check if birthdate is not in the future
        SET birthdateCheck = STR_TO_DATE(birthdate, '%Y-%m-%d');
        IF birthdateCheck IS NULL OR birthdateCheck > today THEN
            RETURN NULL;
        END IF;

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
END~



        
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
                'CREATE TABLE `', flat_encounter_table_name, '` (encounter_id INT NOT NULL, client_id INT NOT NULL, encounter_datetime DATETIME NOT NULL, INDEX idx_encounter_id (encounter_id), INDEX idx_client_id (client_id), INDEX idx_encounter_datetime (encounter_datetime));');
    ELSE
        SET @create_table = CONCAT(
                'CREATE TABLE `', flat_encounter_table_name, '` (encounter_id INT NOT NULL, client_id INT NOT NULL, encounter_datetime DATETIME NOT NULL, ', @column_labels, ' TEXT, INDEX idx_encounter_id (encounter_id), INDEX idx_client_id (client_id), INDEX idx_encounter_datetime (encounter_datetime));');
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
-- ----------------------  sp_mamba_extract_report_definition_metadata  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_extract_report_definition_metadata;


~
CREATE PROCEDURE sp_mamba_extract_report_definition_metadata(
    IN report_definition_json JSON,
    IN metadata_table VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    IF report_definition_json IS NULL OR JSON_LENGTH(report_definition_json) = 0 THEN
        SIGNAL SQLSTATE '02000'
            SET MESSAGE_TEXT = 'Warn: report_definition_json is empty or null.';
    ELSE

        SET session group_concat_max_len = 20000;

        SELECT JSON_EXTRACT(report_definition_json, '$.report_definitions') INTO @report_array;
        SELECT JSON_LENGTH(@report_array) INTO @report_array_len;

        SET @report_count = 0;
        WHILE @report_count < @report_array_len
            DO

                SELECT JSON_EXTRACT(@report_array, CONCAT('$[', @report_count, ']')) INTO @report;
                SELECT JSON_UNQUOTE(JSON_EXTRACT(@report, '$.report_name')) INTO @report_name;
                SELECT JSON_UNQUOTE(JSON_EXTRACT(@report, '$.report_id')) INTO @report_id;
                SELECT CONCAT('sp_mamba_', @report_id, '_query') INTO @report_procedure_name;
                SELECT CONCAT('sp_mamba_', @report_id, '_columns_query') INTO @report_columns_procedure_name;
                SELECT CONCAT('mamba_dim_', @report_id) INTO @table_name;
                SELECT JSON_UNQUOTE(JSON_EXTRACT(@report, CONCAT('$.report_sql.sql_query'))) INTO @sql_query;
                SELECT JSON_EXTRACT(@report, CONCAT('$.report_sql.query_params')) INTO @query_params_array;

                INSERT INTO mamba_dim_report_definition(report_id,
                                                        report_procedure_name,
                                                        report_columns_procedure_name,
                                                        sql_query,
                                                        table_name,
                                                        report_name)
                VALUES (@report_id,
                        @report_procedure_name,
                        @report_columns_procedure_name,
                        @sql_query,
                        @table_name,
                        @report_name);

                -- Iterate over the "params" array for each report
                SELECT JSON_LENGTH(@query_params_array) INTO @total_params;

                SET @parameters := NULL;
                SET @param_count = 0;
                WHILE @param_count < @total_params
                    DO
                        SELECT JSON_EXTRACT(@query_params_array, CONCAT('$[', @param_count, ']')) INTO @param;
                        SELECT JSON_UNQUOTE(JSON_EXTRACT(@param, '$.name')) INTO @param_name;
                        SELECT JSON_UNQUOTE(JSON_EXTRACT(@param, '$.type')) INTO @param_type;
                        SET @param_position = @param_count + 1;

                        INSERT INTO mamba_dim_report_definition_parameters(report_id,
                                                                           parameter_name,
                                                                           parameter_type,
                                                                           parameter_position)
                        VALUES (@report_id,
                                @param_name,
                                @param_type,
                                @param_position);

                        SET @param_count = @param_position;
                    END WHILE;


--                SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
--                INTO @column_names
--                FROM INFORMATION_SCHEMA.COLUMNS
--                -- WHERE TABLE_SCHEMA = 'alive' TODO: add back after verifying schema name
--                WHERE TABLE_NAME = @report_id;
--
--                SET @drop_table = CONCAT('DROP TABLE IF EXISTS `', @report_id, '`');
--
--                SET @createtb = CONCAT('CREATE TEMP TABLE AS SELECT ', @report_id, ';', CHAR(10),
--                                       'CREATE PROCEDURE ', @report_procedure_name, '(', CHAR(10),
--                                       @parameters, CHAR(10),
--                                       ')', CHAR(10),
--                                       'BEGIN', CHAR(10),
--                                       @sql_query, CHAR(10),
--                                       'END;', CHAR(10));
--
--                PREPARE deletetb FROM @drop_table;
--                PREPARE createtb FROM @create_table;
--
--               EXECUTE deletetb;
--               EXECUTE createtb;
--
--                DEALLOCATE PREPARE deletetb;
--                DEALLOCATE PREPARE createtb;

                --                SELECT GROUP_CONCAT(CONCAT('IN ', parameter_name, ' ', parameter_type) SEPARATOR ', ')
--                INTO @parameters
--                FROM mamba_dim_report_definition_parameters
--                WHERE report_id = @report_id
--                ORDER BY parameter_position;
--
--                SET @procedure_definition = CONCAT('DROP PROCEDURE IF EXISTS ', @report_procedure_name, ';', CHAR(10),
--                                                   'CREATE PROCEDURE ', @report_procedure_name, '(', CHAR(10),
--                                                   @parameters, CHAR(10),
--                                                   ')', CHAR(10),
--                                                   'BEGIN', CHAR(10),
--                                                   @sql_query, CHAR(10),
--                                                   'END;', CHAR(10));
--
--                PREPARE CREATE_PROC FROM @procedure_definition;
--                EXECUTE CREATE_PROC;
--                DEALLOCATE PREPARE CREATE_PROC;
--
                SET @report_count = @report_count + 1;
            END WHILE;

    END IF;

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
-- ----------------------  sp_mamba_get_report_column_names  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_get_report_column_names;


~
CREATE PROCEDURE sp_mamba_get_report_column_names(IN report_identifier VARCHAR(255))
BEGIN

    -- We could also pick the column names from the report definition table but it is in a comma-separated list (weigh both options)
    SELECT table_name
    INTO @table_name
    FROM mamba_dim_report_definition
    WHERE report_id = report_identifier;

    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = @table_name;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_generate_report_wrapper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_generate_report_wrapper;


~
CREATE PROCEDURE sp_mamba_generate_report_wrapper(IN generate_columns_flag TINYINT(1),
                                                  IN report_identifier VARCHAR(255),
                                                  IN parameter_list JSON)
BEGIN

    DECLARE proc_name VARCHAR(255);
    DECLARE sql_args VARCHAR(1000);
    DECLARE arg_name VARCHAR(50);
    DECLARE arg_value VARCHAR(255);
    DECLARE tester VARCHAR(255);
    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_parameter_names CURSOR FOR
        SELECT DISTINCT (p.parameter_name)
        FROM mamba_dim_report_definition_parameters p
        WHERE p.report_id = report_identifier;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    IF generate_columns_flag = 1 THEN
        SET proc_name = (SELECT DISTINCT (rd.report_columns_procedure_name)
                         FROM mamba_dim_report_definition rd
                         WHERE rd.report_id = report_identifier);
    ELSE
        SET proc_name = (SELECT DISTINCT (rd.report_procedure_name)
                         FROM mamba_dim_report_definition rd
                         WHERE rd.report_id = report_identifier);
    END IF;

    OPEN cursor_parameter_names;
    read_loop:
    LOOP
        FETCH cursor_parameter_names INTO arg_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        SET arg_value = IFNULL((JSON_EXTRACT(parameter_list, CONCAT('$[', ((SELECT p.parameter_position
                                                                            FROM mamba_dim_report_definition_parameters p
                                                                            WHERE p.parameter_name = arg_name
                                                                              AND p.report_id = report_identifier) - 1),
                                                                    '].value'))), 'NULL');
        SET tester = CONCAT_WS(', ', tester, arg_value);
        SET sql_args = IFNULL(CONCAT_WS(', ', sql_args, arg_value), NULL);

    END LOOP;

    CLOSE cursor_parameter_names;

    SET @sql = CONCAT('CALL ', proc_name, '(', IFNULL(sql_args, ''), ')');

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

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
FROM openmrs.location;

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
FROM openmrs.patient_identifier_type;

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
FROM openmrs.concept_datatype dt;
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
    datatype_id INT          NOT NULL, -- make it a FK
    datatype    VARCHAR(100) NULL,
    retired     TINYINT(1)   NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_concept_concept_id_index
    ON mamba_dim_concept (concept_id);

CREATE INDEX mamba_dim_concept_uuid_index
    ON mamba_dim_concept (uuid);

CREATE INDEX mamba_dim_concept_datatype_id_index
    ON mamba_dim_concept (datatype_id);

CREATE INDEX mamba_dim_concept_retired_index
    ON mamba_dim_concept (retired);

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
                               datatype_id,
                               retired)
SELECT c.uuid        AS uuid,
       c.concept_id  AS concept_id,
       c.datatype_id AS datatype_id,
       c.retired
FROM openmrs.concept c;
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
FROM openmrs.concept_answer ca;

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
FROM openmrs.concept_name cn
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
    id                INT         NOT NULL AUTO_INCREMENT,
    encounter_type_id INT         NOT NULL,
    uuid              CHAR(38)    NOT NULL,
    name              VARCHAR(50) NOT NULL,

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
                                      uuid,
                                      name)
SELECT et.encounter_type_id,
       et.uuid,
       et.name
FROM openmrs.encounter_type et;
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
    id                  INT      NOT NULL AUTO_INCREMENT,
    encounter_id        INT      NOT NULL,
    uuid                CHAR(38) NOT NULL,
    encounter_type      INT      NOT NULL,
    encounter_type_uuid CHAR(38) NULL,
    patient_id          INT      NOT NULL,
    encounter_datetime  DATETIME NOT NULL,
    date_created        DATETIME NOT NULL,
    voided              TINYINT  NOT NULL,
    visit_id            INT      NULL,

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

CREATE INDEX mamba_dim_encounter_encounter_type_uuid_index
    ON mamba_dim_encounter (encounter_type_uuid);

CREATE INDEX mamba_dim_encounter_patient_id_index
    ON mamba_dim_encounter (patient_id);

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
FROM openmrs.encounter e
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

CREATE INDEX mamba_dim_concept_metadata_row_num_index
    ON mamba_dim_concept_metadata (row_num);

CREATE INDEX mamba_dim_concept_metadata_flat_table_name_index
    ON mamba_dim_concept_metadata (flat_table_name);

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

      SET @report_data = '{"flat_report_metadata":[{
  "report_name": "ART_Register",
  "flat_table_name": "mamba_flat_encounter_art_card",
  "encounter_type_uuid": "8d5b2be0-c2cc-11de-8d13-0010c6dffd0f" ,
  "concepts_locale": "en",
  "table_columns": {
    "return_date": "dcac04cf-30ab-102d-86b0-7a5022ba4115",
    "current_regimen": "dd2b0b4d-30ab-102d-86b0-7a5022ba4115",
    "who_stage": "dcdff274-30ab-102d-86b0-7a5022ba4115",
    "no_of_days": "7593ede6-6574-4326-a8a6-3d742e843659",
    "no_of_pills": "b0e53f0a-eaca-49e6-b663-d0df61601b70",
    "tb_status": "dce02aa1-30ab-102d-86b0-7a5022ba4115",
    "dsdm": "73312fee-c321-11e8-a355-529269fb1459",
    "pregnant": "dcda5179-30ab-102d-86b0-7a5022ba4115",
    "emtct": "dcd7e8e5-30ab-102d-86b0-7a5022ba4115",
    "cotrim": "c3d744f6-00ef-4774-b9a7-d33c58f5b017"
  }
},{
  "report_name": "HTS Report",
  "flat_table_name": "mamba_flat_encounter_hts",
  "encounter_type_uuid": "79c1f50f-f77d-42e2-ad2a-d29304dde2fe",
  "concepts_locale": "en",
  "table_columns": {
    "test_setting": "13abe5c9-6de2-4970-b348-36d352ee8eeb",
    "community_service_point": "74a3b695-30f7-403b-8f63-3f766461e104",
    "facility_service_point": "80bcc9c1-e328-47e8-affe-6d1bffe4adf1",
    "hts_approach": "9641ead9-8821-4898-b633-a8e96c0933cf",
    "pop_type": "166432AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "key_pop_type": "d3d4ae96-8c8a-43db-a9dc-dac951f5dcb3",
    "key_pop_migrant_worker": "63ea75cb-205f-4e7b-9ede-5f9b8a4dda9f",
    "key_pop_uniformed_forces": "b282bb08-62a7-42c2-9bea-8751c267d13e",
    "key_pop_transgender": "22b202fc-67de-4af9-8c88-46e22559d4b2",
    "key_pop_AGYW": "678f3144-302f-493e-ba22-7ec60a84732a",
    "key_pop_fisher_folk": "def00c73-f6d5-42fb-bcec-0b192b5be22d",
    "key_pop_prisoners": "8da9bf92-22f6-40be-b468-1ad08de7d457",
    "key_pop_refugees": "dc1058ea-4edd-4780-aeaa-a474f7f3a437",
    "key_pop_msm": "160578AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "key_pop_fsw": "160579AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "key_pop_truck_driver": "162198AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "key_pop_pwd": "365371fd-0106-4a53-abc4-575e3d65d372",
    "key_pop_pwid": "c038bff0-8e33-408c-b51f-7fb6448d2f6c",
    "sexually_active": "160109AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "unprotected_sex_last_12mo": "159218AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "sti_last_6mo": "156660AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "ever_tested_hiv": "1492AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "duration_since_last_test": "e7947a45-acff-49e1-ba1c-33e43a710e0d",
    "last_test_result": "159427AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "reason_for_test": "ce3816e7-082d-496b-890b-a2b169922c22",
    "pretest_counselling": "de32152d-93b0-412a-908a-20af0c46f215",
    "type_pretest_counselling": "0473ec07-2f34-4447-9c58-e35a1c491b6f",
    "consent_provided": "1710AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "test_conducted": "164401AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_test_conducted": "164400AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "initial_kit_name": "afa64df8-50af-4bc3-8135-6e6603f62068",
    "initial_test_result": "e767ba5d-7560-43ba-a746-2b0ff0a2a513",
    "confirmatory_kit_name": "b78d89e7-08aa-484f-befb-1e3e70cd6985",
    "tiebreaker_kit_name": "73434a78-e4fc-42f7-a812-f30f3b3cabe3",
    "tiebreaker_test_result": "bfc5fbb9-2b23-422e-a741-329bb2597032",
    "final_test_result": "e16b0068-b6a2-46b7-aba9-e3be00a7b4ab",
    "syphilis_test_result": "165303AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "given_result": "164848AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_given_result": "160082AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "result_received_couple": "445846e9-b929-4519-bc83-d51c051918f5",
    "couple_result": "5f38bc97-d6ca-43f8-a019-b9a9647d0c6a",
    "recency_consent": "976ca997-fb2b-4bef-a299-f7c9e16b50a8",
    "recency_test_done": "4fe5857e-c804-41cf-b3c9-0acc1f516ab7",
    "recency_test_type": "05112308-79ba-4e00-802e-a7576733b98e",
    "recency_rtri_result": "165092AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "recency_vl_result": "856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tb_symptoms": "159800AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tb_symptoms_fever": "1494AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tb_symptoms_cough": "159799AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tb_symptoms_hemoptysis": "138905AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tb_symptoms_nightsweats": "133027AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "sti_symptoms": "c4f81292-61a3-4561-a4ae-78be7d16d928",
    "sti_symptoms_female_genitalulcers": "153872AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "sti_symptoms_genitalsores": "faf06026-fce9-4d2c-9ef2-24fb45343804",
    "sti_symptoms_lower_abdominalpain": "06be8996-ef55-438b-bbb9-5bebeb18e779",
    "sti_symptoms_scrotalmass": "d8e46cc0-4d08-45d9-a46d-bd083db63057",
    "sti_symptoms_male_genitalulcers": "123861AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "sti_symptoms_urethral_discharge": "60817acb-90f1-4d46-be87-2c47e150770b",
    "sti_symptomsVaginal_discharge": "9a24bedc-d42c-422e-9f5d-371b59af0660",
    "client_linked_care": "e8e8fe71-adbb-48e7-b531-589985094d30",
    "facility_referred_care": "161562AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referred_for_services": "494117dd-c763-4374-8402-5ed91bd9b8d0",
    "is_referred_prevention_services": "5832db34-152d-4ead-a591-c627683c7f05",
    "is_referred_srh_services": "7ea48919-1cfd-46fd-9ea0-8255d596e463",
    "is_referred_clinical_services": "ca0b979e-d69a-43d3-bbea-9b24290b021e",
    "referred_support_services": "fbe382b6-6f01-49ff-a6c9-19c1cb50b916",
    "referred_prevention_services": "5f394708-ca7d-4558-8d23-a73de181b02d",
    "referred_preexposure_services": "88cdde2b-753b-48ac-a51a-ae5e1ab24846",
    "referred_postexposure_services": "1691AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referred_vmmc_services": "162223AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referred_harmreduction_services": "da0238c1-0ddd-49cc-b10d-c552391b6332",
    "referred_behavioural_services": "ac2e75dc-fceb-4591-9ffb-3f852c0750d9",
    "referred_postgbv_services": "0be6a668-b4ff-4fc5-bbae-0e2a86af1bd1",
    "referred_prevention_info_services": "e7ee9ec2-3cc7-4e59-8172-9fd08911e8c5",
    "referred_other_prevention_services": "5622AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referred_srh_services": "bf634be9-197a-433b-8e4e-7a04242a4e1d",
    "referred_hiv_partner_kpcontacts_testing": "a56cdd43-f2eb-49d6-88fd-113aaea2e85f",
    "referred_hiv_partner_testing": "f0589be1-d457-4138-b244-bfb115cdea21",
    "referred_sti_testing_tx": "46da10c7-49e3-45e5-8e82-7c529d52a1a8",
    "referred_analcancer_screening": "9d4c029a-2ac3-44c3-9a20-fb32c81a9ba2",
    "referred_cacx_screening_tx": "060dd5b2-2d65-4db5-85f0-cd1ba809350f",
    "referred_pregnancy_check": "0097d9b1-6758-4754-8713-91638efe12ea",
    "referred_contraception_fp": "6488e62a-314b-49da-b8d4-ca9c7a6941fc",
    "referred_srh_other": "5622AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referred_clinical_services": "960f2980-35e2-4677-88ed-79424fe0fc91",
    "referred_tb_program": "160541AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referred_ipt_rogram": "164128AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referred_ctx_services": "858f0f06-bc62-4b04-b864-cef98a2f3845",
    "referred_vaccinations_services": "0cf2ce2c-cd3f-478b-89b7-542018674dba",
    "referred_other_clinical_services": "5622AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referred_other_support": "b5afd495-00fc-4d94-9e26-8f6c8cc8caa0",
    "referred_psychosocial_support": "5490AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referred_mentalhealth_support": "5489AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referred_violence_support": "ea08440d-41d4-4795-bb4d-4639cf32645c",
    "referred_legal_support": "a046ce31-e0d9-4044-a384-ecc429dc4035",
    "referred_disclosure_support": "846a63c0-4530-4008-b6a1-12201b9e0b88",
    "is_referred_other_support": "5622AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
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
    INNER JOIN (SELECT id,
                       ROW_NUMBER() OVER (PARTITION BY flat_table_name,concept_id ORDER BY id ASC) num
                FROM mamba_dim_concept_metadata) m
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
-- ----------------------  sp_mamba_dim_report_definition_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition_create;


~
CREATE PROCEDURE sp_mamba_dim_report_definition_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_report_definition
(
    id                            INT          NOT NULL AUTO_INCREMENT,
    report_id                     VARCHAR(255) NOT NULL UNIQUE,
    report_procedure_name         VARCHAR(255) NOT NULL UNIQUE, -- should be derived from report_id??
    report_columns_procedure_name VARCHAR(255) NOT NULL UNIQUE,
    sql_query                     TEXT         NOT NULL,
    table_name                    VARCHAR(255) NOT NULL,        -- name of the table (will contain columns) of this query
    report_name                   VARCHAR(255) NULL,
    result_column_names           TEXT         NULL,            -- comma-separated column names

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_report_definition_report_id_index
    ON mamba_dim_report_definition (report_id);


CREATE TABLE mamba_dim_report_definition_parameters
(
    id                 INT          NOT NULL AUTO_INCREMENT,
    report_id          VARCHAR(255) NOT NULL,
    parameter_name     VARCHAR(255) NOT NULL,
    parameter_type     VARCHAR(30)  NOT NULL,
    parameter_position INT          NOT NULL, -- takes order or declaration in JSON file

    PRIMARY KEY (id),
    FOREIGN KEY (`report_id`) REFERENCES `mamba_dim_report_definition` (`report_id`)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_report_definition_parameter_position_index
    ON mamba_dim_report_definition_parameters (parameter_position);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_report_definition_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition_insert;


~
CREATE PROCEDURE sp_mamba_dim_report_definition_insert()
BEGIN
-- $BEGIN
SET @report_definition_json = '{
  "report_definitions": [
    {
      "report_name": "PMTCT Mother HIV Status",
      "report_id": "mother_hiv_status",
      "report_sql": {
        "sql_query": "SELECT pm.hiv_test_result from mamba_flat_encounter_pmtct_anc pm INNER JOIN mamba_dim_person p ON pm.client_id = p.person_id WHERE p.uuid = person_uuid AND pm.ptracker_id = ptracker_id",
        "query_params": [
          {
            "name": "ptracker_id",
            "type": "VARCHAR(255)"
          },
          {
            "name": "person_uuid",
            "type": "VARCHAR(255)"
          }
        ]
      }
    },
    {
      "report_name": "PMTCT Total Deliveries",
      "report_id": "total_deliveries",
      "report_sql": {
        "sql_query": "SELECT COUNT(*) AS total_deliveries FROM encounter e INNER JOIN encounter_type et ON e.encounter_type = et.encounter_type_id WHERE et.uuid = ''2678423c-0523-4d76-b0da-18177b439eed'' AND DATE(e.encounter_datetime) > CONCAT(YEAR(CURDATE()), ''-01-01 00:00:00'')",
        "query_params": []
      }
    },
    {
      "report_name": "PMTCT HIV-Exposed Infants",
      "report_id": "hiv_exposed_infants",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT ei.infant_client_id) hiv_exposed_infants_count FROM mamba_fact_pmtct_exposedinfants ei INNER JOIN mamba_dim_person p ON ei.infant_client_id = p.person_id WHERE ei.encounter_datetime BETWEEN DATE_FORMAT(NOW(), ''%Y-01-01'') AND CURDATE() AND birthdate BETWEEN DATE_FORMAT(NOW(), ''%Y-01-01'') AND CURDATE()",
        "query_params": []
      }
    },
    {
      "report_name": "PMTCT Total Pregnant women",
      "report_id": "total_pregnant_women",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT pw.client_id) total_pregnant_women FROM mamba_fact_pmtct_pregnant_women pw WHERE visit_type = ''New ANC Visit'' AND encounter_datetime BETWEEN DATE_FORMAT(NOW(), ''%Y-01-01'') AND CURDATE() AND DATE_ADD(date_of_last_menstrual_period, INTERVAL 40 WEEK) > CURDATE()",
        "query_params": []
      }
    }
  ]
}';
CALL sp_mamba_extract_report_definition_metadata(@report_definition_json, 'mamba_dim_report_definition');
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_report_definition_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition_update;


~
CREATE PROCEDURE sp_mamba_dim_report_definition_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_report_definition  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition;


~
CREATE PROCEDURE sp_mamba_dim_report_definition()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_report_definition_create();
CALL sp_mamba_dim_report_definition_insert();
CALL sp_mamba_dim_report_definition_update();

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
    id                  INT          NOT NULL AUTO_INCREMENT,
    person_id           INT          NOT NULL,
    birthdate           DATE         NULL,
    birthdate_estimated TINYINT(1)   NOT NULL,
    age                 INT          NULL,
    dead                TINYINT(1)   NOT NULL,
    death_date          DATETIME     NULL,
    deathdate_estimated TINYINT      NOT NULL,
    gender              VARCHAR(50)  NULL,
    date_created        DATETIME     NOT NULL,
    person_name_short   VARCHAR(255) NULL,
    person_name_long    TEXT         NULL,
    uuid                CHAR(38)     NOT NULL,
    voided              TINYINT(1)   NOT NULL,

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

INSERT INTO mamba_dim_person (person_id,
                              birthdate,
                              birthdate_estimated,
                              age,
                              dead,
                              death_date,
                              deathdate_estimated,
                              gender,
                              date_created,
                              person_name_short,
                              person_name_long,
                              uuid,
                              voided)

SELECT psn.person_id,
       psn.birthdate,
       psn.birthdate_estimated,
       fn_mamba_age_calculator(birthdate, death_date)               AS age,
       psn.dead,
       psn.death_date,
       psn.deathdate_estimated,
       psn.gender,
       psn.date_created,
       CONCAT_WS(' ', prefix, given_name, middle_name, family_name) AS person_name_short,
       CONCAT_WS(' ', prefix, given_name, middle_name, family_name_prefix, family_name, family_name2,
                 family_name_suffix, degree)
                                                                    AS person_name_long,
       psn.uuid,
       psn.voided
FROM openmrs.person psn
         INNER JOIN mamba_dim_person_name pn
                    on psn.person_id = pn.person_id
WHERE pn.preferred = 1
  AND pn.voided = 0;

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

CREATE INDEX mamba_dim_patient_identifier_preferred_index
    ON mamba_dim_patient_identifier (preferred);

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
FROM openmrs.patient_identifier;

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
    preferred          TINYINT     NOT NULL,
    prefix             VARCHAR(50) NULL,
    given_name         VARCHAR(50) NULL,
    middle_name        VARCHAR(50) NULL,
    family_name_prefix VARCHAR(50) NULL,
    family_name        VARCHAR(50) NULL,
    family_name2       VARCHAR(50) NULL,
    family_name_suffix VARCHAR(50) NULL,
    degree             VARCHAR(50) NULL,
    voided             TINYINT(1)  NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_person_name_person_name_id_index
    ON mamba_dim_person_name (person_name_id);

CREATE INDEX mamba_dim_person_name_person_id_index
    ON mamba_dim_person_name (person_id);

CREATE INDEX mamba_dim_person_name_voided_index
    ON mamba_dim_person_name (voided);

CREATE INDEX mamba_dim_person_name_preferred_index
    ON mamba_dim_person_name (preferred);

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
INSERT INTO mamba_dim_person_name(person_name_id,
                                  person_id,
                                  preferred,
                                  prefix,
                                  given_name,
                                  middle_name,
                                  family_name_prefix,
                                  family_name,
                                  family_name2,
                                  family_name_suffix,
                                  degree,
                                  voided)
SELECT pn.person_name_id,
       pn.person_id,
       pn.preferred,
       pn.prefix,
       pn.given_name,
       pn.middle_name,
       pn.family_name_prefix,
       pn.family_name,
       pn.family_name2,
       pn.family_name_suffix,
       pn.degree,
       pn.voided
FROM openmrs.person_name pn;
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

CREATE INDEX mamba_dim_person_address_person_id_index
    ON mamba_dim_person_address (person_id);

CREATE INDEX mamba_dim_person_address_preferred_index
    ON mamba_dim_person_address (preferred);

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
FROM openmrs.person_address;

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
        FROM openmrs.users c;
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

    relationship_id INT          NOT NULL AUTO_INCREMENT,
    person_a        INT          NOT NULL,
    relationship    INT          NOT NULL,
    person_b        INT          NOT NULL,
    start_date      DATETIME     NULL,
    end_date        DATETIME     NULL,
    creator         INT          NOT NULL,
    date_created    DATETIME     NOT NULL,
    date_changed    DATETIME     NULL,
    changed_by      INT          NULL,
    voided          TINYINT(1)   NOT NULL,
    voided_by       INT          NULL,
    date_voided     DATETIME     NULL,
    void_reason     VARCHAR(255) NULL,
    uuid            CHAR(38)     NOT NULL,

    PRIMARY KEY (relationship_id)

) CHARSET = UTF8MB3;

CREATE INDEX mamba_dim_relationship_person_a_index
    ON mamba_dim_relationship (person_a);

CREATE INDEX mamba_dim_relationship_person_b_index
    ON mamba_dim_relationship (person_b);

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
FROM openmrs.relationship;

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
-- ----------------------  sp_mamba_dim_orders_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_create;


~
CREATE PROCEDURE sp_mamba_dim_orders_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_orders
(
    id                     INT           NOT NULL AUTO_INCREMENT,
    order_id               INT           NOT NULL,
    uuid                   CHAR(38)      NOT NULL,
    order_type_id          INT           NOT NULL,
    concept_id             INT           NOT NULL,
    patient_id             INT           NOT NULL,
    encounter_id           INT           NOT NULL, -- links with encounter table
    accession_number       VARCHAR(255)  NULL,
    order_number           VARCHAR(50)   NOT NULL,
    orderer                INT           NOT NULL,
    instructions           TEXT          NULL,
    date_activated         DATETIME      NULL,
    auto_expire_date       DATETIME      NULL,
    date_stopped           DATETIME      NULL,
    order_reason           INT           NULL,
    creator                INT           NOT NULL,
    date_created           DATETIME      NOT NULL,
    voided                 TINYINT(1)    NOT NULL,
    voided_by              INT           NULL,
    date_voided            DATETIME      NULL,
    void_reason            VARCHAR(255)  NULL,
    order_reason_non_coded VARCHAR(255)  NULL,
    urgency                VARCHAR(50)   NOT NULL,
    previous_order_id      INT           NULL,
    order_action           VARCHAR(50)   NOT NULL,
    comment_to_fulfiller   VARCHAR(1024) NULL,
    care_setting           INT           NOT NULL,
    scheduled_date         DATETIME      NULL,
    order_group_id         INT           NULL,
    sort_weight            DOUBLE        NULL,
    fulfiller_comment      VARCHAR(1024) NULL,
    fulfiller_status       VARCHAR(50)   NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_orders_order_id_index
    ON mamba_dim_orders (order_id);

CREATE INDEX mamba_dim_orders_uuid_index
    ON mamba_dim_orders (uuid);

CREATE INDEX mamba_dim_orders_order_type_id_index
    ON mamba_dim_orders (order_type_id);

CREATE INDEX mamba_dim_orders_concept_id_index
    ON mamba_dim_orders (concept_id);

CREATE INDEX mamba_dim_orders_patient_id_index
    ON mamba_dim_orders (patient_id);

CREATE INDEX mamba_dim_orders_encounter_id_index
    ON mamba_dim_orders (encounter_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_insert;


~
CREATE PROCEDURE sp_mamba_dim_orders_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_orders (order_id, uuid, order_type_id, concept_id, patient_id, encounter_id, accession_number,
                              order_number, orderer, instructions, date_activated, auto_expire_date, date_stopped,
                              order_reason, creator, date_created, voided, voided_by, date_voided, void_reason,
                              order_reason_non_coded, urgency, previous_order_id, order_action, comment_to_fulfiller,
                              care_setting, scheduled_date, order_group_id, sort_weight, fulfiller_comment,
                              fulfiller_status)
SELECT order_id,
       uuid,
       order_type_id,
       concept_id,
       patient_id,
       encounter_id,
       accession_number,
       order_number,
       orderer,
       instructions,
       date_activated,
       auto_expire_date,
       date_stopped,
       order_reason,
       creator,
       date_created,
       voided,
       voided_by,
       date_voided,
       void_reason,
       order_reason_non_coded,
       urgency,
       previous_order_id,
       order_action,
       comment_to_fulfiller,
       care_setting,
       scheduled_date,
       order_group_id,
       sort_weight,
       fulfiller_comment,
       fulfiller_status
FROM openmrs.orders;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_update;


~
CREATE PROCEDURE sp_mamba_dim_orders_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders;


~
CREATE PROCEDURE sp_mamba_dim_orders()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_orders_create();
CALL sp_mamba_dim_orders_insert();
CALL sp_mamba_dim_orders_update();

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
    ON mamba_z_encounter_obs (encounter_id, person_id, encounter_datetime);

CREATE INDEX mamba_z_encounter_obs_encounter_id_index
    ON mamba_z_encounter_obs (encounter_id);

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

CREATE INDEX mamba_z_encounter_obs_row_num_index
    ON mamba_z_encounter_obs (row_num);

CREATE INDEX mamba_z_encounter_obs_encounter_datetime_index
    ON mamba_z_encounter_obs (encounter_datetime);

CREATE INDEX mamba_z_encounter_obs_person_id_index
    ON mamba_z_encounter_obs (person_id);

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
(encounter_id,
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
 row_num)
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
       ROW_NUMBER() OVER (PARTITION BY person_id,encounter_id,concept_id)
FROM openmrs.obs o
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
    INNER JOIN mamba_dim_concept_name cn
    ON z.obs_value_coded = cn.concept_id
    INNER JOIN mamba_dim_concept c
    ON c.concept_id = cn.concept_id
SET z.obs_value_text       = cn.name,
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

CALL sp_mamba_dim_report_definition;

CALL sp_mamba_dim_encounter_type;

CALL sp_mamba_dim_encounter;

CALL sp_mamba_dim_person_name;

CALL sp_mamba_dim_person;

CALL sp_mamba_dim_person_address;

CALL sp_mamba_dim_user;

CALL sp_mamba_dim_relationship;

CALL sp_mamba_dim_patient_identifier;

CALL sp_mamba_dim_orders;

CALL sp_mamba_dim_agegroup;

CALL sp_mamba_z_encounter_obs;

CALL sp_mamba_flat_encounter_table_create_all;

CALL sp_mamba_flat_encounter_table_insert_all;
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
CALL sp_mamba_data_processing_derived_hts();
CALL sp_mamba_data_processing_derived_covid();
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
-- ----------------------  sp_mamba_mother_hiv_status_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_mother_hiv_status_query;


~
CREATE PROCEDURE sp_mamba_mother_hiv_status_query(IN ptracker_id VARCHAR(255), IN person_uuid VARCHAR(255))
BEGIN

SELECT pm.hiv_test_result from mamba_flat_encounter_pmtct_anc pm INNER JOIN mamba_dim_person p ON pm.client_id = p.person_id WHERE p.uuid = person_uuid AND pm.ptracker_id = ptracker_id;

END~




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_mother_hiv_status_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_mother_hiv_status_columns_query;


~
CREATE PROCEDURE sp_mamba_mother_hiv_status_columns_query(IN ptracker_id VARCHAR(255), IN person_uuid VARCHAR(255))
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_mother_hiv_status;
CREATE TABLE mamba_dim_mother_hiv_status AS
SELECT pm.hiv_test_result from mamba_flat_encounter_pmtct_anc pm INNER JOIN mamba_dim_person p ON pm.client_id = p.person_id WHERE p.uuid = person_uuid AND pm.ptracker_id = ptracker_id
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_mother_hiv_status';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='mother_hiv_status';

END~




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_deliveries_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_deliveries_query;


~
CREATE PROCEDURE sp_mamba_total_deliveries_query()
BEGIN

SELECT COUNT(*) AS total_deliveries FROM encounter e INNER JOIN encounter_type et ON e.encounter_type = et.encounter_type_id WHERE et.uuid = '2678423c-0523-4d76-b0da-18177b439eed' AND DATE(e.encounter_datetime) > CONCAT(YEAR(CURDATE()), '-01-01 00:00:00');

END~




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_deliveries_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_deliveries_columns_query;


~
CREATE PROCEDURE sp_mamba_total_deliveries_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_total_deliveries;
CREATE TABLE mamba_dim_total_deliveries AS
SELECT COUNT(*) AS total_deliveries FROM encounter e INNER JOIN encounter_type et ON e.encounter_type = et.encounter_type_id WHERE et.uuid = '2678423c-0523-4d76-b0da-18177b439eed' AND DATE(e.encounter_datetime) > CONCAT(YEAR(CURDATE()), '-01-01 00:00:00')
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_total_deliveries';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_deliveries';

END~




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_hiv_exposed_infants_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_hiv_exposed_infants_query;


~
CREATE PROCEDURE sp_mamba_hiv_exposed_infants_query()
BEGIN

SELECT COUNT(DISTINCT ei.infant_client_id) hiv_exposed_infants_count FROM mamba_fact_pmtct_exposedinfants ei INNER JOIN mamba_dim_person p ON ei.infant_client_id = p.person_id WHERE ei.encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND CURDATE() AND birthdate BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND CURDATE();

END~




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_hiv_exposed_infants_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_hiv_exposed_infants_columns_query;


~
CREATE PROCEDURE sp_mamba_hiv_exposed_infants_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_hiv_exposed_infants;
CREATE TABLE mamba_dim_hiv_exposed_infants AS
SELECT COUNT(DISTINCT ei.infant_client_id) hiv_exposed_infants_count FROM mamba_fact_pmtct_exposedinfants ei INNER JOIN mamba_dim_person p ON ei.infant_client_id = p.person_id WHERE ei.encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND CURDATE() AND birthdate BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND CURDATE()
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_hiv_exposed_infants';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='hiv_exposed_infants';

END~




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_pregnant_women_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_pregnant_women_query;


~
CREATE PROCEDURE sp_mamba_total_pregnant_women_query()
BEGIN

SELECT COUNT(DISTINCT pw.client_id) total_pregnant_women FROM mamba_fact_pmtct_pregnant_women pw WHERE visit_type = 'New ANC Visit' AND encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND CURDATE() AND DATE_ADD(date_of_last_menstrual_period, INTERVAL 40 WEEK) > CURDATE();

END~




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_pregnant_women_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_pregnant_women_columns_query;


~
CREATE PROCEDURE sp_mamba_total_pregnant_women_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_total_pregnant_women;
CREATE TABLE mamba_dim_total_pregnant_women AS
SELECT COUNT(DISTINCT pw.client_id) total_pregnant_women FROM mamba_fact_pmtct_pregnant_women pw WHERE visit_type = 'New ANC Visit' AND encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND CURDATE() AND DATE_ADD(date_of_last_menstrual_period, INTERVAL 40 WEEK) > CURDATE()
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_total_pregnant_women';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_pregnant_women';

END~



USE openmrs;
