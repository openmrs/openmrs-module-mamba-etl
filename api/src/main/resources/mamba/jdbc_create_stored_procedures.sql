CREATE database IF NOT EXISTS analysis_db;
~-~-

USE analysis_db;
~-~-


        
    
        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_calculate_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_calculate_agegroup;


~-~-
CREATE FUNCTION fn_mamba_calculate_agegroup(age INT) RETURNS VARCHAR(15)
    DETERMINISTIC
BEGIN
    DECLARE agegroup VARCHAR(15);
    CASE
        WHEN age < 1 THEN SET agegroup = '<1';
        WHEN age between 1 and 4 THEN SET agegroup = '1-4';
        WHEN age between 5 and 9 THEN SET agegroup = '5-9';
        WHEN age between 10 and 14 THEN SET agegroup = '10-14';
        WHEN age between 15 and 19 THEN SET agegroup = '15-19';
        WHEN age between 20 and 24 THEN SET agegroup = '20-24';
        WHEN age between 25 and 29 THEN SET agegroup = '25-29';
        WHEN age between 30 and 34 THEN SET agegroup = '30-34';
        WHEN age between 35 and 39 THEN SET agegroup = '35-39';
        WHEN age between 40 and 44 THEN SET agegroup = '40-44';
        WHEN age between 45 and 49 THEN SET agegroup = '45-49';
        WHEN age between 50 and 54 THEN SET agegroup = '50-54';
        WHEN age between 55 and 59 THEN SET agegroup = '55-59';
        WHEN age between 60 and 64 THEN SET agegroup = '60-64';
        ELSE SET agegroup = '65+';
        END CASE;

    RETURN agegroup;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_get_obs_value_column  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_get_obs_value_column;


~-~-
CREATE FUNCTION fn_mamba_get_obs_value_column(conceptDatatype VARCHAR(20)) RETURNS VARCHAR(20)
    DETERMINISTIC
BEGIN
    DECLARE obsValueColumn VARCHAR(20);

        IF conceptDatatype = 'Text' THEN
            SET obsValueColumn = 'obs_value_text';

        ELSEIF conceptDatatype = 'Coded'
           OR conceptDatatype = 'N/A' THEN
            SET obsValueColumn = 'obs_value_text';

        ELSEIF conceptDatatype = 'Boolean' THEN
            SET obsValueColumn = 'obs_value_boolean';

        ELSEIF  conceptDatatype = 'Date'
                OR conceptDatatype = 'Datetime' THEN
            SET obsValueColumn = 'obs_value_datetime';

        ELSEIF conceptDatatype = 'Numeric' THEN
            SET obsValueColumn = 'obs_value_numeric';

        ELSE
            SET obsValueColumn = 'obs_value_text';

        END IF;

    RETURN (obsValueColumn);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_age_calculator  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_age_calculator;


~-~-
CREATE FUNCTION fn_mamba_age_calculator(birthdate DATE, deathDate DATE) RETURNS INTEGER
    DETERMINISTIC
BEGIN
    DECLARE today DATE;
    DECLARE age INT;

    -- Check if birthdate is not null and not an empty string
    IF birthdate IS NULL OR TRIM(birthdate) = '' THEN
        RETURN NULL;
    ELSE
        SET today = IFNULL(CURDATE(), '0000-00-00');
        -- Check if birthdate is a valid date using STR_TO_DATE and if it's not in the future
        IF STR_TO_DATE(birthdate, '%Y-%m-%d') IS NULL OR STR_TO_DATE(birthdate, '%Y-%m-%d') > today THEN
            RETURN NULL;
        END IF;

        -- If deathDate is provided and in the past, set today to deathDate
        IF deathDate IS NOT NULL AND today > deathDate THEN
            SET today = deathDate;
        END IF;

        SET age = YEAR(today) - YEAR(birthdate);

        -- Adjust age based on month and day
        IF MONTH(today) < MONTH(birthdate) OR (MONTH(today) = MONTH(birthdate) AND DAY(today) < DAY(birthdate)) THEN
            SET age = age - 1;
        END IF;

        RETURN age;
    END IF;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_get_datatype_for_concept  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_get_datatype_for_concept;


~-~-
CREATE FUNCTION fn_mamba_get_datatype_for_concept(conceptDatatype VARCHAR(20)) RETURNS VARCHAR(20)
    DETERMINISTIC
BEGIN
    DECLARE mysqlDatatype VARCHAR(20);


    IF conceptDatatype = 'Text' THEN
        SET mysqlDatatype = 'TEXT';

    ELSEIF conceptDatatype = 'Coded'
        OR conceptDatatype = 'N/A' THEN
        SET mysqlDatatype = 'Varchar(250)';

    ELSEIF conceptDatatype = 'Boolean' THEN
        SET mysqlDatatype = 'Boolean';

    ELSEIF conceptDatatype = 'Date' THEN
        SET mysqlDatatype = 'DATE';

    ELSEIF conceptDatatype = 'Datetime' THEN
        SET mysqlDatatype = 'DATETIME';

    ELSEIF conceptDatatype = 'Numeric' THEN
        SET mysqlDatatype = 'DOUBLE';

    ELSE
        SET mysqlDatatype = 'TEXT';

    END IF;

    RETURN mysqlDatatype;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_generate_json_from_mamba_flat_table_config  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_generate_json_from_mamba_flat_table_config;


~-~-
CREATE FUNCTION fn_mamba_generate_json_from_mamba_flat_table_config(
    is_incremental TINYINT(1)
) RETURNS JSON
    DETERMINISTIC
BEGIN
    DECLARE report_array JSON;
    SET session group_concat_max_len = 200000;

    SELECT CONCAT('{"flat_report_metadata":[', GROUP_CONCAT(
            CONCAT(
                    '{',
                    '"report_name":', JSON_EXTRACT(table_json_data, '$.report_name'),
                    ',"flat_table_name":', JSON_EXTRACT(table_json_data, '$.flat_table_name'),
                    ',"encounter_type_uuid":', JSON_EXTRACT(table_json_data, '$.encounter_type_uuid'),
                    ',"table_columns": ', JSON_EXTRACT(table_json_data, '$.table_columns'),
                    '}'
            ) SEPARATOR ','), ']}')
    INTO report_array
    FROM mamba_flat_table_config
    WHERE (IF(is_incremental = 1, incremental_record = 1, 1));

    RETURN report_array;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_array_length  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_array_length;

~-~-
CREATE FUNCTION fn_mamba_array_length(array_string TEXT) RETURNS INT
    DETERMINISTIC
BEGIN
  DECLARE length INT DEFAULT 0;
  DECLARE i INT DEFAULT 1;

  -- If the array_string is not empty, initialize length to 1
    IF TRIM(array_string) != '' AND TRIM(array_string) != '[]' THEN
        SET length = 1;
    END IF;

  -- Count the number of commas in the array string
    WHILE i <= CHAR_LENGTH(array_string) DO
        IF SUBSTRING(array_string, i, 1) = ',' THEN
          SET length = length + 1;
        END IF;
        SET i = i + 1;
    END WHILE;

RETURN length;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_get_array_item_by_index  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_get_array_item_by_index;

~-~-
CREATE FUNCTION fn_mamba_get_array_item_by_index(array_string TEXT, item_index INT) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE elem_start INT DEFAULT 1;
  DECLARE elem_end INT DEFAULT 0;
  DECLARE current_index INT DEFAULT 0;
  DECLARE result TEXT DEFAULT '';

    -- If the item_index is less than 1 or the array_string is empty, return an empty string
    IF item_index < 1 OR array_string = '[]' OR TRIM(array_string) = '' THEN
        RETURN '';
    END IF;

    -- Loop until we find the start quote of the desired index
    WHILE current_index < item_index DO
        -- Find the start quote of the next element
        SET elem_start = LOCATE('"', array_string, elem_end + 1);
        -- If we can't find a new element, return an empty string
        IF elem_start = 0 THEN
          RETURN '';
        END IF;

        -- Find the end quote of this element
        SET elem_end = LOCATE('"', array_string, elem_start + 1);
        -- If we can't find the end quote, return an empty string
        IF elem_end = 0 THEN
          RETURN '';
        END IF;

        -- Increment the current_index
        SET current_index = current_index + 1;
    END WHILE;

    -- When the loop exits, current_index should equal item_index, and elem_start/end should be the positions of the quotes
    -- Extract the element
    SET result = SUBSTRING(array_string, elem_start + 1, elem_end - elem_start - 1);

    RETURN result;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_array_length  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_array_length;

~-~-
CREATE FUNCTION fn_mamba_json_array_length(json_array TEXT) RETURNS INT
    DETERMINISTIC
BEGIN
    DECLARE array_length INT DEFAULT 0;
    DECLARE current_pos INT DEFAULT 1;
    DECLARE char_val CHAR(1);

    IF json_array IS NULL THEN
        RETURN 0;
    END IF;

  -- Iterate over the string to count the number of objects based on commas and curly braces
    WHILE current_pos <= CHAR_LENGTH(json_array) DO
        SET char_val = SUBSTRING(json_array, current_pos, 1);

    -- Check for the start of an object
        IF char_val = '{' THEN
            SET array_length = array_length + 1;

      -- Move current_pos to the end of this object
            SET current_pos = LOCATE('}', json_array, current_pos) + 1;
        ELSE
            SET current_pos = current_pos + 1;
        END IF;
    END WHILE;

RETURN array_length;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_extract  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_extract;

~-~-
CREATE FUNCTION fn_mamba_json_extract(json TEXT, key_name VARCHAR(255)) RETURNS VARCHAR(255)
    DETERMINISTIC
BEGIN
  DECLARE start_index INT;
  DECLARE end_index INT;
  DECLARE key_length INT;
  DECLARE key_index INT;

  SET key_name = CONCAT( key_name, '":');
  SET key_length = CHAR_LENGTH(key_name);
  SET key_index = LOCATE(key_name, json);

    IF key_index = 0 THEN
        RETURN NULL;
    END IF;

    SET start_index = key_index + key_length;

    CASE
        WHEN SUBSTRING(json, start_index, 1) = '"' THEN
            SET start_index = start_index + 1;
            SET end_index = LOCATE('"', json, start_index);
        ELSE
            SET end_index = LOCATE(',', json, start_index);
            IF end_index = 0 THEN
                SET end_index = LOCATE('}', json, start_index);
            END IF;
    END CASE;

RETURN SUBSTRING(json, start_index, end_index - start_index);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_extract_array  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_extract_array;

~-~-
CREATE FUNCTION fn_mamba_json_extract_array(json TEXT, key_name VARCHAR(255)) RETURNS TEXT
    DETERMINISTIC
BEGIN
DECLARE start_index INT;
DECLARE end_index INT;
DECLARE array_text TEXT;

    SET key_name = CONCAT('"', key_name, '":');
    SET start_index = LOCATE(key_name, json);

    IF start_index = 0 THEN
        RETURN NULL;
    END IF;

    SET start_index = start_index + CHAR_LENGTH(key_name);

    IF SUBSTRING(json, start_index, 1) != '[' THEN
        RETURN NULL;
    END IF;

    SET start_index = start_index + 1; -- Start after the '['
    SET end_index = start_index;

    -- Loop to find the matching closing bracket for the array
    SET @bracket_counter = 1;
    WHILE @bracket_counter > 0 AND end_index <= CHAR_LENGTH(json) DO
        SET end_index = end_index + 1;
        IF SUBSTRING(json, end_index, 1) = '[' THEN
          SET @bracket_counter = @bracket_counter + 1;
        ELSEIF SUBSTRING(json, end_index, 1) = ']' THEN
          SET @bracket_counter = @bracket_counter - 1;
        END IF;
    END WHILE;

    IF @bracket_counter != 0 THEN
        RETURN NULL; -- The brackets are not balanced, return NULL
    END IF;

SET array_text = SUBSTRING(json, start_index, end_index - start_index);

RETURN array_text;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_extract_object  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_extract_object;

~-~-
CREATE FUNCTION fn_mamba_json_extract_object(json_string TEXT, key_name VARCHAR(255)) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE start_index INT;
  DECLARE end_index INT;
  DECLARE nested_level INT DEFAULT 0;
  DECLARE substring_length INT;
  DECLARE key_str VARCHAR(255);
  DECLARE result TEXT DEFAULT '';

  SET key_str := CONCAT('"', key_name, '": {');

  -- Find the start position of the key
  SET start_index := LOCATE(key_str, json_string);
    IF start_index = 0 THEN
        RETURN NULL;
    END IF;

    -- Adjust start_index to the start of the value
    SET start_index := start_index + CHAR_LENGTH(key_str);

    -- Initialize the end_index to start_index
    SET end_index := start_index;

    -- Find the end of the object
    WHILE nested_level >= 0 AND end_index <= CHAR_LENGTH(json_string) DO
        SET end_index := end_index + 1;
        SET substring_length := end_index - start_index;

        -- Check for nested objects
        IF SUBSTRING(json_string, end_index, 1) = '{' THEN
          SET nested_level := nested_level + 1;
        ELSEIF SUBSTRING(json_string, end_index, 1) = '}' THEN
          SET nested_level := nested_level - 1;
        END IF;
    END WHILE;

    -- Get the JSON object
    IF nested_level < 0 THEN
    -- We found a matching pair of curly braces
        SET result := SUBSTRING(json_string, start_index, substring_length);
    END IF;

RETURN result;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_keys_array  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS fn_mamba_json_keys_array;

~-~-
CREATE FUNCTION fn_mamba_json_keys_array(json_object TEXT) RETURNS TEXT
    DETERMINISTIC
BEGIN
    DECLARE finished INT DEFAULT 0;
    DECLARE start_index INT DEFAULT 1;
    DECLARE end_index INT DEFAULT 1;
    DECLARE key_name TEXT DEFAULT '';
    DECLARE my_keys TEXT DEFAULT '';
    DECLARE json_length INT;
    DECLARE key_end_index INT;

    SET json_length = CHAR_LENGTH(json_object);

    -- Initialize the my_keys string as an empty 'array'
    SET my_keys = '';

    -- This loop goes through the JSON object and extracts the my_keys
    WHILE NOT finished DO
            -- Find the start of the key
            SET start_index = LOCATE('"', json_object, end_index);
            IF start_index = 0 OR start_index >= json_length THEN
                SET finished = 1;
            ELSE
                -- Find the end of the key
                SET end_index = LOCATE('"', json_object, start_index + 1);
                SET key_name = SUBSTRING(json_object, start_index + 1, end_index - start_index - 1);

                -- Append the key to the 'array' of my_keys
                IF my_keys = ''
                    THEN
                    SET my_keys = CONCAT('["', key_name, '"');
                ELSE
                    SET my_keys = CONCAT(my_keys, ',"', key_name, '"');
                END IF;

                -- Move past the current key-value pair
                SET key_end_index = LOCATE(',', json_object, end_index);
                IF key_end_index = 0 THEN
                    SET key_end_index = LOCATE('}', json_object, end_index);
                END IF;
                IF key_end_index = 0 THEN
                    -- Closing brace not found - malformed JSON
                    SET finished = 1;
                ELSE
                    -- Prepare for the next iteration
                    SET end_index = key_end_index + 1;
                END IF;
            END IF;
    END WHILE;

    -- Close the 'array' of my_keys
    IF my_keys != '' THEN
        SET my_keys = CONCAT(my_keys, ']');
    END IF;

    RETURN my_keys;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_length  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_length;

~-~-
CREATE FUNCTION fn_mamba_json_length(json_array TEXT) RETURNS INT
    DETERMINISTIC
BEGIN
    DECLARE element_count INT DEFAULT 0;
    DECLARE current_position INT DEFAULT 1;

    WHILE current_position <= LENGTH(json_array) DO
        SET element_count = element_count + 1;
        SET current_position = LOCATE(',', json_array, current_position) + 1;

        IF current_position = 0 THEN
            RETURN element_count;
        END IF;
    END WHILE;

RETURN element_count;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_object_at_index  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_object_at_index;

~-~-
CREATE FUNCTION fn_mamba_json_object_at_index(json_array TEXT, index_pos INT) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE obj_start INT DEFAULT 1;
  DECLARE obj_end INT DEFAULT 1;
  DECLARE current_index INT DEFAULT 0;
  DECLARE obj_text TEXT;

    -- Handle negative index_pos or json_array being NULL
    IF index_pos < 1 OR json_array IS NULL THEN
        RETURN NULL;
    END IF;

    -- Find the start of the requested object
    WHILE obj_start < CHAR_LENGTH(json_array) AND current_index < index_pos DO
        SET obj_start = LOCATE('{', json_array, obj_end);

        -- If we can't find a new object, return NULL
        IF obj_start = 0 THEN
          RETURN NULL;
        END IF;

        SET current_index = current_index + 1;
        -- If this isn't the object we want, find the end and continue
        IF current_index < index_pos THEN
          SET obj_end = LOCATE('}', json_array, obj_start) + 1;
        END IF;
    END WHILE;

    -- Now obj_start points to the start of the desired object
    -- Find the end of it
    SET obj_end = LOCATE('}', json_array, obj_start);
    IF obj_end = 0 THEN
        -- The object is not well-formed
        RETURN NULL;
    END IF;

    -- Extract the object
    SET obj_text = SUBSTRING(json_array, obj_start, obj_end - obj_start + 1);

RETURN obj_text;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_json_value_by_key  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_json_value_by_key;

~-~-
CREATE FUNCTION fn_mamba_json_value_by_key(json TEXT, key_name VARCHAR(255)) RETURNS VARCHAR(255)
    DETERMINISTIC
BEGIN
    DECLARE start_index INT;
    DECLARE end_index INT;
    DECLARE key_length INT;
    DECLARE key_index INT;
    DECLARE value_length INT;
    DECLARE extracted_value VARCHAR(255);

    -- Add the key structure to search for in the JSON string
    SET key_name = CONCAT('"', key_name, '":');
    SET key_length = CHAR_LENGTH(key_name);

    -- Locate the key within the JSON string
    SET key_index = LOCATE(key_name, json);

    -- If the key is not found, return NULL
    IF key_index = 0 THEN
        RETURN NULL;
    END IF;

    -- Set the starting index of the value
    SET start_index = key_index + key_length;

    -- Check if the value is a string (starts with a quote)
    IF SUBSTRING(json, start_index, 1) = '"' THEN
        -- Set the start index to the first character of the value (skipping the quote)
        SET start_index = start_index + 1;

        -- Find the end of the string value (the next quote)
        SET end_index = LOCATE('"', json, start_index);
        IF end_index = 0 THEN
            -- If there's no end quote, the JSON is malformed
            RETURN NULL;
        END IF;
    ELSE
        -- The value is not a string (e.g., a number, boolean, or null)
        -- Find the end of the value (either a comma or closing brace)
        SET end_index = LOCATE(',', json, start_index);
        IF end_index = 0 THEN
            SET end_index = LOCATE('}', json, start_index);
        END IF;
    END IF;

    -- Calculate the length of the extracted value
    SET value_length = end_index - start_index;

    -- Extract the value
    SET extracted_value = SUBSTRING(json, start_index, value_length);

    -- Return the extracted value without leading or trailing quotes
RETURN TRIM(BOTH '"' FROM extracted_value);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_remove_all_whitespace  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_remove_all_whitespace;

~-~-
CREATE FUNCTION fn_mamba_remove_all_whitespace(input_string TEXT) RETURNS TEXT
    DETERMINISTIC

BEGIN
  DECLARE cleaned_string TEXT;
  SET cleaned_string = input_string;

  -- Replace common whitespace characters
  SET cleaned_string = REPLACE(cleaned_string, CHAR(9), '');   -- Horizontal tab
  SET cleaned_string = REPLACE(cleaned_string, CHAR(10), '');  -- Line feed
  SET cleaned_string = REPLACE(cleaned_string, CHAR(13), '');  -- Carriage return
  SET cleaned_string = REPLACE(cleaned_string, CHAR(32), '');  -- Space
  -- SET cleaned_string = REPLACE(cleaned_string, CHAR(160), ''); -- Non-breaking space

RETURN TRIM(cleaned_string);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_remove_quotes  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_remove_quotes;

~-~-
CREATE FUNCTION fn_mamba_remove_quotes(original TEXT) RETURNS TEXT
    DETERMINISTIC
BEGIN
  DECLARE without_quotes TEXT;

  -- Replace both single and double quotes with nothing
  SET without_quotes = REPLACE(REPLACE(original, '"', ''), '''', '');

RETURN fn_mamba_remove_all_whitespace(without_quotes);
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_remove_special_characters  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP FUNCTION IF EXISTS fn_mamba_remove_special_characters;


~-~-
CREATE FUNCTION fn_mamba_remove_special_characters(input_text VARCHAR(255))
    RETURNS VARCHAR(255) DETERMINISTIC
BEGIN
    DECLARE modified_string VARCHAR(255);
    DECLARE special_chars VARCHAR(255);
    DECLARE char_index INT DEFAULT 1;
    DECLARE current_char CHAR(1);

    SET modified_string = input_text;
    -- SET special_chars = '!@#$%^&*?/,()"-=+£:;><ã';
    SET special_chars = '!@#$%^&*?/,()"-=+£:;><ã\|[]{}\'~`';

    WHILE char_index <= LENGTH(special_chars) DO
            SET current_char = SUBSTRING(special_chars, char_index, 1);
            SET modified_string = REPLACE(modified_string, current_char, '');
            SET char_index = char_index + 1;
        END WHILE;

    RETURN modified_string;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_functions_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_functions_in_schema;


~-~-
CREATE PROCEDURE sp_xf_system_drop_all_stored_functions_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN
    DELETE FROM `mysql`.`proc` WHERE `type` = 'FUNCTION' AND `db` = database_name; -- works in mysql before v.8

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_stored_procedures_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_procedures_in_schema;


~-~-
CREATE PROCEDURE sp_xf_system_drop_all_stored_procedures_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN

    DELETE FROM `mysql`.`proc` WHERE `type` = 'PROCEDURE' AND `db` = database_name; -- works in mysql before v.8

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_objects_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_objects_in_schema;


~-~-
CREATE PROCEDURE sp_xf_system_drop_all_objects_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN

    CALL sp_xf_system_drop_all_stored_functions_in_schema(database_name);
    CALL sp_xf_system_drop_all_stored_procedures_in_schema(database_name);
    CALL sp_mamba_system_drop_all_tables(database_name);
    # CALL sp_xf_system_drop_all_views_in_schema (database_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_system_drop_all_tables  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_system_drop_all_tables;


-- CREATE PROCEDURE sp_mamba_system_drop_all_tables(IN database_name CHAR(255) CHARACTER SET UTF8MB4)
~-~-
CREATE PROCEDURE sp_mamba_system_drop_all_tables()
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

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_scheduler_wrapper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_scheduler_wrapper;


~-~-
CREATE PROCEDURE sp_mamba_etl_scheduler_wrapper()

BEGIN

    DECLARE etl_ever_scheduled TINYINT(1);
    DECLARE incremental_mode TINYINT(1);

    SELECT COUNT(1)
    INTO etl_ever_scheduled
    FROM _mamba_etl_schedule;

    SELECT incremental_mode_switch
    INTO incremental_mode
    FROM _mamba_etl_user_settings;

    IF etl_ever_scheduled <= 1 OR incremental_mode = 0 THEN
        CALL sp_mamba_data_processing_drop_and_flatten();
    ELSE
        CALL sp_mamba_data_processing_increment_and_flatten();
    END IF;

    CALL sp_mamba_data_processing_etl();

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_schedule_table_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_schedule_table_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_schedule_table_create()
BEGIN

    CREATE TABLE IF NOT EXISTS _mamba_etl_schedule
    (
        id                         INT      NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
        start_time                 DATETIME NOT NULL DEFAULT NOW(),
        end_time                   DATETIME,
        next_schedule              DATETIME,
        execution_duration_seconds BIGINT,
        missed_schedule_by_seconds BIGINT,
        completion_status          ENUM ('SUCCESS', 'ERROR'),
        transaction_status         ENUM ('RUNNING', 'COMPLETED'),
        success_or_error_message   MEDIUMTEXT,

        INDEX mamba_idx_start_time (start_time),
        INDEX mamba_idx_end_time (end_time),
        INDEX mamba_idx_transaction_status (transaction_status),
        INDEX mamba_idx_completion_status (completion_status)
    )
        CHARSET = UTF8MB4;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_schedule  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_schedule;


~-~-
CREATE PROCEDURE sp_mamba_etl_schedule()

BEGIN

    DECLARE etl_execution_delay_seconds TINYINT(2) DEFAULT 0; -- 0 Seconds
    DECLARE interval_seconds INT;
    DECLARE start_time_seconds BIGINT;
    DECLARE end_time_seconds BIGINT;
    DECLARE time_now DATETIME;
    DECLARE txn_end_time DATETIME;
    DECLARE next_schedule_time DATETIME;
    DECLARE next_schedule_seconds BIGINT;
    DECLARE missed_schedule_seconds INT DEFAULT 0;
    DECLARE time_taken BIGINT;
    DECLARE etl_is_ready_to_run BOOLEAN DEFAULT FALSE;

    -- check if _mamba_etl_schedule is empty(new) or last transaction_status
    -- is 'COMPLETED' AND it was a 'SUCCESS' AND its 'end_time' was set.
    SET etl_is_ready_to_run = (SELECT COALESCE(
                                              (SELECT IF(end_time IS NOT NULL
                                                             AND transaction_status = 'COMPLETED'
                                                             AND completion_status = 'SUCCESS',
                                                         TRUE, FALSE)
                                               FROM _mamba_etl_schedule
                                               ORDER BY id DESC
                                               LIMIT 1), TRUE));

    IF etl_is_ready_to_run THEN

        SET time_now = NOW();
        SET start_time_seconds = UNIX_TIMESTAMP(time_now);

        INSERT INTO _mamba_etl_schedule(start_time, transaction_status)
        VALUES (time_now, 'RUNNING');

        SET @last_inserted_id = LAST_INSERT_ID();

        UPDATE _mamba_etl_user_settings
        SET last_etl_schedule_insert_id = @last_inserted_id
        WHERE TRUE
        ORDER BY id DESC
        LIMIT 1;

        -- Call ETL
        CALL sp_mamba_etl_scheduler_wrapper();

        SET txn_end_time = NOW();
        SET end_time_seconds = UNIX_TIMESTAMP(txn_end_time);

        SET time_taken = (end_time_seconds - start_time_seconds);


        SET interval_seconds = (SELECT etl_interval_seconds
                                FROM _mamba_etl_user_settings
                                ORDER BY id DESC
                                LIMIT 1);

        SET next_schedule_seconds = start_time_seconds + interval_seconds + etl_execution_delay_seconds;
        SET next_schedule_time = FROM_UNIXTIME(next_schedule_seconds);

        -- Run ETL immediately if schedule was missed (give allowance of 1 second)
        IF end_time_seconds > next_schedule_seconds THEN
            SET missed_schedule_seconds = end_time_seconds - next_schedule_seconds;
            SET next_schedule_time = FROM_UNIXTIME(end_time_seconds + 1);
        END IF;

        UPDATE _mamba_etl_schedule
        SET end_time                   = txn_end_time,
            next_schedule              = next_schedule_time,
            execution_duration_seconds = time_taken,
            missed_schedule_by_seconds = missed_schedule_seconds,
            completion_status          = 'SUCCESS',
            transaction_status         = 'COMPLETED'
        WHERE id = @last_inserted_id;

    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_setup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_setup;


~-~-
CREATE PROCEDURE sp_mamba_etl_setup(
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4,
    IN table_partition_number INT,
    IN incremental_mode_switch TINYINT(1),
    IN automatic_flattening_mode_switch TINYINT(1),
    IN etl_interval_seconds INT
)
BEGIN

    -- Setup ETL Error log Table
    CALL sp_mamba_etl_error_log();

    -- Setup ETL configurations
    CALL sp_mamba_etl_user_settings(concepts_locale,
                                    table_partition_number,
                                    incremental_mode_switch,
                                    automatic_flattening_mode_switch,
                                    etl_interval_seconds);

    -- create ETL schedule log table
    CALL sp_mamba_etl_schedule_table_create();

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_create;


~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_table_create(
    IN flat_encounter_table_name VARCHAR(60) CHARSET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @column_labels := NULL;

    SET @drop_table = CONCAT('DROP TABLE IF EXISTS `', flat_encounter_table_name, '`');

    SELECT GROUP_CONCAT(CONCAT(column_label, ' ', fn_mamba_get_datatype_for_concept(concept_datatype)) SEPARATOR ', ')
    INTO @column_labels
    FROM mamba_concept_metadata
    WHERE flat_table_name = flat_encounter_table_name
      AND concept_datatype IS NOT NULL;

    IF @column_labels IS NOT NULL THEN
        SET @create_table = CONCAT(
            'CREATE TABLE `', flat_encounter_table_name, '` (encounter_id INT NOT NULL, visit_id INT NULL, client_id INT NOT NULL, encounter_datetime DATETIME NOT NULL, location_id INT NULL, ', @column_labels, ', INDEX mamba_idx_encounter_id (encounter_id), INDEX mamba_idx_visit_id (visit_id), INDEX mamba_idx_client_id (client_id), INDEX mamba_idx_encounter_datetime (encounter_datetime), INDEX mamba_idx_location_id (location_id));');
    END IF;

    IF @column_labels IS NOT NULL THEN
        PREPARE deletetb FROM @drop_table;
        PREPARE createtb FROM @create_table;

        EXECUTE deletetb;
        EXECUTE createtb;

        DEALLOCATE PREPARE deletetb;
        DEALLOCATE PREPARE createtb;
    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_create_all;


~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_table_create_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name) FROM mamba_concept_metadata;

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

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_insert;


~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_table_insert(
    IN flat_encounter_table_name VARCHAR(60) CHARACTER SET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @tbl_name = flat_encounter_table_name;

    -- Precompute the concept metadata table to minimize repeated queries
    CREATE TEMPORARY TABLE mamba_temp_concept_metadata
    (
        id                 INT          NOT NULL,
        column_label       VARCHAR(255) NOT NULL,
        obs_value_column   VARCHAR(50),
        concept_uuid       CHAR(38)     NOT NULL,
        concept_answer_obs INT,

        INDEX mamba_idx_id (id),
        INDEX mamba_idx_column_label (column_label),
        INDEX mamba_idx_concept_uuid (concept_uuid),
        INDEX mamba_idx_concept_answer_obs (concept_answer_obs)
    )
        CHARSET = UTF8MB4;

    INSERT INTO mamba_temp_concept_metadata
    SELECT DISTINCT id,
                    column_label,
                    fn_mamba_get_obs_value_column(concept_datatype) AS obs_value_column,
                    concept_uuid,
                    concept_answer_obs
    FROM mamba_concept_metadata
    WHERE flat_table_name = @tbl_name;

    SELECT GROUP_CONCAT(DISTINCT
                        CONCAT(' MAX(CASE WHEN column_label = ''', column_label, ''' THEN ',
                               obs_value_column, ' END) ', column_label)
                        ORDER BY id ASC)
    INTO @column_labels
    FROM mamba_temp_concept_metadata;

    IF @column_labels IS NOT NULL THEN
        -- Insert for concept_answer_obs = 1
        SET @insert_stmt = CONCAT(
                'INSERT INTO `', @tbl_name,
                '` SELECT o.encounter_id, MAX(o.visit_id) AS visit_id, o.person_id, o.encounter_datetime, MAX(o.location_id) AS location_id, ',
                @column_labels, '
                FROM mamba_z_encounter_obs o
                    INNER JOIN mamba_temp_concept_metadata tcm
                    ON tcm.concept_uuid = o.obs_value_coded_uuid
                WHERE tcm.concept_answer_obs = 1
                AND tcm.obs_value_column IS NOT NULL
                AND o.obs_group_id IS NULL AND o.voided = 0
                GROUP BY o.encounter_id, o.person_id, o.encounter_datetime
                ORDER BY o.encounter_id ASC');
        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;

        -- Insert for concept_answer_obs != 1
        SET @insert_stmt = CONCAT(
                'INSERT INTO `', @tbl_name,
                '` SELECT o.encounter_id, MAX(o.visit_id) AS visit_id, o.person_id, o.encounter_datetime, MAX(o.location_id) AS location_id, ',
                @column_labels, '
                FROM mamba_z_encounter_obs o
                    INNER JOIN mamba_temp_concept_metadata tcm
                    ON tcm.concept_uuid = o.obs_question_uuid
                WHERE tcm.concept_answer_obs != 1
                AND tcm.obs_value_column IS NOT NULL
                AND o.obs_group_id IS NULL AND o.voided = 0
                GROUP BY o.encounter_id, o.person_id, o.encounter_datetime
                ORDER BY o.encounter_id ASC');
        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;
    END IF;

    DROP TEMPORARY TABLE IF EXISTS mamba_temp_concept_metadata;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_insert_all;


~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_table_insert_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name) FROM mamba_concept_metadata;

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

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_incremental_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Modified or New Encounters
DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_incremental_create_all;


~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_table_incremental_create_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT cm.flat_table_name
        FROM mamba_z_encounter_obs eo
                 INNER JOIN mamba_concept_metadata cm ON eo.encounter_type_uuid = cm.encounter_type_uuid
        WHERE eo.incremental_record = 1;

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

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_incremental_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Modified and New Encounters
DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_incremental_insert_all;


~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_table_incremental_insert_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT cm.flat_table_name
        FROM mamba_z_encounter_obs eo
                 INNER JOIN mamba_concept_metadata cm ON eo.encounter_type_uuid = cm.encounter_type_uuid
        WHERE eo.incremental_record = 1;

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

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_obs_group_table_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_obs_group_table_create;

~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_obs_group_table_create(
    IN flat_encounter_table_name VARCHAR(60) CHARSET UTF8MB4,
    obs_group_concept_name VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @column_labels := NULL;
    SET @tbl_obs_group_name = CONCAT(LEFT(flat_encounter_table_name, 50), '_', obs_group_concept_name); -- TODO: 50 + 12 to make 62

    SET @drop_table = CONCAT('DROP TABLE IF EXISTS `', @tbl_obs_group_name, '`');

    SELECT GROUP_CONCAT(CONCAT(column_label, ' ', fn_mamba_get_datatype_for_concept(concept_datatype)) SEPARATOR ', ')
    INTO @column_labels
    FROM mamba_concept_metadata cm
             INNER JOIN
         (SELECT DISTINCT obs_question_concept_id
          FROM mamba_z_encounter_obs eo
                   INNER JOIN mamba_obs_group og
                              on eo.obs_group_id = og.obs_id
          WHERE obs_group_id IS NOT NULL
            AND og.obs_group_concept_name = obs_group_concept_name) eo
         ON cm.concept_id = eo.obs_question_concept_id
    WHERE flat_table_name = flat_encounter_table_name
      AND concept_datatype IS NOT NULL;

    IF @column_labels IS NOT NULL THEN
        SET @create_table = CONCAT(
                'CREATE TABLE `', @tbl_obs_group_name,
                '` (encounter_id INT NOT NULL, visit_id INT NULL, client_id INT NOT NULL, encounter_datetime DATETIME NOT NULL, location_id INT NULL, ',
                @column_labels,
                ', INDEX mamba_idx_encounter_id (encounter_id), INDEX mamba_idx_visit_id (visit_id), INDEX mamba_idx_client_id (client_id), INDEX mamba_idx_encounter_datetime (encounter_datetime), INDEX mamba_idx_location_id (location_id));');
    END IF;

    IF @column_labels IS NOT NULL THEN
        PREPARE deletetb FROM @drop_table;
        PREPARE createtb FROM @create_table;

        EXECUTE deletetb;
        EXECUTE createtb;

        DEALLOCATE PREPARE deletetb;
        DEALLOCATE PREPARE createtb;
    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_obs_group_table_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_obs_group_table_create_all;

~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_obs_group_table_create_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;
    DECLARE obs_name CHAR(50) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT 0;

    DECLARE cursor_flat_tables CURSOR FOR
    SELECT DISTINCT(flat_table_name) FROM mamba_concept_metadata;

    DECLARE cursor_obs_group_tables CURSOR FOR
    SELECT DISTINCT(obs_group_concept_name) FROM mamba_obs_group;

    -- DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cursor_flat_tables;

        REPEAT
            FETCH cursor_flat_tables INTO tbl_name;
                IF NOT done THEN
                    OPEN cursor_obs_group_tables;
                        block2: BEGIN
                            DECLARE doneobs_name INT DEFAULT 0;
                            DECLARE firstobs_name varchar(255) DEFAULT '';
                            DECLARE i int DEFAULT 1;
                            DECLARE CONTINUE HANDLER FOR NOT FOUND SET doneobs_name = 1;

                            REPEAT
                                FETCH cursor_obs_group_tables INTO obs_name;

                                    IF i = 1 THEN
                                        SET firstobs_name = obs_name;
                                    END IF;

                                    CALL sp_mamba_flat_encounter_obs_group_table_create(tbl_name,obs_name);
                                    SET i = i + 1;

                                UNTIL doneobs_name
                            END REPEAT;

                            CALL sp_mamba_flat_encounter_obs_group_table_create(tbl_name,firstobs_name);
                        END block2;
                    CLOSE cursor_obs_group_tables;
                END IF;
            UNTIL done
        END REPEAT;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_obs_group_table_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_obs_group_table_insert;

~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_obs_group_table_insert(
    IN flat_encounter_table_name VARCHAR(60) CHARACTER SET UTF8MB4,
    obs_group_concept_name VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @tbl_name = flat_encounter_table_name;

    SET @tbl_obs_group_name = CONCAT(LEFT(@tbl_name, 50), '_', obs_group_concept_name); -- TODO: 50 + 12 to make 62

    SET @old_sql = (SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = @tbl_name
                      AND TABLE_SCHEMA = Database());

    SELECT GROUP_CONCAT(DISTINCT
                        CONCAT(' MAX(CASE WHEN column_label = ''', column_label, ''' THEN ',
                               fn_mamba_get_obs_value_column(concept_datatype), ' END) ', column_label)
                        ORDER BY id ASC)
    INTO @column_labels
    FROM mamba_concept_metadata cm
             INNER JOIN
         (SELECT DISTINCT obs_question_concept_id
          FROM mamba_z_encounter_obs eo
                   INNER JOIN mamba_obs_group og
                              on eo.obs_group_id = og.obs_id
          WHERE obs_group_id IS NOT NULL
            AND og.obs_group_concept_name = obs_group_concept_name) eo
         ON cm.concept_id = eo.obs_question_concept_id
    WHERE flat_table_name = @tbl_name;

    IF @column_labels IS NOT NULL THEN
        IF (SELECT count(*) FROM information_schema.tables WHERE table_name = @tbl_obs_group_name) > 0 THEN
            SET @insert_stmt = CONCAT(
                    'INSERT INTO `', @tbl_obs_group_name,
                    '` SELECT eo.encounter_id, MAX(eo.visit_id) AS visit_id, eo.person_id, eo.encounter_datetime, MAX(eo.location_id) AS location_id, ',
                    @column_labels, '
                    FROM mamba_z_encounter_obs eo
                        INNER JOIN mamba_concept_metadata cm
                        ON IF(cm.concept_answer_obs=1, cm.concept_uuid=eo.obs_value_coded_uuid, cm.concept_uuid=eo.obs_question_uuid)
                    WHERE  cm.flat_table_name = ''', @tbl_name, '''
                    AND eo.encounter_type_uuid = cm.encounter_type_uuid
                    AND eo.obs_group_id IS NOT NULL
                    GROUP BY eo.encounter_id, eo.person_id, eo.encounter_datetime,eo.obs_group_id;');
        END IF;
    END IF;

    IF @column_labels IS NOT NULL THEN
        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;
    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_obs_group_table_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_obs_group_table_insert_all;

~-~-
CREATE PROCEDURE sp_mamba_flat_encounter_obs_group_table_insert_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;
    DECLARE obs_name CHAR(50) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT 0;

    DECLARE cursor_flat_tables CURSOR FOR
    SELECT DISTINCT(flat_table_name) FROM mamba_concept_metadata;

    DECLARE cursor_obs_group_tables CURSOR FOR
    SELECT DISTINCT(obs_group_concept_name) FROM mamba_obs_group;

    -- DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cursor_flat_tables;

        REPEAT
            FETCH cursor_flat_tables INTO tbl_name;
                IF NOT done THEN
                    OPEN cursor_obs_group_tables;
                        block2: BEGIN
                            DECLARE doneobs_name INT DEFAULT 0;
                            DECLARE firstobs_name varchar(255) DEFAULT '';
                            DECLARE i int DEFAULT 1;
                            DECLARE CONTINUE HANDLER FOR NOT FOUND SET doneobs_name = 1;

                            REPEAT
                                FETCH cursor_obs_group_tables INTO obs_name;

                                    IF i = 1 THEN
                                        SET firstobs_name = obs_name;
                                    END IF;

                                    CALL sp_mamba_flat_encounter_obs_group_table_insert(tbl_name,obs_name);
                                    SET i = i + 1;

                                UNTIL doneobs_name
                            END REPEAT;

                            CALL sp_mamba_flat_encounter_obs_group_table_insert(tbl_name,firstobs_name);
                        END block2;
                    CLOSE cursor_obs_group_tables;
            END IF;
                        UNTIL done
        END REPEAT;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_multiselect_values_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_mamba_multiselect_values_update`;


~-~-
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

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_extract_report_definition_metadata  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_extract_report_definition_metadata;


~-~-
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

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_load_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_load_agegroup;


~-~-
CREATE PROCEDURE sp_mamba_load_agegroup()
BEGIN
    DECLARE age INT DEFAULT 0;
    WHILE age <= 120
        DO
            INSERT INTO mamba_dim_agegroup(age, datim_agegroup, normal_agegroup)
            VALUES (age, fn_mamba_calculate_agegroup(age), IF(age < 15, '<15', '15+'));
            SET age = age + 1;
        END WHILE;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_get_report_column_names  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_get_report_column_names;


~-~-
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

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_generate_report_wrapper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_generate_report_wrapper;


~-~-
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

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_write_automated_json_config  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_write_automated_json_config;


~-~-
CREATE PROCEDURE sp_mamba_write_automated_json_config()
BEGIN

    DECLARE done INT DEFAULT FALSE;
    DECLARE jsonData JSON;
    DECLARE cur CURSOR FOR
        SELECT json_data FROM mamba_flat_table_config;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

        SET @report_data = '{"flat_report_metadata":[';

        OPEN cur;
        FETCH cur INTO jsonData;

        IF NOT done THEN
                    SET @report_data = CONCAT(@report_data, jsonData);
        FETCH cur INTO jsonData; -- Fetch next record after the first one
        END IF;

                read_loop: LOOP
                    IF done THEN
                        LEAVE read_loop;
        END IF;

                    SET @report_data = CONCAT(@report_data, ',', jsonData);
        FETCH cur INTO jsonData;
        END LOOP;
        CLOSE cur;

        SET @report_data = CONCAT(@report_data, ']}');

        CALL sp_mamba_extract_report_metadata(@report_data, 'mamba_concept_metadata');

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_locale_insert_helper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_locale_insert_helper;


~-~-
CREATE PROCEDURE sp_mamba_locale_insert_helper(
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4
)
BEGIN

    SET @conc_locale = concepts_locale;
    SET @insert_stmt = CONCAT('INSERT INTO mamba_dim_locale (locale) VALUES (''', @conc_locale, ''');');

    PREPARE inserttbl FROM @insert_stmt;
    EXECUTE inserttbl;
    DEALLOCATE PREPARE inserttbl;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_extract_report_column_names  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_extract_report_column_names;


~-~-
CREATE PROCEDURE sp_mamba_extract_report_column_names()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE proc_name VARCHAR(255);
    DECLARE cur CURSOR FOR SELECT DISTINCT report_columns_procedure_name FROM mamba_dim_report_definition;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO proc_name;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Fetch the parameters for the procedure and provide empty string values for each
        SET @params := NULL;

        SELECT GROUP_CONCAT('\'\'' SEPARATOR ', ')
        INTO @params
        FROM mamba_dim_report_definition_parameters rdp
                 INNER JOIN mamba_dim_report_definition rd on rdp.report_id = rd.report_id
        WHERE rd.report_columns_procedure_name = proc_name;

        IF @params IS NULL THEN
            SET @procedure_call = CONCAT('CALL ', proc_name, '();');
        ELSE
            SET @procedure_call = CONCAT('CALL ', proc_name, '(', @params, ');');
        END IF;

        PREPARE stmt FROM @procedure_call;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_truncate_tables_before_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_truncate_tables_before_incremental;


~-~-
CREATE PROCEDURE sp_mamba_truncate_tables_before_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_truncate_tables_before_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_truncate_tables_before_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- CALL sp_mamba_flat_table_config_incremental_truncate();
-- TRUNCATE TABLE mamba_concept_metadata;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_reset_incremental_update_flag  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Given a table name, this procedure will reset the incremental_record column to 0 for all rows where the incremental_record is 1.
-- This is useful when we want to re-run the incremental updates for a table.

DROP PROCEDURE IF EXISTS sp_mamba_reset_incremental_update_flag;


~-~-
CREATE PROCEDURE sp_mamba_reset_incremental_update_flag(
    IN table_name VARCHAR(60) CHARACTER SET UTF8MB4
)
BEGIN

    SET @tbl_name = table_name;

    SET @update_stmt =
            CONCAT('UPDATE ', @tbl_name, ' SET incremental_record = 0 WHERE incremental_record = 1');
    PREPARE updatetb FROM @update_stmt;
    EXECUTE updatetb;
    DEALLOCATE PREPARE updatetb;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_reset_incremental_update_flag_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_reset_incremental_update_flag_all;


~-~-
CREATE PROCEDURE sp_mamba_reset_incremental_update_flag_all()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_reset_incremental_update_flag_all', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_reset_incremental_update_flag_all', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Given a table name, this procedure will reset the incremental_record column to 0 for all rows where the incremental_record is 1.
-- This is useful when we want to re-run the incremental updates for a table.

CALL sp_mamba_reset_incremental_update_flag('mamba_dim_location');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_patient_identifier_type');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_concept_datatype');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_concept_name');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_concept');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_concept_answer');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_encounter_type');
CALL sp_mamba_reset_incremental_update_flag('mamba_flat_table_config');
CALL sp_mamba_reset_incremental_update_flag('mamba_concept_metadata');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_encounter');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_person_name');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_person');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_person_attribute_type');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_person_attribute');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_person_address');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_users');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_relationship');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_patient_identifier');
CALL sp_mamba_reset_incremental_update_flag('mamba_dim_orders');
CALL sp_mamba_reset_incremental_update_flag('mamba_z_encounter_obs');

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_concept_metadata_create();
CALL sp_mamba_concept_metadata_insert();
CALL sp_mamba_concept_metadata_missing_columns_insert(); -- Update/insert table column metadata configs without table_columns json
CALL sp_mamba_concept_metadata_update();
CALL sp_mamba_concept_metadata_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_create;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_concept_metadata
(
    id                  INT          NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    concept_id          INT          NULL,
    concept_uuid        CHAR(38)     NOT NULL,
    concept_name        VARCHAR(255) NULL,
    column_number       INT,
    column_label        VARCHAR(60)  NOT NULL,
    concept_datatype    VARCHAR(255) NULL,
    concept_answer_obs  TINYINT      NOT NULL DEFAULT 0,
    report_name         VARCHAR(255) NOT NULL,
    flat_table_name     VARCHAR(60)  NULL,
    encounter_type_uuid CHAR(38)     NOT NULL,
    row_num             INT          NULL     DEFAULT 1,
    incremental_record  INT          NOT NULL DEFAULT 0,

    INDEX mamba_idx_concept_id (concept_id),
    INDEX mamba_idx_concept_uuid (concept_uuid),
    INDEX mamba_idx_encounter_type_uuid (encounter_type_uuid),
    INDEX mamba_idx_row_num (row_num),
    INDEX mamba_idx_concept_datatype (concept_datatype),
    INDEX mamba_idx_flat_table_name (flat_table_name),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_insert;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @is_incremental = 0;
SET @report_data = fn_mamba_generate_json_from_mamba_flat_table_config(@is_incremental);
CALL sp_mamba_concept_metadata_insert_helper(@is_incremental, @report_data, 'mamba_concept_metadata');


-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_insert_helper  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_insert_helper;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_insert_helper(
    IN is_incremental TINYINT(1),
    IN report_data JSON,
    IN metadata_table VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    DECLARE is_incremental_record TINYINT(1) DEFAULT 0;

    SET session group_concat_max_len = 20000;

    SELECT DISTINCT(table_partition_number)
    INTO @table_partition_number
    FROM _mamba_etl_user_settings;

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

            -- if is_incremental = 1, delete records (if they exist) from mamba_concept_metadata table with encounter_type_uuid = @encounter_type
            IF is_incremental = 1 THEN

                SET is_incremental_record = 1;
                SET @delete_query = CONCAT('DELETE FROM mamba_concept_metadata WHERE encounter_type_uuid = ''',
                                           JSON_UNQUOTE(@encounter_type), '''');

                PREPARE stmt FROM @delete_query;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            END IF;

            IF @column_keys_array_len = 0 THEN

                INSERT INTO mamba_concept_metadata
                (report_name,
                 flat_table_name,
                 encounter_type_uuid,
                 column_label,
                 concept_uuid,
                 incremental_record)
                VALUES (JSON_UNQUOTE(@report_name),
                        JSON_UNQUOTE(@flat_table_name),
                        JSON_UNQUOTE(@encounter_type),
                        'AUTO-GENERATE',
                        'AUTO-GENERATE',
                        is_incremental_record);
            ELSE

                SET @col_count = 0;
                SET @table_name = JSON_UNQUOTE(@flat_table_name);
                SET @current_table_count = 1;

                WHILE @col_count < @column_keys_array_len
                    DO
                        SELECT JSON_EXTRACT(@column_keys_array, CONCAT('$[', @col_count, ']')) INTO @field_name;
                        SELECT JSON_EXTRACT(@column_array, CONCAT('$.', @field_name)) INTO @concept_uuid;

                        IF @col_count > @table_partition_number THEN

                            SET @table_name = CONCAT(JSON_UNQUOTE(@flat_table_name), '_', @current_table_count);
                            SET @current_table_count = @current_table_count;

                            INSERT INTO mamba_concept_metadata
                            (report_name,
                             flat_table_name,
                             encounter_type_uuid,
                             column_label,
                             concept_uuid,
                             incremental_record)
                            VALUES (JSON_UNQUOTE(@report_name),
                                    JSON_UNQUOTE(@table_name),
                                    JSON_UNQUOTE(@encounter_type),
                                    JSON_UNQUOTE(@field_name),
                                    JSON_UNQUOTE(@concept_uuid),
                                    is_incremental_record);

                        ELSE
                            INSERT INTO mamba_concept_metadata
                            (report_name,
                             flat_table_name,
                             encounter_type_uuid,
                             column_label,
                             concept_uuid,
                             incremental_record)
                            VALUES (JSON_UNQUOTE(@report_name),
                                    JSON_UNQUOTE(@flat_table_name),
                                    JSON_UNQUOTE(@encounter_type),
                                    JSON_UNQUOTE(@field_name),
                                    JSON_UNQUOTE(@concept_uuid),
                                    is_incremental_record);
                        END IF;


                        SET @col_count = @col_count + 1;

                    END WHILE;
            END IF;

            SET @report_count = @report_count + 1;
        END WHILE;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_update;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the Concept datatypes, concept_name and concept_id based on given locale
UPDATE mamba_concept_metadata md
    INNER JOIN mamba_dim_concept c
    ON md.concept_uuid = c.uuid
SET md.concept_datatype = c.datatype,
    md.concept_id       = c.concept_id,
    md.concept_name     = c.name
WHERE md.id > 0;

-- Update to True if this field is an obs answer to an obs Question
UPDATE mamba_concept_metadata md
    INNER JOIN mamba_dim_concept_answer ca
    ON md.concept_id = ca.answer_concept
SET md.concept_answer_obs = 1
WHERE md.id > 0
  AND md.concept_id IN (SELECT DISTINCT ca.concept_id
                        FROM mamba_dim_concept_answer ca);

-- Update to for multiple selects/dropdowns/options this field is an obs answer to an obs Question
-- TODO: check this implementation here
UPDATE mamba_concept_metadata md
SET md.concept_answer_obs = 1
WHERE md.id > 0
  and concept_datatype = 'N/A';

-- Update row number
SET @row_number = 0;
SET @prev_flat_table_name = NULL;
SET @prev_concept_id = NULL;

UPDATE mamba_concept_metadata md
    INNER JOIN (SELECT flat_table_name,
                       concept_id,
                       id,
                       @row_number := CASE
                                          WHEN @prev_flat_table_name = flat_table_name
                                              AND @prev_concept_id = concept_id
                                              THEN @row_number + 1
                                          ELSE 1
                           END AS num,
                       @prev_flat_table_name := flat_table_name,
                       @prev_concept_id := concept_id
                FROM mamba_concept_metadata
                ORDER BY flat_table_name, concept_id, id) m ON md.id = m.id
SET md.row_num = num
WHERE md.id > 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_cleanup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_cleanup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_cleanup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- delete un wanted rows after inserting columns that were not given in the .json config file into the meta data table,
-- all rows with 'AUTO-GENERATE' are not used anymore. Delete them/1
DELETE
FROM mamba_concept_metadata
WHERE concept_uuid = 'AUTO-GENERATE'
  AND column_label = 'AUTO-GENERATE';

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_missing_columns_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_missing_columns_insert;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_missing_columns_insert()
BEGIN

    DECLARE encounter_type_uuid_value CHAR(38);
    DECLARE report_name_val VARCHAR(100);
    DECLARE encounter_type_id_val INT;
    DECLARE flat_table_name_val VARCHAR(255);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounters CURSOR FOR
        SELECT DISTINCT(encounter_type_uuid), m.report_name, m.flat_table_name, et.encounter_type_id
        FROM mamba_concept_metadata m
                 INNER JOIN mamba_dim_encounter_type et ON m.encounter_type_uuid = et.uuid
        WHERE et.retired = 0
          AND m.concept_uuid = 'AUTO-GENERATE'
          AND m.column_label = 'AUTO-GENERATE';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_encounters;
    computations_loop:
    LOOP
        FETCH cursor_encounters
            INTO encounter_type_uuid_value, report_name_val, flat_table_name_val, encounter_type_id_val;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO mamba_concept_metadata
                (
                    report_name,
                    flat_table_name,
                    encounter_type_uuid,
                    column_label,
                    concept_uuid
                )
                SELECT
                    ''', report_name_val, ''',
                ''', flat_table_name_val, ''',
                ''', encounter_type_uuid_value, ''',
                field_name,
                concept_uuid,
                FROM (
                     SELECT
                          DISTINCT et.encounter_type_id,
                          c.auto_table_column_name AS field_name,
                          c.uuid AS concept_uuid
                     FROM openmrs.obs o
                          INNER JOIN openmrs.encounter e
                            ON e.encounter_id = o.encounter_id
                          INNER JOIN mamba_dim_encounter_type et
                            ON e.encounter_type = et.encounter_type_id
                          INNER JOIN mamba_dim_concept c
                            ON o.concept_id = c.concept_id
                     WHERE et.encounter_type_id = ''', encounter_type_id_val, '''
                       AND et.retired = 0
                ) mamba_missing_concept;
            ');

        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;

    END LOOP computations_loop;
    CLOSE cursor_encounters;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_incremental;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_concept_metadata_incremental_insert();
CALL sp_mamba_concept_metadata_missing_columns_incremental_insert(); -- Update/insert table column metadata configs without table_columns json
CALL sp_mamba_concept_metadata_incremental_update();
CALL sp_mamba_concept_metadata_incremental_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @is_incremental = 1;
SET @report_data = fn_mamba_generate_json_from_mamba_flat_table_config(@is_incremental);
CALL sp_mamba_concept_metadata_insert_helper(@is_incremental, @report_data, 'mamba_concept_metadata');


-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the Concept datatypes, concept_name and concept_id based on given locale
UPDATE mamba_concept_metadata md
    INNER JOIN mamba_dim_concept c
    ON md.concept_uuid = c.uuid
SET md.concept_datatype = c.datatype,
    md.concept_id       = c.concept_id,
    md.concept_name     = c.name
WHERE md.incremental_record = 1;

-- Update to True if this field is an obs answer to an obs Question
UPDATE mamba_concept_metadata md
    INNER JOIN mamba_dim_concept_answer ca
    ON md.concept_id = ca.answer_concept
SET md.concept_answer_obs = 1
WHERE md.incremental_record = 1
  AND md.concept_id IN (SELECT DISTINCT ca.concept_id
                        FROM mamba_dim_concept_answer ca);

-- Update to for multiple selects/dropdowns/options this field is an obs answer to an obs Question
-- TODO: check this implementation here
UPDATE mamba_concept_metadata md
SET md.concept_answer_obs = 1
WHERE md.incremental_record = 1
  and concept_datatype = 'N/A';

-- Update row number
SET @row_number = 0;
SET @prev_flat_table_name = NULL;
SET @prev_concept_id = NULL;

UPDATE mamba_concept_metadata md
    INNER JOIN (SELECT flat_table_name,
                       concept_id,
                       id,
                       @row_number := CASE
                                          WHEN @prev_flat_table_name = flat_table_name
                                              AND @prev_concept_id = concept_id
                                              THEN @row_number + 1
                                          ELSE 1
                           END AS num,
                       @prev_flat_table_name := flat_table_name,
                       @prev_concept_id := concept_id
                FROM mamba_concept_metadata
                -- WHERE incremental_record = 1
                ORDER BY flat_table_name, concept_id, id) m ON md.id = m.id
SET md.row_num = num
WHERE md.incremental_record = 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_incremental_cleanup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_concept_metadata_incremental_cleanup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_concept_metadata_incremental_cleanup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- delete un wanted rows after inserting columns that were not given in the .json config file into the meta data table,
-- all rows with 'AUTO-GENERATE' are not used anymore. Delete them/1
DELETE
FROM mamba_concept_metadata
WHERE incremental_record = 1
  AND concept_uuid = 'AUTO-GENERATE'
  AND column_label = 'AUTO-GENERATE';

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_concept_metadata_missing_columns_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_concept_metadata_missing_columns_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_concept_metadata_missing_columns_incremental_insert()
BEGIN

    DECLARE encounter_type_uuid_value CHAR(38);
    DECLARE report_name_val VARCHAR(100);
    DECLARE encounter_type_id_val INT;
    DECLARE flat_table_name_val VARCHAR(255);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounters CURSOR FOR
        SELECT DISTINCT(encounter_type_uuid), m.report_name, m.flat_table_name, et.encounter_type_id
        FROM mamba_concept_metadata m
                 INNER JOIN mamba_dim_encounter_type et ON m.encounter_type_uuid = et.uuid
        WHERE et.retired = 0
          AND m.concept_uuid = 'AUTO-GENERATE'
          AND m.column_label = 'AUTO-GENERATE'
          AND m.incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_encounters;
    computations_loop:
    LOOP
        FETCH cursor_encounters
            INTO encounter_type_uuid_value, report_name_val, flat_table_name_val, encounter_type_id_val;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO mamba_concept_metadata
                (
                    report_name,
                    flat_table_name,
                    encounter_type_uuid,
                    column_label,
                    concept_uuid,
                    incremental_record
                )
                SELECT
                    ''', report_name_val, ''',
                ''', flat_table_name_val, ''',
                ''', encounter_type_uuid_value, ''',
                field_name,
                concept_uuid,
                1
                FROM (
                     SELECT
                          DISTINCT et.encounter_type_id,
                          c.auto_table_column_name AS field_name,
                          c.uuid AS concept_uuid
                     FROM openmrs.obs o
                          INNER JOIN openmrs.encounter e
                            ON e.encounter_id = o.encounter_id
                          INNER JOIN mamba_dim_encounter_type et
                            ON e.encounter_type = et.encounter_type_id
                          INNER JOIN mamba_dim_concept c
                            ON o.concept_id = c.concept_id
                     WHERE et.encounter_type_id = ''', encounter_type_id_val, '''
                       AND et.retired = 0
                ) mamba_missing_concept;
            ');

        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;

    END LOOP computations_loop;
    CLOSE cursor_encounters;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_create;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_flat_table_config
(
    id                   INT           NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    encounter_type_id    INT           NOT NULL UNIQUE,
    report_name          VARCHAR(100)  NOT NULL,
    table_json_data      JSON          NOT NULL,
    table_json_data_hash CHAR(32)      NULL,
    encounter_type_uuid  CHAR(38)      NOT NULL,
    incremental_record   INT DEFAULT 0 NOT NULL COMMENT 'Whether `table_json_data` has been modified or not',

    INDEX mamba_idx_encounter_type_id (encounter_type_id),
    INDEX mamba_idx_report_name (report_name),
    INDEX mamba_idx_table_json_data_hash (table_json_data_hash),
    INDEX mamba_idx_uuid (encounter_type_uuid),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_insert_helper_manual  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- manually extracts user given flat table config file json into the mamba_flat_table_config table
-- this data together with automatically extracted flat table data is inserted into the mamba_flat_table_config table
-- later it is processed by the 'fn_mamba_generate_report_array_from_automated_json_table' function
-- into the @report_data variable inside the compile-mysql.sh script

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_insert_helper_manual;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_insert_helper_manual(
    IN report_data MEDIUMTEXT CHARACTER SET UTF8MB4
)
BEGIN

    DECLARE report_count INT DEFAULT 0;
    DECLARE report_array_len INT;
    DECLARE report_enc_type_id INT DEFAULT NULL;
    DECLARE report_enc_type_uuid VARCHAR(50);
    DECLARE report_enc_name VARCHAR(500);

    SET session group_concat_max_len = 20000;

    SELECT JSON_EXTRACT(report_data, '$.flat_report_metadata') INTO @report_array;
    SELECT JSON_LENGTH(@report_array) INTO report_array_len;

    WHILE report_count < report_array_len
        DO

            SELECT JSON_EXTRACT(@report_array, CONCAT('$[', report_count, ']')) INTO @report_data_item;
            SELECT JSON_EXTRACT(@report_data_item, '$.report_name') INTO report_enc_name;
            SELECT JSON_EXTRACT(@report_data_item, '$.encounter_type_uuid') INTO report_enc_type_uuid;

            SET report_enc_type_uuid = JSON_UNQUOTE(report_enc_type_uuid);

            SET report_enc_type_id = (SELECT DISTINCT et.encounter_type_id
                                      FROM mamba_dim_encounter_type et
                                      WHERE et.uuid = report_enc_type_uuid
                                      LIMIT 1);

            IF report_enc_type_id IS NOT NULL THEN
                INSERT INTO mamba_flat_table_config
                (report_name,
                 encounter_type_id,
                 table_json_data,
                 encounter_type_uuid)
                VALUES (JSON_UNQUOTE(report_enc_name),
                        report_enc_type_id,
                        @report_data_item,
                        report_enc_type_uuid);
            END IF;

            SET report_count = report_count + 1;

        END WHILE;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_insert_helper_auto  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_insert_helper_auto;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_insert_helper_auto()
main_block:
BEGIN

    DECLARE encounter_type_name CHAR(50) CHARACTER SET UTF8MB4;
    DECLARE is_automatic_flattening TINYINT(1);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounter_type_name CURSOR FOR
        SELECT DISTINCT et.name
        FROM openmrs.obs o
                 INNER JOIN openmrs.encounter e ON e.encounter_id = o.encounter_id
                 INNER JOIN mamba_dim_encounter_type et ON e.encounter_type = et.encounter_type_id
        WHERE et.encounter_type_id NOT IN (SELECT DISTINCT tc.encounter_type_id from mamba_flat_table_config tc)
          AND et.retired = 0;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SELECT DISTINCT(automatic_flattening_mode_switch)
    INTO is_automatic_flattening
    FROM _mamba_etl_user_settings;

    -- If auto-flattening is not switched on, do nothing
    IF is_automatic_flattening = 0 THEN
        LEAVE main_block;
    END IF;

    OPEN cursor_encounter_type_name;
    computations_loop:
    LOOP
        FETCH cursor_encounter_type_name INTO encounter_type_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO mamba_flat_table_config(report_name, encounter_type_id, table_json_data, encounter_type_uuid)
                    SELECT
                        name,
                        encounter_type_id,
                         CONCAT(''{'',
                            ''"report_name": "'', name, ''", '',
                            ''"flat_table_name": "'', table_name, ''", '',
                            ''"encounter_type_uuid": "'', uuid, ''", '',
                            ''"table_columns": '', json_obj, '' '',
                            ''}'') AS table_json_data,
                        encounter_type_uuid
                    FROM (
                        SELECT DISTINCT
                            et.name,
                            encounter_type_id,
                            et.auto_flat_table_name AS table_name,
                            et.uuid, ',
                '(
                SELECT DISTINCT CONCAT(''{'', GROUP_CONCAT(CONCAT(''"'', name, ''":"'', uuid, ''"'') SEPARATOR '','' ),''}'') x
                FROM (
                        SELECT
                            DISTINCT et.encounter_type_id,
                            c.auto_table_column_name AS name,
                            c.uuid
                        FROM openmrs.obs o
                        INNER JOIN openmrs.encounter e
                                  ON e.encounter_id = o.encounter_id
                        INNER JOIN mamba_dim_encounter_type et
                                  ON e.encounter_type = et.encounter_type_id
                        INNER JOIN mamba_dim_concept c
                                  ON o.concept_id = c.concept_id
                        WHERE et.name = ''', encounter_type_name, '''
                                    AND et.retired = 0
                                ) json_obj
                        ) json_obj,
                       et.uuid as encounter_type_uuid
                    FROM mamba_dim_encounter_type et
                    INNER JOIN openmrs.encounter e
                        ON e.encounter_type = et.encounter_type_id
                    WHERE et.name = ''', encounter_type_name, '''
                ) X  ;   ');
        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;
    END LOOP computations_loop;
    CLOSE cursor_encounter_type_name;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_insert;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @report_data = '{"flat_report_metadata":[{
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
        "linked_to_art": "a40d8bc4-56b8-4f28-a1dd-412da5cf20ed"
      }
},{
  "report_name": "PMTCT ANC visit",
  "flat_table_name": "mamba_flat_encounter_pmtct_anc",
  "encounter_type_uuid": "677d1a80-dbbe-4399-be34-aa7f54f11405",
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
},{
  "report_name": "Clinical Encounter",
  "flat_table_name": "mamba_flat_encounter_clinical_visit",
  "encounter_type_uuid": "cb0a65a7-0587-477e-89b9-cf2fd144f1d4",
  "concepts_locale": "en",
  "table_columns": {
    "nutritional_support": "5484AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referrals_ordered": "1272AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "name_of_health_care_provider": "1473AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "general_patient_note": "165095AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "sti_screening": "c4f81292-61a3-4561-a4ae-78be7d16d928",
    "reason_for_referral_text": "164359AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "sign_symptom_start_date": "1730AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "what_was_the_tb_screening_outcome": "c0661c0f-348b-4941-812b-c531a0a67f2e",
    "was_the_patient_screened_for_tb": "f8868467-bd15-4576-9da8-bfb8ef64ea17",
    "type_of_visit": "8a9809e9-8a0b-4e0e-b1f6-80b0cbbe361b",
    "currently_taking_tuberculosis_prophylaxis": "166449AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_last_normal_menstrual_period": "166079AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_tuberculosis_prophylaxis_ended": "163284AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_started_on_tuberculosis_prophylaxis": "162320AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tuberculosis_treatment_end_date": "159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "isoniazid_adherence": "161653AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "reason_not_on_family_planning": "160575AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "chief_complaint_text": "160531AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "currently_on_tuberculosis_treatment": "159798AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tuberculosis_drug_treatment_start_date": "1113AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "currently_breastfeeding_child": "5632AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "estimated_date_of_confinement": "5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "return_visit_date": "5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "currently_pregnant": "1434AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "scheduled_visit": "1246AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "new_complaints": "1154AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "method_of_family_planning": "374AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cervical_cancer_screening_status": "e5e99fc7-ff2d-4306-aefd-b87a07fc9ab4",
    "antenatal_profile_screening_done": "975f11e5-7471-4e57-bba7-d3ee358ef7ea",
    "intend_to_conceive_in_the_next_three_months": "9109b9f3-8176-4d2f-b47d-82630dcc02ce",
    "reason_for_poor_treatment_adherence": "160582AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_last_cervical_cancer_screening": "165429AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cotrimoxazole_prophylaxis_start_date": "164361AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_reported_hiv_viral_load": "163281AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "family_planning_status": "160653AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_reported_cd4_count": "159376AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cd4_count": "5497AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cryptococcal_treatment_plan": "1277AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "pcp_prophylaxis_plan": "1261AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "current_who_hiv_stage": "5356AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cd4_panel": "657AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hiv_viral_load_test_qualitiative": "686dc1b2-68b5-4024-b311-bd2f5e3ce394",
    "fluconazole_start_date": "5ac4300a-5e19-45c8-8692-31a57d6d5b8c",
    "hiv_treatment_adherence": "da4e1fd2-727f-4677-ab5f-44058555052c",
    "last_hiv_viral_load": "163545AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "previously_hospitalized": "163403AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "patient_tested_for_sti_in_current_visit": "161558AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "adherence_counselling_qualitative": "cbb9382b-3a47-44bf-9ecc-828966535524",
    "nutritional_assessment_status": "c481f80d-7553-41ab-94ca-efddb8ab294c",
    "nervous_system_examination_text": "163109AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "urogenital_examination_text": "163047AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "heent_examination_text": "163045AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "abdominal_examination_text": "160947AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "chest_examination_text": "160689AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "musculoskeletal_examination_text": "163048AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cardiac_examination_text": "163046AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "general_examination_text": "163042AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "weeks_of_current_gestation": "1438AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "patient_referred": "1648AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "was_a_lab_order_made": "2dbf01ab-77f6-4b35-aa2c-46dfc14b0af0",
    "tb_screening_questions_peads": "16ba5f4b-5430-44c8-91e4-c4c66b072f29",
    "tb_screening_questions_adults": "12a22a0b-f0ed-4f1a-8d70-7c6acda5ae78",
    "action_taken_presumptive_tb": "a39dfa34-a139-4335-9eac-219d6fedf868",
    "general_examination": "b78e20ec-0833-4e87-969b-760a29864be8",
    "nutrition_counseling": "1380AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "encounter_start_date": "163137AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "completed_course_of_tuberculosis_prophylaxis": "166463AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "list_of_chief_complaints": "f2ff2d1c-25e6-4143-9000-89633a932c2f",
    "hiv_related_opportunistic_infections": "6bdf2636-7da1-4691-afcc-5eede094138f",
    "opportunistic_infection_present": "c52ecf45-bd6c-43ed-861b-9a2714878729",
    "patient_reported_current_pcp_prophylaxis": "1109AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tb_treatment_comment": "163323AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_medication_refills_due": "162549AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tuberculosis_prophylaxis_plan": "1265AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "evaluated_for_tuberculosis_prophylaxis": "162275AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "current_menstruation_status": "082ddc79-e355-4344-a4f8-ee458c15e3ef",
    "treatment_of_precancerous_lesions_of_the_cervix": "3a8bb4b4-7496-415d-a327-57ae3711d4eb",
    "cervical_cancer_screening_method": "53ff5cd0-0f37-4190-87b1-9eb439a15e94",
    "arv_dispensed_in_days": "3a0709e9-d7a8-44b9-9512-111db5ce3989",
    "screening_test_result_tb": "160108AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "history_of_tuberculosis": "1389AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "current_tuberculosis_preventive_treatment": "90c9e554-b959-48e6-90d5-8d595a074c86",
    "neurologic_systems_review": "14d61422-5323-4706-9152-781ce59c90de",
    "musculoskeletal_systems_review": "c6665eb5-23a9-4add-9f39-d44e42a4e5b1",
    "genitourinary_systems_review": "bd337c08-384a-47b5-88c9-fb0a67e316bd",
    "gastrointestinal_system_review": "3fc7236b-2b72-4a52-8286-881ea2c8dbc7",
    "cardiovascular_system_review": "3909220e-0d0e-4547-a54e-fecd619cd861",
    "heent_systems_review": "33b614d0-1953-4056-ac84-66b0492394e5",
    "respiratory_systems_review": "f089c930-1c55-4ab6-934e-f7e7eca6f4e0",
    "tuberculosis_treatment_plan": "1268AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "sub_form_control": "c731a01e-7268-11ee-b962-0242ac120002",
    "name_subform_filled": "5ac76d80-726a-11ee-b962-0242ac120002",
    "sub_form_filled" : "9674e958-7269-11ee-b962-0242ac120002"

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
},{
  "report_name": "LaborandDelivery_Register",
  "flat_table_name": "mamba_flat_encounter_pmtct_labor_delivery",
  "encounter_type_uuid": "6dc5308d-27c9-4d49-b16f-2c5e3c759757" ,
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
          "anc_hiv_status_first_visit": "c5f74c86-62cd-4d22-9260-4238f1e45fe0"
  }
},{
  "report_name": "MotherPostnatal_Register",
  "flat_table_name": "mamba_flat_encounter_pmtct_mother_postnatal",
  "encounter_type_uuid": "a4362fd2d-1866-4ea0-84ef-5e5da9627440" ,
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

CALL sp_mamba_flat_table_config_insert_helper_manual(@report_data); -- insert manually added config JSON data from config dir
CALL sp_mamba_flat_table_config_insert_helper_auto(); -- insert automatically generated config JSON data from db

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_update;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the hash of the JSON data
UPDATE mamba_flat_table_config
SET table_json_data_hash = MD5(TRIM(table_json_data))
WHERE id > 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_flat_table_config_create();
CALL sp_mamba_flat_table_config_insert();
CALL sp_mamba_flat_table_config_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_create;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_incremental_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_incremental_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE IF NOT EXISTS mamba_flat_table_config_incremental
(
    id                   INT           NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    encounter_type_id    INT           NOT NULL UNIQUE,
    report_name          VARCHAR(100)  NOT NULL,
    table_json_data      JSON          NOT NULL,
    table_json_data_hash CHAR(32)      NULL,
    encounter_type_uuid  CHAR(38)      NOT NULL,
    incremental_record   INT DEFAULT 0 NOT NULL COMMENT 'Whether `table_json_data` has been modified or not',

    INDEX mamba_idx_encounter_type_id (encounter_type_id),
    INDEX mamba_idx_report_name (report_name),
    INDEX mamba_idx_table_json_data_hash (table_json_data_hash),
    INDEX mamba_idx_uuid (encounter_type_uuid),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_insert_helper_manual  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- manually extracts user given flat table config file json into the mamba_flat_table_config_incremental table
-- this data together with automatically extracted flat table data is inserted into the mamba_flat_table_config_incremental table
-- later it is processed by the 'fn_mamba_generate_report_array_from_automated_json_table' function
-- into the @report_data variable inside the compile-mysql.sh script

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_insert_helper_manual;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_insert_helper_manual(
    IN report_data MEDIUMTEXT CHARACTER SET UTF8MB4
)
BEGIN

    DECLARE report_count INT DEFAULT 0;
    DECLARE report_array_len INT;
    DECLARE report_enc_type_id INT DEFAULT NULL;
    DECLARE report_enc_type_uuid VARCHAR(50);
    DECLARE report_enc_name VARCHAR(500);

    SET session group_concat_max_len = 20000;

    SELECT JSON_EXTRACT(report_data, '$.flat_report_metadata') INTO @report_array;
    SELECT JSON_LENGTH(@report_array) INTO report_array_len;

    WHILE report_count < report_array_len
        DO

            SELECT JSON_EXTRACT(@report_array, CONCAT('$[', report_count, ']')) INTO @report_data_item;
            SELECT JSON_EXTRACT(@report_data_item, '$.report_name') INTO report_enc_name;
            SELECT JSON_EXTRACT(@report_data_item, '$.encounter_type_uuid') INTO report_enc_type_uuid;

            SET report_enc_type_uuid = JSON_UNQUOTE(report_enc_type_uuid);

            SET report_enc_type_id = (SELECT DISTINCT et.encounter_type_id
                                      FROM mamba_dim_encounter_type et
                                      WHERE et.uuid = report_enc_type_uuid
                                      LIMIT 1);

            IF report_enc_type_id IS NOT NULL THEN
                INSERT INTO mamba_flat_table_config_incremental
                (report_name,
                 encounter_type_id,
                 table_json_data,
                 encounter_type_uuid)
                VALUES (JSON_UNQUOTE(report_enc_name),
                        report_enc_type_id,
                        @report_data_item,
                        report_enc_type_uuid);
            END IF;

            SET report_count = report_count + 1;

        END WHILE;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_insert_helper_auto  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder
DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_insert_helper_auto;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_insert_helper_auto()
main_block:
BEGIN

    DECLARE encounter_type_name CHAR(50) CHARACTER SET UTF8MB4;
    DECLARE is_automatic_flattening TINYINT(1);

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_encounter_type_name CURSOR FOR
        SELECT DISTINCT et.name
        FROM openmrs.obs o
                 INNER JOIN openmrs.encounter e ON e.encounter_id = o.encounter_id
                 INNER JOIN mamba_dim_encounter_type et ON e.encounter_type = et.encounter_type_id
        WHERE et.encounter_type_id NOT IN (SELECT DISTINCT tc.encounter_type_id from mamba_flat_table_config_incremental tc)
          AND et.retired = 0;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SELECT DISTINCT(automatic_flattening_mode_switch)
    INTO is_automatic_flattening
    FROM _mamba_etl_user_settings;

    -- If auto-flattening is not switched on, do nothing
    IF is_automatic_flattening = 0 THEN
        LEAVE main_block;
    END IF;

    OPEN cursor_encounter_type_name;
    computations_loop:
    LOOP
        FETCH cursor_encounter_type_name INTO encounter_type_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        SET @insert_stmt = CONCAT(
                'INSERT INTO mamba_flat_table_config_incremental (report_name, encounter_type_id, table_json_data, encounter_type_uuid)
                    SELECT
                        name,
                        encounter_type_id,
                         CONCAT(''{'',
                            ''"report_name": "'', name, ''", '',
                            ''"flat_table_name": "'', table_name, ''", '',
                            ''"encounter_type_uuid": "'', uuid, ''", '',
                            ''"table_columns": '', json_obj, '' '',
                            ''}'') AS table_json_data,
                        encounter_type_uuid
                    FROM (
                        SELECT DISTINCT
                            et.name,
                            encounter_type_id,
                            et.auto_flat_table_name AS table_name,
                            et.uuid, ',
                '(
                SELECT DISTINCT CONCAT(''{'', GROUP_CONCAT(CONCAT(''"'', name, ''":"'', uuid, ''"'') SEPARATOR '','' ),''}'') x
                FROM (
                        SELECT
                            DISTINCT et.encounter_type_id,
                            c.auto_table_column_name AS name,
                            c.uuid
                        FROM openmrs.obs o
                        INNER JOIN openmrs.encounter e
                                  ON e.encounter_id = o.encounter_id
                        INNER JOIN mamba_dim_encounter_type et
                                  ON e.encounter_type = et.encounter_type_id
                        INNER JOIN mamba_dim_concept c
                                  ON o.concept_id = c.concept_id
                        WHERE et.name = ''', encounter_type_name, '''
                                    AND et.retired = 0
                                ) json_obj
                        ) json_obj,
                       et.uuid as encounter_type_uuid
                    FROM mamba_dim_encounter_type et
                    INNER JOIN openmrs.encounter e
                        ON e.encounter_type = et.encounter_type_id
                    WHERE et.name = ''', encounter_type_name, '''
                ) X  ;   ');
        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;
    END LOOP computations_loop;
    CLOSE cursor_encounter_type_name;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN


SET @report_data = '{"flat_report_metadata":[{
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
        "linked_to_art": "a40d8bc4-56b8-4f28-a1dd-412da5cf20ed"
      }
},{
  "report_name": "PMTCT ANC visit",
  "flat_table_name": "mamba_flat_encounter_pmtct_anc",
  "encounter_type_uuid": "677d1a80-dbbe-4399-be34-aa7f54f11405",
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
},{
  "report_name": "Clinical Encounter",
  "flat_table_name": "mamba_flat_encounter_clinical_visit",
  "encounter_type_uuid": "cb0a65a7-0587-477e-89b9-cf2fd144f1d4",
  "concepts_locale": "en",
  "table_columns": {
    "nutritional_support": "5484AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "referrals_ordered": "1272AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "name_of_health_care_provider": "1473AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "general_patient_note": "165095AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "sti_screening": "c4f81292-61a3-4561-a4ae-78be7d16d928",
    "reason_for_referral_text": "164359AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "sign_symptom_start_date": "1730AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "what_was_the_tb_screening_outcome": "c0661c0f-348b-4941-812b-c531a0a67f2e",
    "was_the_patient_screened_for_tb": "f8868467-bd15-4576-9da8-bfb8ef64ea17",
    "type_of_visit": "8a9809e9-8a0b-4e0e-b1f6-80b0cbbe361b",
    "currently_taking_tuberculosis_prophylaxis": "166449AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_last_normal_menstrual_period": "166079AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_tuberculosis_prophylaxis_ended": "163284AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_started_on_tuberculosis_prophylaxis": "162320AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tuberculosis_treatment_end_date": "159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "isoniazid_adherence": "161653AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "reason_not_on_family_planning": "160575AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "chief_complaint_text": "160531AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "currently_on_tuberculosis_treatment": "159798AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tuberculosis_drug_treatment_start_date": "1113AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "currently_breastfeeding_child": "5632AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "estimated_date_of_confinement": "5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "return_visit_date": "5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "currently_pregnant": "1434AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "scheduled_visit": "1246AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "new_complaints": "1154AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "method_of_family_planning": "374AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cervical_cancer_screening_status": "e5e99fc7-ff2d-4306-aefd-b87a07fc9ab4",
    "antenatal_profile_screening_done": "975f11e5-7471-4e57-bba7-d3ee358ef7ea",
    "intend_to_conceive_in_the_next_three_months": "9109b9f3-8176-4d2f-b47d-82630dcc02ce",
    "reason_for_poor_treatment_adherence": "160582AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_last_cervical_cancer_screening": "165429AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cotrimoxazole_prophylaxis_start_date": "164361AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_reported_hiv_viral_load": "163281AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "family_planning_status": "160653AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_of_reported_cd4_count": "159376AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cd4_count": "5497AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cryptococcal_treatment_plan": "1277AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "pcp_prophylaxis_plan": "1261AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "current_who_hiv_stage": "5356AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cd4_panel": "657AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "hiv_viral_load_test_qualitiative": "686dc1b2-68b5-4024-b311-bd2f5e3ce394",
    "fluconazole_start_date": "5ac4300a-5e19-45c8-8692-31a57d6d5b8c",
    "hiv_treatment_adherence": "da4e1fd2-727f-4677-ab5f-44058555052c",
    "last_hiv_viral_load": "163545AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "previously_hospitalized": "163403AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "patient_tested_for_sti_in_current_visit": "161558AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "adherence_counselling_qualitative": "cbb9382b-3a47-44bf-9ecc-828966535524",
    "nutritional_assessment_status": "c481f80d-7553-41ab-94ca-efddb8ab294c",
    "nervous_system_examination_text": "163109AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "urogenital_examination_text": "163047AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "heent_examination_text": "163045AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "abdominal_examination_text": "160947AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "chest_examination_text": "160689AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "musculoskeletal_examination_text": "163048AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "cardiac_examination_text": "163046AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "general_examination_text": "163042AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "weeks_of_current_gestation": "1438AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "patient_referred": "1648AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "was_a_lab_order_made": "2dbf01ab-77f6-4b35-aa2c-46dfc14b0af0",
    "tb_screening_questions_peads": "16ba5f4b-5430-44c8-91e4-c4c66b072f29",
    "tb_screening_questions_adults": "12a22a0b-f0ed-4f1a-8d70-7c6acda5ae78",
    "action_taken_presumptive_tb": "a39dfa34-a139-4335-9eac-219d6fedf868",
    "general_examination": "b78e20ec-0833-4e87-969b-760a29864be8",
    "nutrition_counseling": "1380AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "encounter_start_date": "163137AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "completed_course_of_tuberculosis_prophylaxis": "166463AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "list_of_chief_complaints": "f2ff2d1c-25e6-4143-9000-89633a932c2f",
    "hiv_related_opportunistic_infections": "6bdf2636-7da1-4691-afcc-5eede094138f",
    "opportunistic_infection_present": "c52ecf45-bd6c-43ed-861b-9a2714878729",
    "patient_reported_current_pcp_prophylaxis": "1109AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tb_treatment_comment": "163323AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "date_medication_refills_due": "162549AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "tuberculosis_prophylaxis_plan": "1265AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "evaluated_for_tuberculosis_prophylaxis": "162275AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "current_menstruation_status": "082ddc79-e355-4344-a4f8-ee458c15e3ef",
    "treatment_of_precancerous_lesions_of_the_cervix": "3a8bb4b4-7496-415d-a327-57ae3711d4eb",
    "cervical_cancer_screening_method": "53ff5cd0-0f37-4190-87b1-9eb439a15e94",
    "arv_dispensed_in_days": "3a0709e9-d7a8-44b9-9512-111db5ce3989",
    "screening_test_result_tb": "160108AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "history_of_tuberculosis": "1389AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "current_tuberculosis_preventive_treatment": "90c9e554-b959-48e6-90d5-8d595a074c86",
    "neurologic_systems_review": "14d61422-5323-4706-9152-781ce59c90de",
    "musculoskeletal_systems_review": "c6665eb5-23a9-4add-9f39-d44e42a4e5b1",
    "genitourinary_systems_review": "bd337c08-384a-47b5-88c9-fb0a67e316bd",
    "gastrointestinal_system_review": "3fc7236b-2b72-4a52-8286-881ea2c8dbc7",
    "cardiovascular_system_review": "3909220e-0d0e-4547-a54e-fecd619cd861",
    "heent_systems_review": "33b614d0-1953-4056-ac84-66b0492394e5",
    "respiratory_systems_review": "f089c930-1c55-4ab6-934e-f7e7eca6f4e0",
    "tuberculosis_treatment_plan": "1268AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "sub_form_control": "c731a01e-7268-11ee-b962-0242ac120002",
    "name_subform_filled": "5ac76d80-726a-11ee-b962-0242ac120002",
    "sub_form_filled" : "9674e958-7269-11ee-b962-0242ac120002"

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
},{
  "report_name": "LaborandDelivery_Register",
  "flat_table_name": "mamba_flat_encounter_pmtct_labor_delivery",
  "encounter_type_uuid": "6dc5308d-27c9-4d49-b16f-2c5e3c759757" ,
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
          "anc_hiv_status_first_visit": "c5f74c86-62cd-4d22-9260-4238f1e45fe0"
  }
},{
  "report_name": "MotherPostnatal_Register",
  "flat_table_name": "mamba_flat_encounter_pmtct_mother_postnatal",
  "encounter_type_uuid": "a4362fd2d-1866-4ea0-84ef-5e5da9627440" ,
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

CALL sp_mamba_flat_table_config_incremental_insert_helper_manual(@report_data); -- insert manually added config JSON data from config dir
CALL sp_mamba_flat_table_config_incremental_insert_helper_auto(); -- insert automatically generated config JSON data from db

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the hash of the JSON data
UPDATE mamba_flat_table_config_incremental
SET table_json_data_hash = MD5(TRIM(table_json_data))
WHERE id > 0;

-- If a new encounter type has been added
INSERT INTO mamba_flat_table_config (report_name,
                                     encounter_type_id,
                                     table_json_data,
                                     encounter_type_uuid,
                                     table_json_data_hash,
                                     incremental_record)
SELECT tci.report_name,
       tci.encounter_type_id,
       tci.table_json_data,
       tci.encounter_type_uuid,
       tci.table_json_data_hash,
       1
FROM mamba_flat_table_config_incremental tci
WHERE tci.encounter_type_id NOT IN (SELECT encounter_type_id FROM mamba_flat_table_config);

-- If there is any change in either concepts or encounter types in terms of names or additional questions
UPDATE mamba_flat_table_config tc
    INNER JOIN mamba_flat_table_config_incremental tci ON tc.encounter_type_id = tci.encounter_type_id
SET tc.table_json_data      = tci.table_json_data,
    tc.table_json_data_hash = tci.table_json_data_hash,
    tc.report_name          = tci.report_name,
    tc.encounter_type_uuid  = tci.encounter_type_uuid,
    tc.incremental_record   = 1
WHERE tc.table_json_data_hash <> tci.table_json_data_hash
  AND tc.table_json_data_hash IS NOT NULL;

-- If an encounter type has been voided then delete it from dim_json
DELETE
FROM mamba_flat_table_config
WHERE encounter_type_id NOT IN (SELECT tci.encounter_type_id FROM mamba_flat_table_config_incremental tci);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental_truncate;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_incremental_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_incremental_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_truncate_table('mamba_flat_table_config_incremental');
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_config_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_config_incremental;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_config_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_flat_table_config_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_flat_table_config_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_flat_table_config_incremental_create();
CALL sp_mamba_flat_table_config_incremental_truncate();
CALL sp_mamba_flat_table_config_incremental_insert();
CALL sp_mamba_flat_table_config_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_obs_group  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_obs_group;


~-~-
CREATE PROCEDURE sp_mamba_obs_group()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_obs_group', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_obs_group', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_obs_group_create();
CALL sp_mamba_obs_group_insert();
CALL sp_mamba_obs_group_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_obs_group_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_obs_group_create;


~-~-
CREATE PROCEDURE sp_mamba_obs_group_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_obs_group_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_obs_group_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_obs_group
(
    id                     INT          NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    obs_id                 INT          NOT NULL,
    obs_group_concept_id   INT          NOT NULL,
    obs_group_concept_name VARCHAR(255) NOT NULL, -- should be the concept name of the obs

    INDEX mamba_idx_obs_id (obs_id),
    INDEX mamba_idx_obs_group_concept_id (obs_group_concept_id),
    INDEX mamba_idx_obs_group_concept_name (obs_group_concept_name)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_obs_group_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_obs_group_insert;


~-~-
CREATE PROCEDURE sp_mamba_obs_group_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_obs_group_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_obs_group_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TEMPORARY TABLE mamba_temp_obs_group_ids
(
    obs_group_id INT NOT NULL,
    row_num      INT NOT NULL,

    INDEX mamba_idx_obs_group_id (obs_group_id),
    INDEX mamba_idx_visit_id (row_num)
)
    CHARSET = UTF8MB4;

INSERT INTO mamba_temp_obs_group_ids
SELECT obs_group_id,
       COUNT(*) AS row_num
FROM mamba_z_encounter_obs o
WHERE obs_group_id IS NOT NULL
GROUP BY obs_group_id, person_id, encounter_id;

INSERT INTO mamba_obs_group (obs_group_concept_id,
                             obs_group_concept_name,
                             obs_id)
SELECT DISTINCT o.obs_question_concept_id,
                LEFT(c.auto_table_column_name, 12) AS name,
                o.obs_id
FROM mamba_temp_obs_group_ids t
         INNER JOIN mamba_z_encounter_obs o
                    ON t.obs_group_id = o.obs_group_id
         INNER JOIN mamba_dim_concept c
                    ON o.obs_question_concept_id = c.concept_id
WHERE t.row_num > 1;

DROP TEMPORARY TABLE mamba_temp_obs_group_ids;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_obs_group_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_obs_group_update;


~-~-
CREATE PROCEDURE sp_mamba_obs_group_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_obs_group_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_obs_group_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_error_log_drop  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_error_log_drop;


~-~-
CREATE PROCEDURE sp_mamba_etl_error_log_drop()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_error_log_drop', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_error_log_drop', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

DROP TABLE IF EXISTS _mamba_etl_error_log;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_error_log_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_error_log_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_error_log_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_error_log_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_error_log_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE _mamba_etl_error_log
(
    id             INT          NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary Key',
    procedure_name VARCHAR(255) NOT NULL,
    error_message  VARCHAR(1000),
    error_code     INT,
    sql_state      VARCHAR(5),
    error_time     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_error_log_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_error_log_insert;


~-~-
CREATE PROCEDURE sp_mamba_etl_error_log_insert(
    IN procedure_name VARCHAR(255),
    IN error_message VARCHAR(1000),
    IN error_code INT,
    IN sql_state VARCHAR(5)
)
BEGIN
    INSERT INTO _mamba_etl_error_log (procedure_name, error_message, error_code, sql_state)
    VALUES (procedure_name, error_message, error_code, sql_state);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_error_log  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_error_log;


~-~-
CREATE PROCEDURE sp_mamba_etl_error_log()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_error_log', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_error_log', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_error_log_drop();
CALL sp_mamba_etl_error_log_create();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_user_settings_drop  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_user_settings_drop;


~-~-
CREATE PROCEDURE sp_mamba_etl_user_settings_drop()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_user_settings_drop', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_user_settings_drop', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

DROP TABLE IF EXISTS _mamba_etl_user_settings;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_user_settings_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_user_settings_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_user_settings_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_user_settings_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_user_settings_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE _mamba_etl_user_settings
(
    id                               INT          NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY COMMENT 'Primary Key',
    openmrs_database                 VARCHAR(255) NOT NULL DEFAULT 'openmrs',
    etl_database                     VARCHAR(255) NOT NULL DEFAULT 'analysis_db',
    concepts_locale                  VARCHAR(4)   NOT NULL,
    table_partition_number           INT          NOT NULL COMMENT 'Number of columns at which to partition \'many columned\' Tables',
    incremental_mode_switch          TINYINT(1)   NOT NULL COMMENT 'If MambaETL should/not run in Incremental Mode',
    automatic_flattening_mode_switch TINYINT(1)   NOT NULL COMMENT 'If MambaETL should/not automatically flatten ALL encounter types',
    etl_interval_seconds             INT          NOT NULL COMMENT 'ETL Runs every 60 seconds',
    last_etl_schedule_insert_id      INT          NOT NULL DEFAULT 1 COMMENT 'Insert ID of the last ETL that run'

) CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_user_settings_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_user_settings_insert;


~-~-
CREATE PROCEDURE sp_mamba_etl_user_settings_insert(
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4,
    IN table_partition_number INT,
    IN incremental_mode_switch TINYINT(1),
    IN automatic_flattening_mode_switch TINYINT(1),
    IN etl_interval_seconds INT
)
BEGIN

    SET @insert_stmt = CONCAT(
            'INSERT INTO _mamba_etl_user_settings (`concepts_locale`, `table_partition_number`, `incremental_mode_switch`, `automatic_flattening_mode_switch`, `etl_interval_seconds`) VALUES (''',
            concepts_locale, ''', ', table_partition_number, ', ', incremental_mode_switch, ', ',
            automatic_flattening_mode_switch, ', ', etl_interval_seconds, ');');

    PREPARE inserttbl FROM @insert_stmt;
    EXECUTE inserttbl;
    DEALLOCATE PREPARE inserttbl;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_user_settings  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_user_settings;


~-~-
CREATE PROCEDURE sp_mamba_etl_user_settings(
    IN concepts_locale CHAR(4) CHARACTER SET UTF8MB4,
    IN table_partition_number INT,
    IN incremental_mode_switch TINYINT(1),
    IN automatic_flattening_mode_switch TINYINT(1),
    IN etl_interval_seconds INT
)
BEGIN

    DECLARE locale CHAR(4) DEFAULT IFNULL(concepts_locale, 'en');
    DECLARE column_number INT DEFAULT IFNULL(table_partition_number, 50);
    DECLARE is_incremental TINYINT(1) DEFAULT IFNULL(incremental_mode_switch, 1);
    DECLARE is_automatic_flattening TINYINT(1) DEFAULT IFNULL(automatic_flattening_mode_switch, 1);
    DECLARE etl_interval INT DEFAULT IFNULL(etl_interval_seconds, 60);

    CALL sp_mamba_etl_user_settings_drop();
    CALL sp_mamba_etl_user_settings_create();
    CALL sp_mamba_etl_user_settings_insert(locale,
                                           column_number,
                                           is_incremental,
                                           is_automatic_flattening,
                                           etl_interval);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_all_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_all_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_all_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_all_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_all_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- This table will be used to index the columns that are used to determine if a record is new, changed, retired or voided
-- It will be used to speed up the incremental updates for each incremental Table indentified in the ETL process

CREATE TABLE IF NOT EXISTS mamba_etl_incremental_columns_index_all
(
    incremental_table_pkey INT        NOT NULL UNIQUE PRIMARY KEY,

    date_created           DATETIME   NOT NULL,
    date_changed           DATETIME   NULL,
    date_retired           DATETIME   NULL,
    date_voided            DATETIME   NULL,

    retired                TINYINT(1) NULL,
    voided                 TINYINT(1) NULL,

    INDEX mamba_idx_date_created (date_created),
    INDEX mamba_idx_date_changed (date_changed),
    INDEX mamba_idx_date_retired (date_retired),
    INDEX mamba_idx_date_voided (date_voided),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_voided (voided)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_all_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_all_insert;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_all_insert(
    IN openmrs_table VARCHAR(255)
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE incremental_column_name VARCHAR(255);
    DECLARE column_list VARCHAR(500) DEFAULT 'incremental_table_pkey, ';
    DECLARE select_list VARCHAR(500) DEFAULT '';
    DECLARE pkey_column VARCHAR(255);

    DECLARE column_cursor CURSOR FOR
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'mamba_etl_incremental_columns_index_all';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Identify the primary key of the target table
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'openmrs'
      AND TABLE_NAME = openmrs_table
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    -- Add the primary key to the select list
    SET select_list = CONCAT(select_list, pkey_column, ', ');

    OPEN column_cursor;

    column_loop:
    LOOP
        FETCH column_cursor INTO incremental_column_name;
        IF done THEN
            LEAVE column_loop;
        END IF;

        -- Check if the column exists in openmrs_table
        IF EXISTS (SELECT 1
                   FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_SCHEMA = 'openmrs'
                     AND TABLE_NAME = openmrs_table
                     AND COLUMN_NAME = incremental_column_name) THEN
            SET column_list = CONCAT(column_list, incremental_column_name, ', ');
            SET select_list = CONCAT(select_list, incremental_column_name, ', ');
        END IF;
    END LOOP column_loop;

    CLOSE column_cursor;

    -- Remove the trailing comma and space
    SET column_list = LEFT(column_list, LENGTH(column_list) - 2);
    SET select_list = LEFT(select_list, LENGTH(select_list) - 2);

    SET @insert_sql = CONCAT(
            'INSERT INTO mamba_etl_incremental_columns_index_all (', column_list, ') ',
            'SELECT ', select_list, ' FROM openmrs.', openmrs_table
                      );

    PREPARE stmt FROM @insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_all_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_all_truncate;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_all_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_all_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_all_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

TRUNCATE TABLE mamba_etl_incremental_columns_index_all;
-- CALL sp_mamba_truncate_table('mamba_etl_incremental_columns_index_all');

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_all;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_all(
    IN target_table_name VARCHAR(255)
)
BEGIN

    CALL sp_mamba_etl_incremental_columns_index_all_create();
    CALL sp_mamba_etl_incremental_columns_index_all_truncate();
    CALL sp_mamba_etl_incremental_columns_index_all_insert(target_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_new_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_new_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_new_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_new_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_new_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- This Table will only contain Primary keys for only those records that are NEW (i.e. Newly Inserted)

CREATE TEMPORARY TABLE IF NOT EXISTS mamba_etl_incremental_columns_index_new
(
    incremental_table_pkey INT NOT NULL UNIQUE PRIMARY KEY
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_new_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_new_insert;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_new_insert(
    IN mamba_table_name VARCHAR(255)
)
BEGIN
    DECLARE incremental_start_time DATETIME;
    DECLARE pkey_column VARCHAR(255);

    -- Identify the primary key of the 'mamba_table_name'
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = mamba_table_name
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    SET incremental_start_time = (SELECT start_time
                                  FROM _mamba_etl_schedule sch
                                  WHERE end_time IS NOT NULL
                                    AND transaction_status = 'COMPLETED'
                                  ORDER BY id DESC
                                  LIMIT 1);

    -- Insert only records that are NOT in the mamba ETL table
    -- and were created after the last ETL run time (start_time)
    SET @insert_sql = CONCAT(
            'INSERT INTO mamba_etl_incremental_columns_index_new (incremental_table_pkey) ',
            'SELECT DISTINCT incremental_table_pkey ',
            'FROM mamba_etl_incremental_columns_index_all ',
            'WHERE date_created >= ?',
            ' AND incremental_table_pkey NOT IN (SELECT DISTINCT (', pkey_column, ') FROM ', mamba_table_name, ')');

    PREPARE stmt FROM @insert_sql;
    SET @inc_start_time = incremental_start_time;
    EXECUTE stmt USING @inc_start_time;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_new_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_new_truncate;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_new_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_new_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_new_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

TRUNCATE TABLE mamba_etl_incremental_columns_index_new;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_new  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_new;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_new(
    IN mamba_table_name VARCHAR(255)
)
BEGIN

    CALL sp_mamba_etl_incremental_columns_index_new_create();
    CALL sp_mamba_etl_incremental_columns_index_new_truncate();
    CALL sp_mamba_etl_incremental_columns_index_new_insert(mamba_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_modified_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_modified_create;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_modified_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_modified_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_modified_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- This Table will only contain Primary keys for only those records that have been modified/updated (i.e. Retired, Voided, Changed)

CREATE TEMPORARY TABLE IF NOT EXISTS mamba_etl_incremental_columns_index_modified
(
    incremental_table_pkey INT NOT NULL UNIQUE PRIMARY KEY
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_modified_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_modified_insert;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_modified_insert(
    IN mamba_table_name VARCHAR(255)
)
BEGIN
    DECLARE incremental_start_time DATETIME;
    DECLARE pkey_column VARCHAR(255);

    -- Identify the primary key of the 'mamba_table_name'
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = mamba_table_name
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    SET incremental_start_time = (SELECT start_time
                                  FROM _mamba_etl_schedule sch
                                  WHERE end_time IS NOT NULL
                                    AND transaction_status = 'COMPLETED'
                                  ORDER BY id DESC
                                  LIMIT 1);

    -- Insert only records that are NOT in the mamba ETL table
    -- and were created after the last ETL run time (start_time)
    SET @insert_sql = CONCAT(
            'INSERT INTO mamba_etl_incremental_columns_index_modified (incremental_table_pkey) ',
            'SELECT DISTINCT incremental_table_pkey ',
            'FROM mamba_etl_incremental_columns_index_all ',
            'WHERE date_changed >= ?',
            ' OR (voided = 1 AND date_voided >= ?)',
            ' OR (retired = 1 AND date_retired >= ?)');

    PREPARE stmt FROM @insert_sql;
    SET @incremental_start_time = incremental_start_time;
    EXECUTE stmt USING @incremental_start_time, @incremental_start_time, @incremental_start_time;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_modified_truncate  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_modified_truncate;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_modified_truncate()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_etl_incremental_columns_index_modified_truncate', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_etl_incremental_columns_index_modified_truncate', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

TRUNCATE TABLE mamba_etl_incremental_columns_index_modified;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index_modified  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index_modified;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index_modified(
    IN mamba_table_name VARCHAR(255)
)
BEGIN

    CALL sp_mamba_etl_incremental_columns_index_modified_create();
    CALL sp_mamba_etl_incremental_columns_index_modified_truncate();
    CALL sp_mamba_etl_incremental_columns_index_modified_insert(mamba_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_incremental_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_incremental_create_all;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_incremental_create_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name)
        FROM mamba_concept_metadata md
        WHERE incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_mamba_drop_table(tbl_name);
        CALL sp_mamba_flat_encounter_table_create(tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_incremental_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_incremental_insert_all;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_incremental_insert_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name)
        FROM mamba_concept_metadata md
        WHERE incremental_record = 1;

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

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_obs_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_obs_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_obs_incremental_insert(
    IN encounter_id INT,
    IN flat_encounter_table_name VARCHAR(60) CHARACTER SET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;

    SET @enc_id = encounter_id;
    SET @tbl_name = flat_encounter_table_name;

    SELECT GROUP_CONCAT(DISTINCT
                        CONCAT(' MAX(CASE WHEN column_label = ''', column_label, ''' THEN ',
                               fn_mamba_get_obs_value_column(concept_datatype), ' END) ', column_label)
                        ORDER BY id ASC)
    INTO @column_labels
    FROM mamba_concept_metadata
    WHERE flat_table_name = @tbl_name;

    -- if enc_id exits in the table @tbl_name, then delete the record (to be replaced with the new one)
    SET @delete_stmt = CONCAT('DELETE FROM `', @tbl_name, '` WHERE encounter_id = ', @enc_id);
    PREPARE deletetb FROM @delete_stmt;
    EXECUTE deletetb;
    DEALLOCATE PREPARE deletetb;

    IF @column_labels IS NOT NULL THEN
        SET @insert_stmt = CONCAT(
                'INSERT INTO `', @tbl_name,
                '` SELECT o.encounter_id, MAX(o.visit_id) AS visit_id, o.person_id, o.encounter_datetime, MAX(o.location_id) AS location_id, ',
                @column_labels, '
                FROM mamba_z_encounter_obs o
                    INNER JOIN mamba_concept_metadata cm
                    ON IF(cm.concept_answer_obs=1, cm.concept_uuid=o.obs_value_coded_uuid, cm.concept_uuid=o.obs_question_uuid)
                WHERE cm.flat_table_name = ''', @tbl_name, '''
                AND o.encounter_id = ''', @enc_id, '''
                AND o.encounter_type_uuid = cm.encounter_type_uuid
                AND o.voided = 0
                GROUP BY o.encounter_id, o.person_id, o.encounter_datetime;');
    END IF;

    IF @column_labels IS NOT NULL THEN
        PREPARE inserttbl FROM @insert_stmt;
        EXECUTE inserttbl;
        DEALLOCATE PREPARE inserttbl;
    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_table_obs_incremental_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_flat_table_obs_incremental_insert_all;


~-~-
CREATE PROCEDURE sp_mamba_flat_table_obs_incremental_insert_all()
BEGIN

    DECLARE tbl_name VARCHAR(60) CHARACTER SET UTF8MB4;
    DECLARE encounter_id INT;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT eo.encounter_id, cm.flat_table_name
        FROM mamba_z_encounter_obs eo
                 INNER JOIN mamba_concept_metadata cm ON eo.encounter_type_uuid = cm.encounter_type_uuid
        WHERE eo.incremental_record = 1;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO encounter_id, tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_mamba_flat_table_obs_incremental_insert(encounter_id, tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_incremental_columns_index  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_etl_incremental_columns_index;


~-~-
CREATE PROCEDURE sp_mamba_etl_incremental_columns_index(
    IN openmrs_table_name VARCHAR(255),
    IN mamba_table_name VARCHAR(255)
)
BEGIN

    CALL sp_mamba_etl_incremental_columns_index_all(openmrs_table_name);
    CALL sp_mamba_etl_incremental_columns_index_new(mamba_table_name);
    CALL sp_mamba_etl_incremental_columns_index_modified(mamba_table_name);

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_table_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_table_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_table_insert(
    IN openmrs_table VARCHAR(255),
    IN mamba_table VARCHAR(255),
    IN is_incremental BOOLEAN
)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE tbl_column_name VARCHAR(255);
    DECLARE column_list VARCHAR(500) DEFAULT '';
    DECLARE select_list VARCHAR(500) DEFAULT '';
    DECLARE pkey_column VARCHAR(255);
    DECLARE join_clause VARCHAR(500) DEFAULT '';

    DECLARE column_cursor CURSOR FOR
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = mamba_table;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Identify the primary key of the openmrs table
    SELECT COLUMN_NAME
    INTO pkey_column
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'openmrs'
      AND TABLE_NAME = openmrs_table
      AND COLUMN_KEY = 'PRI'
    LIMIT 1;

    SET column_list = CONCAT(column_list, 'incremental_record', ', ');
    IF is_incremental THEN
        SET select_list = CONCAT(select_list, 1, ', ');
    ELSE
        SET select_list = CONCAT(select_list, 0, ', ');
    END IF;

    OPEN column_cursor;

    column_loop:
    LOOP
        FETCH column_cursor INTO tbl_column_name;
        IF done THEN
            LEAVE column_loop;
        END IF;

        -- Check if the column exists in openmrs_table
        IF EXISTS (SELECT 1
                   FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_SCHEMA = 'openmrs'
                     AND TABLE_NAME = openmrs_table
                     AND COLUMN_NAME = tbl_column_name) THEN
            SET column_list = CONCAT(column_list, tbl_column_name, ', ');
            SET select_list = CONCAT(select_list, tbl_column_name, ', ');
        END IF;
    END LOOP column_loop;

    CLOSE column_cursor;

    -- Remove the trailing comma and space
    SET column_list = LEFT(column_list, LENGTH(column_list) - 2);
    SET select_list = LEFT(select_list, LENGTH(select_list) - 2);

    -- Set the join clause if it is an incremental insert
    IF is_incremental THEN
        SET join_clause = CONCAT(
                ' INNER JOIN mamba_etl_incremental_columns_index_new ic',
                ' ON tb.', pkey_column, ' = ic.incremental_table_pkey');
    END IF;

    SET @insert_sql = CONCAT(
            'INSERT INTO ', mamba_table, ' (', column_list, ') ',
            'SELECT ', select_list,
            ' FROM openmrs.', openmrs_table, ' tb',
            join_clause, ';');

    PREPARE stmt FROM @insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_truncate_table  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_truncate_table;


~-~-
CREATE PROCEDURE sp_mamba_truncate_table(
    IN table_to_truncate VARCHAR(64) CHARACTER SET UTF8MB4
)
BEGIN
    IF EXISTS (SELECT 1
               FROM information_schema.tables
               WHERE table_schema = DATABASE()
                 AND table_name = table_to_truncate) THEN

        SET @sql = CONCAT('TRUNCATE TABLE ', table_to_truncate);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

    END IF;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_drop_table  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_drop_table;


~-~-
CREATE PROCEDURE sp_mamba_drop_table(
    IN table_to_drop VARCHAR(64) CHARACTER SET UTF8MB4
)
BEGIN

    SET @sql = CONCAT('DROP TABLE IF EXISTS ', table_to_drop);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_location
(
    location_id        INT           NOT NULL UNIQUE PRIMARY KEY,
    name               VARCHAR(255)  NOT NULL,
    description        VARCHAR(255)  NULL,
    city_village       VARCHAR(255)  NULL,
    state_province     VARCHAR(255)  NULL,
    postal_code        VARCHAR(50)   NULL,
    country            VARCHAR(50)   NULL,
    latitude           VARCHAR(50)   NULL,
    longitude          VARCHAR(50)   NULL,
    county_district    VARCHAR(255)  NULL,
    address1           VARCHAR(255)  NULL,
    address2           VARCHAR(255)  NULL,
    address3           VARCHAR(255)  NULL,
    address4           VARCHAR(255)  NULL,
    address5           VARCHAR(255)  NULL,
    address6           VARCHAR(255)  NULL,
    address7           VARCHAR(255)  NULL,
    address8           VARCHAR(255)  NULL,
    address9           VARCHAR(255)  NULL,
    address10          VARCHAR(255)  NULL,
    address11          VARCHAR(255)  NULL,
    address12          VARCHAR(255)  NULL,
    address13          VARCHAR(255)  NULL,
    address14          VARCHAR(255)  NULL,
    address15          VARCHAR(255)  NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_retired       DATETIME      NULL,
    retired            TINYINT(1)    NULL,
    retire_reason      VARCHAR(255)  NULL,
    retired_by         INT           NULL,
    changed_by         INT           NULL,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_name (name),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('location', 'mamba_dim_location', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location;


~-~-
CREATE PROCEDURE sp_mamba_dim_location()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_location_create();
CALL sp_mamba_dim_location_insert();
CALL sp_mamba_dim_location_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('location', 'mamba_dim_location');
CALL sp_mamba_dim_location_incremental_insert();
CALL sp_mamba_dim_location_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('location', 'mamba_dim_location', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_location_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_location_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_location_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_location_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_location mdl
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mdl.location_id = im.incremental_table_pkey
    INNER JOIN openmrs.location l
    ON mdl.location_id = l.location_id
SET mdl.name               = l.name,
    mdl.description        = l.description,
    mdl.city_village       = l.city_village,
    mdl.state_province     = l.state_province,
    mdl.postal_code        = l.postal_code,
    mdl.country            = l.country,
    mdl.latitude           = l.latitude,
    mdl.longitude          = l.longitude,
    mdl.county_district    = l.county_district,
    mdl.address1           = l.address1,
    mdl.address2           = l.address2,
    mdl.address3           = l.address3,
    mdl.address4           = l.address4,
    mdl.address5           = l.address5,
    mdl.address6           = l.address6,
    mdl.address7           = l.address7,
    mdl.address8           = l.address8,
    mdl.address9           = l.address9,
    mdl.address10          = l.address10,
    mdl.address11          = l.address11,
    mdl.address12          = l.address12,
    mdl.address13          = l.address13,
    mdl.address14          = l.address14,
    mdl.address15          = l.address15,
    mdl.date_created       = l.date_created,
    mdl.changed_by         = l.changed_by,
    mdl.date_changed       = l.date_changed,
    mdl.retired            = l.retired,
    mdl.retired_by         = l.retired_by,
    mdl.date_retired       = l.date_retired,
    mdl.retire_reason      = l.retire_reason,
    mdl.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_patient_identifier_type
(
    patient_identifier_type_id INT           NOT NULL UNIQUE PRIMARY KEY,
    name                       VARCHAR(50)   NOT NULL,
    description                TEXT          NULL,
    uuid                       CHAR(38)      NOT NULL,
    date_created               DATETIME      NOT NULL,
    date_changed               DATETIME      NULL,
    date_retired               DATETIME      NULL,
    retired                    TINYINT(1)    NULL,
    retire_reason              VARCHAR(255)  NULL,
    retired_by                 INT           NULL,
    changed_by                 INT           NULL,
    incremental_record         INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_name (name),
    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('patient_identifier_type', 'mamba_dim_patient_identifier_type', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_patient_identifier_type_create();
CALL sp_mamba_dim_patient_identifier_type_insert();
CALL sp_mamba_dim_patient_identifier_type_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('patient_identifier_type', 'mamba_dim_patient_identifier_type');
CALL sp_mamba_dim_patient_identifier_type_incremental_insert();
CALL sp_mamba_dim_patient_identifier_type_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('patient_identifier_type', 'mamba_dim_patient_identifier_type', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_type_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_type_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_patient_identifier_type mdpit
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mdpit.patient_identifier_type_id = im.incremental_table_pkey
    INNER JOIN openmrs.patient_identifier_type pit
    ON mdpit.patient_identifier_type_id = pit.patient_identifier_type_id
SET mdpit.name               = pit.name,
    mdpit.description        = pit.description,
    mdpit.uuid               = pit.uuid,
    mdpit.date_created       = pit.date_created,
    mdpit.date_changed       = pit.date_changed,
    mdpit.date_retired       = pit.date_retired,
    mdpit.retired            = pit.retired,
    mdpit.retire_reason      = pit.retire_reason,
    mdpit.retired_by         = pit.retired_by,
    mdpit.changed_by         = pit.changed_by,
    mdpit.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_concept_datatype
(
    concept_datatype_id INT           NOT NULL UNIQUE PRIMARY KEY,
    name                VARCHAR(255)  NOT NULL,
    hl7_abbreviation    VARCHAR(3)    NULL,
    description         VARCHAR(255)  NULL,
    date_created        DATETIME      NOT NULL,
    date_retired        DATETIME      NULL,
    retired             TINYINT(1)    NULL,
    retire_reason       VARCHAR(255)  NULL,
    retired_by          INT           NULL,
    incremental_record  INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_name (name),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('concept_datatype', 'mamba_dim_concept_datatype', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_concept_datatype_create();
CALL sp_mamba_dim_concept_datatype_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('concept_datatype', 'mamba_dim_concept_datatype', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_concept_datatype mcd
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mcd.concept_datatype_id = im.incremental_table_pkey
    INNER JOIN openmrs.concept_datatype cd
    ON mcd.concept_datatype_id = cd.concept_datatype_id
SET mcd.name               = cd.name,
    mcd.hl7_abbreviation   = cd.hl7_abbreviation,
    mcd.description        = cd.description,
    mcd.date_created       = cd.date_created,
    mcd.date_retired       = cd.date_retired,
    mcd.retired            = cd.retired,
    mcd.retired_by         = cd.retired_by,
    mcd.retire_reason      = cd.retire_reason,
    mcd.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_datatype_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_datatype_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_datatype_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('concept_datatype', 'mamba_dim_concept_datatype');
CALL sp_mamba_dim_concept_datatype_incremental_insert();
CALL sp_mamba_dim_concept_datatype_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_concept
(
    concept_id             INT           NOT NULL UNIQUE PRIMARY KEY,
    uuid                   CHAR(38)      NOT NULL,
    datatype_id            INT           NOT NULL, -- make it a FK
    datatype               VARCHAR(100)  NULL,
    name                   VARCHAR(256)  NULL,
    auto_table_column_name VARCHAR(60)   NULL,
    date_created           DATETIME      NOT NULL,
    date_changed           DATETIME      NULL,
    date_retired           DATETIME      NULL,
    retired                TINYINT(1)    NULL,
    retire_reason          VARCHAR(255)  NULL,
    retired_by             INT           NULL,
    changed_by             INT           NULL,
    incremental_record     INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_datatype_id (datatype_id),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_date_created (date_created),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('concept', 'mamba_dim_concept', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update the Data Type
UPDATE mamba_dim_concept c
    INNER JOIN mamba_dim_concept_datatype dt
    ON c.datatype_id = dt.concept_datatype_id
SET c.datatype = dt.name
WHERE c.concept_id > 0;

-- Update the concept name and table column name
UPDATE mamba_dim_concept c
    INNER JOIN mamba_dim_concept_name cn
    ON c.concept_id = cn.concept_id
SET c.name = IF(c.retired = 1, CONCAT(cn.name, '_', 'RETIRED'), cn.name),
    c.auto_table_column_name = LOWER(LEFT(REPLACE(REPLACE(fn_mamba_remove_special_characters(c.name), ' ', '_'),'__', '_'),60))
WHERE c.concept_id > 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_table_column_name VARCHAR(60);
    DECLARE previous_auto_table_column_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT concept_id, auto_table_column_name
        FROM mamba_dim_concept
        ORDER BY auto_table_column_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS mamba_dim_concept_temp
    (
        concept_id             INT,
        auto_table_column_name VARCHAR(60)
    );

    TRUNCATE TABLE mamba_dim_concept_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_table_column_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_table_column_name IS NULL THEN
            SET current_auto_table_column_name = '';
        END IF;

        IF current_auto_table_column_name = previous_auto_table_column_name THEN

            SET counter = counter + 1;
            SET current_auto_table_column_name = CONCAT(
                    IF(LENGTH(previous_auto_table_column_name) <= 57,
                       previous_auto_table_column_name,
                       LEFT(previous_auto_table_column_name, LENGTH(previous_auto_table_column_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_table_column_name = current_auto_table_column_name;
        END IF;

        INSERT INTO mamba_dim_concept_temp (concept_id, auto_table_column_name)
        VALUES (current_id, current_auto_table_column_name);

    END LOOP;

    CLOSE cur;

    UPDATE mamba_dim_concept c
        JOIN mamba_dim_concept_temp t
        ON c.concept_id = t.concept_id
    SET c.auto_table_column_name = t.auto_table_column_name
    WHERE c.concept_id > 0;

    DROP TEMPORARY TABLE mamba_dim_concept_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_concept_create();
CALL sp_mamba_dim_concept_insert();
CALL sp_mamba_dim_concept_update();
CALL sp_mamba_dim_concept_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('concept', 'mamba_dim_concept', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_concept tc
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON tc.concept_id = im.incremental_table_pkey
    INNER JOIN openmrs.concept sc
    ON tc.concept_id = sc.concept_id
SET tc.uuid               = sc.uuid,
    tc.datatype_id        = sc.datatype_id,
    tc.date_created       = sc.date_created,
    tc.date_changed       = sc.date_changed,
    tc.date_retired       = sc.date_retired,
    tc.changed_by         = sc.changed_by,
    tc.retired            = sc.retired,
    tc.retired_by         = sc.retired_by,
    tc.retire_reason      = sc.retire_reason,
    tc.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- Update the Data Type
UPDATE mamba_dim_concept c
    INNER JOIN mamba_dim_concept_datatype dt
    ON c.datatype_id = dt.concept_datatype_id
SET c.datatype = dt.name
WHERE c.incremental_record = 1;

-- Update the concept name and table column name
UPDATE mamba_dim_concept c
    INNER JOIN mamba_dim_concept_name cn
    ON c.concept_id = cn.concept_id
SET c.name = IF(c.retired = 1, CONCAT(cn.name, '_', 'RETIRED'), cn.name),
    c.auto_table_column_name = LOWER(LEFT(REPLACE(REPLACE(fn_mamba_remove_special_characters(c.name), ' ', '_'),'__', '_'),60))
WHERE c.incremental_record = 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_incremental_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_table_column_name VARCHAR(60);
    DECLARE previous_auto_table_column_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT concept_id, auto_table_column_name
        FROM mamba_dim_concept
        WHERE incremental_record = 1
        ORDER BY auto_table_column_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS mamba_dim_concept_temp
    (
        concept_id             INT,
        auto_table_column_name VARCHAR(60)
    );

    TRUNCATE TABLE mamba_dim_concept_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_table_column_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_table_column_name IS NULL THEN
            SET current_auto_table_column_name = '';
        END IF;

        IF current_auto_table_column_name = previous_auto_table_column_name THEN

            SET counter = counter + 1;
            SET current_auto_table_column_name = CONCAT(
                    IF(LENGTH(previous_auto_table_column_name) <= 57,
                       previous_auto_table_column_name,
                       LEFT(previous_auto_table_column_name, LENGTH(previous_auto_table_column_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_table_column_name = current_auto_table_column_name;
        END IF;

        INSERT INTO mamba_dim_concept_temp (concept_id, auto_table_column_name)
        VALUES (current_id, current_auto_table_column_name);

    END LOOP;

    CLOSE cur;

    UPDATE mamba_dim_concept c
        JOIN mamba_dim_concept_temp t
        ON c.concept_id = t.concept_id
    SET c.auto_table_column_name = t.auto_table_column_name
    WHERE incremental_record = 1;

    DROP TEMPORARY TABLE mamba_dim_concept_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('concept', 'mamba_dim_concept');
CALL sp_mamba_dim_concept_incremental_insert();
CALL sp_mamba_dim_concept_incremental_update();
CALL sp_mamba_dim_concept_incremental_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_concept_answer
(
    concept_answer_id  INT           NOT NULL UNIQUE PRIMARY KEY,
    concept_id         INT           NOT NULL,
    answer_concept     INT,
    answer_drug        INT,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_concept_answer (concept_answer_id),
    INDEX mamba_idx_concept_id (concept_id),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('concept_answer', 'mamba_dim_concept_answer', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_concept_answer_create();
CALL sp_mamba_dim_concept_answer_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new records
CALL sp_mamba_dim_table_insert('concept_answer', 'mamba_dim_concept_answer', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_answer_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_answer_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_answer_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('concept_answer', 'mamba_dim_concept_answer');
CALL sp_mamba_dim_concept_answer_incremental_insert();
CALL sp_mamba_dim_concept_answer_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_concept_name
(
    concept_name_id    INT           NOT NULL UNIQUE PRIMARY KEY,
    concept_id         INT,
    name               VARCHAR(255)  NOT NULL,
    locale             VARCHAR(50)   NOT NULL,
    locale_preferred   TINYINT,
    concept_name_type  VARCHAR(255),
    voided             TINYINT,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_voided        DATETIME      NULL,
    changed_by         INT           NULL,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_concept_id (concept_id),
    INDEX mamba_idx_concept_name_type (concept_name_type),
    INDEX mamba_idx_locale (locale),
    INDEX mamba_idx_locale_preferred (locale_preferred),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

INSERT INTO mamba_dim_concept_name (concept_name_id,
                                    concept_id,
                                    name,
                                    locale,
                                    locale_preferred,
                                    voided,
                                    concept_name_type,
                                    date_created,
                                    date_changed,
                                    changed_by,
                                    voided_by,
                                    date_voided,
                                    void_reason)
SELECT cn.concept_name_id,
       cn.concept_id,
       cn.name,
       cn.locale,
       cn.locale_preferred,
       cn.voided,
       cn.concept_name_type,
       cn.date_created,
       cn.date_changed,
       cn.changed_by,
       cn.voided_by,
       cn.date_voided,
       cn.void_reason
FROM openmrs.concept_name cn
WHERE cn.locale IN (SELECT DISTINCT(concepts_locale) FROM _mamba_etl_user_settings)
  AND IF(cn.locale_preferred = 1, cn.locale_preferred = 1, cn.concept_name_type = 'FULLY_SPECIFIED')
  AND cn.voided = 0;
-- Use locale preferred or Fully specified name

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_concept_name_create();
CALL sp_mamba_dim_concept_name_insert();
CALL sp_mamba_dim_concept_name_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
INSERT INTO mamba_dim_concept_name (concept_name_id,
                                    concept_id,
                                    name,
                                    locale,
                                    locale_preferred,
                                    voided,
                                    concept_name_type,
                                    date_created,
                                    date_changed,
                                    changed_by,
                                    voided_by,
                                    date_voided,
                                    void_reason,
                                    incremental_record)
SELECT cn.concept_name_id,
       cn.concept_id,
       cn.name,
       cn.locale,
       cn.locale_preferred,
       cn.voided,
       cn.concept_name_type,
       cn.date_created,
       cn.date_changed,
       cn.changed_by,
       cn.voided_by,
       cn.date_voided,
       cn.void_reason,
       1
FROM openmrs.concept_name cn
         INNER JOIN mamba_etl_incremental_columns_index_new ic
                    ON cn.concept_name_id = ic.incremental_table_pkey
WHERE cn.locale IN (SELECT DISTINCT (concepts_locale) FROM _mamba_etl_user_settings)
  AND cn.locale_preferred = 1
  AND cn.voided = 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- Update only Modified Records
UPDATE mamba_dim_concept_name cn
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON cn.concept_name_id = im.incremental_table_pkey
    INNER JOIN openmrs.concept_name cnm
    ON cn.concept_name_id = cnm.concept_name_id
SET cn.concept_id         = cnm.concept_id,
    cn.name               = cnm.name,
    cn.locale             = cnm.locale,
    cn.locale_preferred   = cnm.locale_preferred,
    cn.concept_name_type  = cnm.concept_name_type,
    cn.voided             = cnm.voided,
    cn.date_created       = cnm.date_created,
    cn.date_changed       = cnm.date_changed,
    cn.changed_by         = cnm.changed_by,
    cn.voided_by          = cnm.voided_by,
    cn.date_voided        = cnm.date_voided,
    cn.void_reason        = cnm.void_reason,
    cn.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_incremental_cleanup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_incremental_cleanup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_incremental_cleanup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- Delete any concept names that have become voided or not locale_preferred or not our locale we set (so we are consistent with the original INSERT statement)
-- We only need to keep the non-voided, locale we set & locale_preferred concept names
-- This is because when concept names are modified, the old name is voided and a new name is created but both have a date_changed value of the same date (donno why)

DELETE
FROM mamba_dim_concept_name
WHERE voided <> 0
   OR locale_preferred <> 1
   OR locale NOT IN (SELECT DISTINCT(concepts_locale) FROM _mamba_etl_user_settings);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_concept_name_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_concept_name_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_concept_name_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('concept_name', 'mamba_dim_concept_name');
CALL sp_mamba_dim_concept_name_incremental_insert();
CALL sp_mamba_dim_concept_name_incremental_update();
CALL sp_mamba_dim_concept_name_incremental_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_encounter_type
(
    encounter_type_id    INT           NOT NULL UNIQUE PRIMARY KEY,
    uuid                 CHAR(38)      NOT NULL,
    name                 VARCHAR(50)   NOT NULL,
    auto_flat_table_name VARCHAR(60)   NULL,
    description          TEXT          NULL,
    retired              TINYINT(1)    NULL,
    date_created         DATETIME      NULL,
    date_changed         DATETIME      NULL,
    changed_by           INT           NULL,
    date_retired         DATETIME      NULL,
    retired_by           INT           NULL,
    retire_reason        VARCHAR(255)  NULL,
    incremental_record   INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_name (name),
    INDEX mamba_idx_auto_flat_table_name (auto_flat_table_name),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('encounter_type', 'mamba_dim_encounter_type', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

UPDATE mamba_dim_encounter_type et
SET et.auto_flat_table_name = LOWER(LEFT(
        REPLACE(REPLACE(fn_mamba_remove_special_characters(CONCAT('mamba_flat_encounter_', et.name)), ' ', '_'), '__',
                '_'), 60))
WHERE et.encounter_type_id > 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_flat_table_name VARCHAR(60);
    DECLARE previous_auto_flat_table_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT encounter_type_id, auto_flat_table_name
        FROM mamba_dim_encounter_type
        ORDER BY auto_flat_table_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS mamba_dim_encounter_type_temp
    (
        encounter_type_id    INT,
        auto_flat_table_name VARCHAR(60)
    );

    TRUNCATE TABLE mamba_dim_encounter_type_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_flat_table_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_flat_table_name IS NULL THEN
            SET current_auto_flat_table_name = '';
        END IF;

        IF current_auto_flat_table_name = previous_auto_flat_table_name THEN

            SET counter = counter + 1;
            SET current_auto_flat_table_name = CONCAT(
                    IF(LENGTH(previous_auto_flat_table_name) <= 57,
                       previous_auto_flat_table_name,
                       LEFT(previous_auto_flat_table_name, LENGTH(previous_auto_flat_table_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_flat_table_name = current_auto_flat_table_name;
        END IF;

        INSERT INTO mamba_dim_encounter_type_temp (encounter_type_id, auto_flat_table_name)
        VALUES (current_id, current_auto_flat_table_name);

    END LOOP;

    CLOSE cur;

    UPDATE mamba_dim_encounter_type c
        JOIN mamba_dim_encounter_type_temp t
        ON c.encounter_type_id = t.encounter_type_id
    SET c.auto_flat_table_name = t.auto_flat_table_name
    WHERE c.encounter_type_id > 0;

    DROP TEMPORARY TABLE mamba_dim_encounter_type_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_encounter_type_create();
CALL sp_mamba_dim_encounter_type_insert();
CALL sp_mamba_dim_encounter_type_update();
CALL sp_mamba_dim_encounter_type_cleanup();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('encounter_type', 'mamba_dim_encounter_type', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounter types
UPDATE mamba_dim_encounter_type et
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON et.encounter_type_id = im.incremental_table_pkey
    INNER JOIN openmrs.encounter_type ent
    ON et.encounter_type_id = ent.encounter_type_id
SET et.uuid               = ent.uuid,
    et.name               = ent.name,
    et.description        = ent.description,
    et.retired            = ent.retired,
    et.date_created       = ent.date_created,
    et.date_changed       = ent.date_changed,
    et.changed_by         = ent.changed_by,
    et.date_retired       = ent.date_retired,
    et.retired_by         = ent.retired_by,
    et.retire_reason      = ent.retire_reason,
    et.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

UPDATE mamba_dim_encounter_type et
SET et.auto_flat_table_name = LOWER(LEFT(
        REPLACE(REPLACE(fn_mamba_remove_special_characters(CONCAT('mamba_flat_encounter_', et.name)), ' ', '_'), '__',
                '_'), 60))
WHERE et.incremental_record = 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_incremental_cleanup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_incremental_cleanup;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_incremental_cleanup()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE current_id INT;
    DECLARE current_auto_flat_table_name VARCHAR(60);
    DECLARE previous_auto_flat_table_name VARCHAR(60) DEFAULT '';
    DECLARE counter INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT encounter_type_id, auto_flat_table_name
        FROM mamba_dim_encounter_type
        WHERE incremental_record = 1
        ORDER BY auto_flat_table_name;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS mamba_dim_encounter_type_temp
    (
        encounter_type_id    INT,
        auto_flat_table_name VARCHAR(60)
    );

    TRUNCATE TABLE mamba_dim_encounter_type_temp;

    OPEN cur;

    read_loop:
    LOOP
        FETCH cur INTO current_id, current_auto_flat_table_name;

        IF done THEN
            LEAVE read_loop;
        END IF;

        IF current_auto_flat_table_name IS NULL THEN
            SET current_auto_flat_table_name = '';
        END IF;

        IF current_auto_flat_table_name = previous_auto_flat_table_name THEN

            SET counter = counter + 1;
            SET current_auto_flat_table_name = CONCAT(
                    IF(LENGTH(previous_auto_flat_table_name) <= 57,
                       previous_auto_flat_table_name,
                       LEFT(previous_auto_flat_table_name, LENGTH(previous_auto_flat_table_name) - 3)
                    ),
                    '_',
                    counter);
        ELSE
            SET counter = 0;
            SET previous_auto_flat_table_name = current_auto_flat_table_name;
        END IF;

        INSERT INTO mamba_dim_encounter_type_temp (encounter_type_id, auto_flat_table_name)
        VALUES (current_id, current_auto_flat_table_name);

    END LOOP;

    CLOSE cur;

    UPDATE mamba_dim_encounter_type et
        JOIN mamba_dim_encounter_type_temp t
        ON et.encounter_type_id = t.encounter_type_id
    SET et.auto_flat_table_name = t.auto_flat_table_name
    WHERE et.incremental_record = 1;

    DROP TEMPORARY TABLE mamba_dim_encounter_type_temp;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_type_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_type_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_type_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('encounter_type', 'mamba_dim_encounter_type');
CALL sp_mamba_dim_encounter_type_incremental_insert();
CALL sp_mamba_dim_encounter_type_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_encounter
(
    encounter_id        INT           NOT NULL UNIQUE PRIMARY KEY,
    uuid                CHAR(38)      NOT NULL,
    encounter_type      INT           NOT NULL,
    encounter_type_uuid CHAR(38)      NULL,
    patient_id          INT           NOT NULL,
    visit_id            INT           NULL,
    encounter_datetime  DATETIME      NOT NULL,
    date_created        DATETIME      NOT NULL,
    date_changed        DATETIME      NULL,
    changed_by          INT           NULL,
    date_voided         DATETIME      NULL,
    voided              TINYINT(1)    NOT NULL,
    voided_by           INT           NULL,
    void_reason         VARCHAR(255)  NULL,
    incremental_record  INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_encounter_id (encounter_id),
    INDEX mamba_idx_encounter_type (encounter_type),
    INDEX mamba_idx_encounter_type_uuid (encounter_type_uuid),
    INDEX mamba_idx_patient_id (patient_id),
    INDEX mamba_idx_visit_id (visit_id),
    INDEX mamba_idx_encounter_datetime (encounter_datetime),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

INSERT INTO mamba_dim_encounter (encounter_id,
                                 uuid,
                                 encounter_type,
                                 encounter_type_uuid,
                                 patient_id,
                                 visit_id,
                                 encounter_datetime,
                                 date_created,
                                 date_changed,
                                 changed_by,
                                 date_voided,
                                 voided,
                                 voided_by,
                                 void_reason)
SELECT e.encounter_id,
       e.uuid,
       e.encounter_type,
       et.uuid,
       e.patient_id,
       e.visit_id,
       e.encounter_datetime,
       e.date_created,
       e.date_changed,
       e.changed_by,
       e.date_voided,
       e.voided,
       e.voided_by,
       e.void_reason
FROM openmrs.encounter e
         INNER JOIN mamba_dim_encounter_type et
                    ON e.encounter_type = et.encounter_type_id
WHERE et.uuid
          IN (SELECT DISTINCT(md.encounter_type_uuid)
              FROM mamba_concept_metadata md);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_encounter_create();
CALL sp_mamba_dim_encounter_insert();
CALL sp_mamba_dim_encounter_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new records
INSERT INTO mamba_dim_encounter (encounter_id,
                                 uuid,
                                 encounter_type,
                                 encounter_type_uuid,
                                 patient_id,
                                 visit_id,
                                 encounter_datetime,
                                 date_created,
                                 date_changed,
                                 changed_by,
                                 date_voided,
                                 voided,
                                 voided_by,
                                 void_reason,
                                 incremental_record)
SELECT e.encounter_id,
       e.uuid,
       e.encounter_type,
       et.uuid,
       e.patient_id,
       e.visit_id,
       e.encounter_datetime,
       e.date_created,
       e.date_changed,
       e.changed_by,
       e.date_voided,
       e.voided,
       e.voided_by,
       e.void_reason,
       1
FROM openmrs.encounter e
         INNER JOIN mamba_etl_incremental_columns_index_new ic
                    ON e.encounter_id = ic.incremental_table_pkey
         INNER JOIN mamba_dim_encounter_type et
                    ON e.encounter_type = et.encounter_type_id;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounters
UPDATE mamba_dim_encounter e
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON e.encounter_id = im.incremental_table_pkey
    INNER JOIN openmrs.encounter enc
    ON e.encounter_id = enc.encounter_id
    INNER JOIN mamba_dim_encounter_type et
    ON e.encounter_type = et.encounter_type_id
SET e.encounter_id        = enc.encounter_id,
    e.uuid                = enc.uuid,
    e.encounter_type      = enc.encounter_type,
    e.encounter_type_uuid = et.uuid,
    e.patient_id          = enc.patient_id,
    e.visit_id            = enc.visit_id,
    e.encounter_datetime  = enc.encounter_datetime,
    e.date_created        = enc.date_created,
    e.date_changed        = enc.date_changed,
    e.changed_by          = enc.changed_by,
    e.date_voided         = enc.date_voided,
    e.voided              = enc.voided,
    e.voided_by           = enc.voided_by,
    e.void_reason         = enc.void_reason,
    e.incremental_record  = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_encounter_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_encounter_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_encounter_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('encounter', 'mamba_dim_encounter');
CALL sp_mamba_dim_encounter_incremental_insert();
CALL sp_mamba_dim_encounter_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_report_definition_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_report_definition_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_report_definition_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_report_definition_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_report_definition_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_report_definition_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_report_definition_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_report_definition_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
SET @report_definition_json = '{
  "report_definitions": [
    {
      "report_name": "MCH Mother HIV Status",
      "report_id": "mother_hiv_status",
      "report_sql": {
        "sql_query": "SELECT pm.encounter_id, pm.client_id, DATE_FORMAT(pm.encounter_datetime, ''%Y-%m-%d %H:%i:%s'') AS encounter_datetime, pm.visit_type, pm.hiv_test_result AS hiv_test_result, pm.hiv_test_performed, pm.partner_hiv_tested, pm.previously_known_positive, DATE_FORMAT(pm.return_visit_date, ''%Y-%m-%d'') AS return_visit_date FROM mamba_flat_encounter_pmtct_anc pm INNER JOIN mamba_dim_person p ON pm.client_id = p.person_id WHERE p.uuid = person_uuid AND pm.ptracker_id = ptracker_id",
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
      "report_name": "MCH Total Deliveries",
      "report_id": "total_deliveries",
      "report_sql": {
        "sql_query": "SELECT COUNT(*) AS total_deliveries FROM mamba_dim_encounter e inner join mamba_dim_encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''6dc5308d-27c9-4d49-b16f-2c5e3c759757'' AND DATE(e.encounter_datetime) > CONCAT(YEAR(CURDATE()), ''-01-01 00:00:00'')",
        "query_params": []
      }
    },
    {
      "report_name": "MCH HIV-Exposed Infants",
      "report_id": "total_hiv_exposed_infants",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT ei.infant_client_id) AS total_hiv_exposed_infants FROM mamba_fact_pmtct_exposedinfants ei INNER JOIN mamba_dim_person p ON ei.infant_client_id = p.person_id WHERE ei.encounter_datetime BETWEEN DATE_FORMAT(NOW(), ''%Y-01-01'') AND NOW() AND birthdate BETWEEN DATE_FORMAT(NOW(), ''%Y-01-01'') AND NOW()",
        "query_params": []
      }
    },
    {
      "report_name": "MCH Total Pregnant women",
      "report_id": "total_pregnant_women",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT pw.client_id) AS total_pregnant_women FROM mamba_fact_pmtct_pregnant_women pw WHERE visit_type like ''New%'' AND encounter_datetime BETWEEN DATE_FORMAT(NOW(), ''%Y-01-01'') AND NOW() AND DATE_ADD(date_of_last_menstrual_period, INTERVAL 40 WEEK) > NOW()",
        "query_params": []
      }
    },
    {
      "report_name": "TB Total Active DR Cases ",
      "report_id": "total_active_dr_cases",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT e.patient_id) AS total_active_dr FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid =''161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid =''159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid =''163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') AND o.value_coded = (SELECT concept_id FROM openmrs.concept WHERE uuid =''160052AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)",
        "query_params": []
      }
    },
    {
      "report_name": "TB Total Active DS Cases ",
      "report_id": "total_active_ds_cases",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT e.patient_id) AS total_active_ds FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid =''161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid =''159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = ''9a199b59-b185-485b-b9b3-a9754e65ae57'' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid =''163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') AND o.value_coded = (SELECT concept_id FROM openmrs.concept WHERE uuid =''160541AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)",
        "query_params": []
      }
    },
    {
      "report_name": "MNCH Mother Status",
      "report_id": "mother_status",
      "report_sql": {
        "sql_query": "SELECT cn.name as mother_status FROM openmrs.obs o INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.concept_name cn on o.value_coded = cn.concept_id WHERE p.uuid = person_uuid AND c.uuid = ''1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND o.encounter_id = (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''6dc5308d-27c9-4d49-b16f-2c5e3c759757'' AND c.uuid = ''1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) AND cn.voided = 0 AND cn.locale_preferred = 1",
        "query_params": [
          {
            "name": "person_uuid",
            "type": "VARCHAR(255)"
          }
        ]
      }
    },
    {
      "report_name": "MNCH Estimated Delivery Date",
      "report_id": "estimated_date_of_delivery",
      "report_sql": {
        "sql_query": "(SELECT CASE WHEN o.value_datetime >=curdate() THEN DATE_FORMAT(o.value_datetime, ''%d %m %Y'') ELSE '''' END AS estimated_date_of_delivery FROM openmrs.obs o INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.concept c on o.concept_id = c.concept_id  WHERE p.uuid = person_uuid AND c.uuid = ''5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND o.encounter_id = (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''677d1a80-dbbe-4399-be34-aa7f54f11405'' AND c.uuid = ''5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) ORDER BY o.value_datetime DESC  LIMIT 1)",
        "query_params": [
          {
            "name": "person_uuid",
            "type": "VARCHAR(255)"
          }
        ]
      }
    },
    {
      "report_name": "MNCH Next Appointment",
      "report_id": "next_appointment_date",
      "report_sql": {
        "sql_query": "(SELECT DATE_FORMAT(o.value_datetime, ''%d %m %Y'') as next_appointment FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''677d1a80-dbbe-4399-be34-aa7f54f11405'' AND c.uuid = ''5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND p.uuid = person_uuid) a ON o.encounter_id = a.encounter_id LEFT JOIN (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id  INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''4362fd2d-1866-4ea0-84ef-5e5da9627440'' AND c.uuid = ''5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND p.uuid = person_uuid)b  on o.encounter_id = b.encounter_id WHERE et.uuid = ''677d1a80-dbbe-4399-be34-aa7f54f11405'' AND c.uuid = ''5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND p.uuid = person_uuid ORDER BY  encounter_datetime DESC LIMIT 1)",
        "query_params": [
          {
            "name": "person_uuid",
            "type": "VARCHAR(255)"
          }
        ]
      }
    },
    {
      "report_name": "MNCH Number of ANC Visits",
      "report_id": "no_of_anc_visits",
      "report_sql": {
        "sql_query": "SELECT COUNT(DISTINCT e.encounter_id)no_of_anc_visits FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''677d1a80-dbbe-4399-be34-aa7f54f11405'' AND c.uuid = ''5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'' AND o.value_datetime >= curdate() AND p.uuid = person_uuid",
        "query_params": [
          {
            "name": "person_uuid",
            "type": "VARCHAR(255)"
          }
        ]
      }
    },
    {
      "report_name": "TPT Active clients",
      "report_id": "total_active_tpt",
      "report_sql": {
        "sql_query": "SELECT DISTINCT COUNT(DISTINCT e.patient_id) AS total_active_tpt FROM openmrs.obs o  INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id,max(value_datetime) date_enrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id  WHERE et.uuid = ''dc6ce80c-83f8-4ace-a638-21df78542551'' AND o.concept_id =  (SELECT concept_id FROM openmrs.concept WHERE uuid = ''162320AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') GROUP BY e.patient_id) ed ON ed.patient_id = e.patient_id LEFT JOIN(SELECT DISTINCT e.patient_id, max(value_datetime) date_disenrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = ''dc6ce80c-83f8-4ace-a638-21df78542551'' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid = ''163284AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'') GROUP BY e.patient_id) dd ON dd.patient_id = e.patient_id WHERE et.uuid = ''dc6ce80c-83f8-4ace-a638-21df78542551'' AND (dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)",
        "query_params": []
      }
    }
  ]
}';
CALL sp_mamba_extract_report_definition_metadata(@report_definition_json, 'mamba_dim_report_definition');
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_report_definition_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_report_definition_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_report_definition_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_report_definition_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_report_definition  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_report_definition;


~-~-
CREATE PROCEDURE sp_mamba_dim_report_definition()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_report_definition', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_report_definition', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_report_definition_create();
CALL sp_mamba_dim_report_definition_insert();
CALL sp_mamba_dim_report_definition_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_person
(
    person_id           INT           NOT NULL UNIQUE PRIMARY KEY,
    birthdate           DATE          NULL,
    birthdate_estimated TINYINT(1)    NOT NULL,
    age                 INT           NULL,
    dead                TINYINT(1)    NOT NULL,
    death_date          DATETIME      NULL,
    deathdate_estimated TINYINT       NOT NULL,
    gender              VARCHAR(50)   NULL,
    person_name_short   VARCHAR(255)  NULL,
    person_name_long    TEXT          NULL,
    uuid                CHAR(38)      NOT NULL,
    date_created        DATETIME      NOT NULL,
    date_changed        DATETIME      NULL,
    changed_by          INT           NULL,
    date_voided         DATETIME      NULL,
    voided              TINYINT(1)    NOT NULL,
    voided_by           INT           NULL,
    void_reason         VARCHAR(255)  NULL,
    incremental_record  INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_person_id (person_id),
    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_incremental_record (incremental_record)

) CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person', 'mamba_dim_person', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

UPDATE mamba_dim_person psn
    INNER JOIN mamba_dim_person_name pn
    on psn.person_id = pn.person_id
SET age               = fn_mamba_age_calculator(psn.birthdate, psn.death_date),
    person_name_short = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name),
    person_name_long  = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name_prefix, pn.family_name,
                                  pn.family_name2,
                                  pn.family_name_suffix, pn.degree)
WHERE pn.preferred = 1
  AND pn.voided = 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person;


~-~-
CREATE PROCEDURE sp_mamba_dim_person()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_person_create();
CALL sp_mamba_dim_person_insert();
CALL sp_mamba_dim_person_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person', 'mamba_dim_person', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Persons
UPDATE mamba_dim_person p
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON p.person_id = im.incremental_table_pkey
    INNER JOIN openmrs.person psn
    ON p.person_id = psn.person_id
SET p.birthdate           = psn.birthdate,
    p.birthdate_estimated = psn.birthdate_estimated,
    p.dead                = psn.dead,
    p.death_date          = psn.death_date,
    p.deathdate_estimated = psn.deathdate_estimated,
    p.gender              = psn.gender,
    p.uuid                = psn.uuid,
    p.date_created        = psn.date_created,
    p.date_changed        = psn.date_changed,
    p.changed_by          = psn.changed_by,
    p.date_voided         = psn.date_voided,
    p.voided              = psn.voided,
    p.voided_by           = psn.voided_by,
    p.void_reason         = psn.void_reason,
    p.incremental_record  = 1
WHERE im.incremental_table_pkey > 1;

UPDATE mamba_dim_person psn
    INNER JOIN mamba_dim_person_name pn
    on psn.person_id = pn.person_id
SET age               = fn_mamba_age_calculator(psn.birthdate, psn.death_date),
    person_name_short = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name),
    person_name_long  = CONCAT_WS(' ', pn.prefix, pn.given_name, pn.middle_name, pn.family_name_prefix, pn.family_name,
                                  pn.family_name2,
                                  pn.family_name_suffix, pn.degree)
WHERE psn.incremental_record = 1
  AND pn.preferred = 1
  AND pn.voided = 0;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('person', 'mamba_dim_person');
CALL sp_mamba_dim_person_incremental_insert();
CALL sp_mamba_dim_person_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_person_attribute
(
    person_attribute_id      INT           NOT NULL UNIQUE PRIMARY KEY,
    person_attribute_type_id INT           NOT NULL,
    person_id                INT           NOT NULL,
    uuid                     CHAR(38)      NOT NULL,
    value                    NVARCHAR(50)  NOT NULL,
    voided                   TINYINT,
    date_created             DATETIME      NOT NULL,
    date_changed             DATETIME      NULL,
    date_voided              DATETIME      NULL,
    changed_by               INT           NULL,
    voided_by                INT           NULL,
    void_reason              VARCHAR(255)  NULL,
    incremental_record       INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_person_attribute_type_id (person_attribute_type_id),
    INDEX mamba_idx_person_id (person_id),
    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_attribute', 'mamba_dim_person_attribute', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_person_attribute_create();
CALL sp_mamba_dim_person_attribute_insert();
CALL sp_mamba_dim_person_attribute_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_attribute', 'mamba_dim_person_attribute', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Persons
UPDATE mamba_dim_person_attribute mpa
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mpa.person_attribute_id = im.incremental_table_pkey
    INNER JOIN openmrs.person_attribute pa
    ON mpa.person_attribute_id = pa.person_attribute_id
SET mpa.person_attribute_id = pa.person_attribute_id,
    mpa.person_id           = pa.person_id,
    mpa.uuid                = pa.uuid,
    mpa.value               = pa.value,
    mpa.date_created        = pa.date_created,
    mpa.date_changed        = pa.date_changed,
    mpa.date_voided         = pa.date_voided,
    mpa.changed_by          = pa.changed_by,
    mpa.voided              = pa.voided,
    mpa.voided_by           = pa.voided_by,
    mpa.void_reason         = pa.void_reason,
    mpa.incremental_record  = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('person_attribute', 'mamba_dim_person_attribute');
CALL sp_mamba_dim_person_attribute_incremental_insert();
CALL sp_mamba_dim_person_attribute_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_person_attribute_type
(
    person_attribute_type_id INT           NOT NULL UNIQUE PRIMARY KEY,
    name                     NVARCHAR(50)  NOT NULL,
    description              TEXT          NULL,
    searchable               TINYINT(1)    NOT NULL,
    uuid                     NVARCHAR(50)  NOT NULL,
    date_created             DATETIME      NOT NULL,
    date_changed             DATETIME      NULL,
    date_retired             DATETIME      NULL,
    retired                  TINYINT(1)    NULL,
    retire_reason            VARCHAR(255)  NULL,
    retired_by               INT           NULL,
    changed_by               INT           NULL,
    incremental_record       INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_name (name),
    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_attribute_type', 'mamba_dim_person_attribute_type', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_person_attribute_type_create();
CALL sp_mamba_dim_person_attribute_type_insert();
CALL sp_mamba_dim_person_attribute_type_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('person_attribute_type', 'mamba_dim_person_attribute_type', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_person_attribute_type mpat
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mpat.person_attribute_type_id = im.incremental_table_pkey
    INNER JOIN openmrs.person_attribute_type pat
    ON mpat.person_attribute_type_id = pat.person_attribute_type_id
SET mpat.name               = pat.name,
    mpat.description        = pat.description,
    mpat.searchable         = pat.searchable,
    mpat.uuid               = pat.uuid,
    mpat.date_created       = pat.date_created,
    mpat.date_changed       = pat.date_changed,
    mpat.date_retired       = pat.date_retired,
    mpat.changed_by         = pat.changed_by,
    mpat.retired            = pat.retired,
    mpat.retired_by         = pat.retired_by,
    mpat.retire_reason      = pat.retire_reason,
    mpat.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_attribute_type_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_attribute_type_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_attribute_type_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_attribute_type_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_attribute_type_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('person_attribute_type', 'mamba_dim_person_attribute_type');
CALL sp_mamba_dim_person_attribute_type_incremental_insert();
CALL sp_mamba_dim_person_attribute_type_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_patient_identifier
(
    patient_identifier_id INT           NOT NULL UNIQUE PRIMARY KEY,
    patient_id            INT           NOT NULL,
    identifier            VARCHAR(50)   NOT NULL,
    identifier_type       INT           NOT NULL,
    preferred             TINYINT       NOT NULL,
    location_id           INT           NULL,
    patient_program_id    INT           NULL,
    uuid                  CHAR(38)      NOT NULL,
    date_created          DATETIME      NOT NULL,
    date_changed          DATETIME      NULL,
    date_voided           DATETIME      NULL,
    changed_by            INT           NULL,
    voided                TINYINT,
    voided_by             INT           NULL,
    void_reason           VARCHAR(255)  NULL,
    incremental_record    INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_patient_id (patient_id),
    INDEX mamba_idx_identifier (identifier),
    INDEX mamba_idx_identifier_type (identifier_type),
    INDEX mamba_idx_preferred (preferred),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('patient_identifier', 'mamba_dim_patient_identifier', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_patient_identifier_create();
CALL sp_mamba_dim_patient_identifier_insert();
CALL sp_mamba_dim_patient_identifier_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('patient_identifier', 'mamba_dim_patient_identifier', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_patient_identifier mpi
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mpi.patient_id = im.incremental_table_pkey
    INNER JOIN openmrs.patient_identifier pi
    ON mpi.patient_id = pi.patient_id
SET mpi.patient_id         = pi.patient_id,
    mpi.identifier         = pi.identifier,
    mpi.identifier_type    = pi.identifier_type,
    mpi.preferred          = pi.preferred,
    mpi.location_id        = pi.location_id,
    mpi.patient_program_id = pi.patient_program_id,
    mpi.uuid               = pi.uuid,
    mpi.voided             = pi.voided,
    mpi.date_created       = pi.date_created,
    mpi.date_changed       = pi.date_changed,
    mpi.date_voided        = pi.date_voided,
    mpi.changed_by         = pi.changed_by,
    mpi.voided_by          = pi.voided_by,
    mpi.void_reason        = pi.void_reason,
    mpi.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_patient_identifier_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_patient_identifier_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_patient_identifier_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('patient_identifier', 'mamba_dim_patient_identifier');
CALL sp_mamba_dim_patient_identifier_incremental_insert();
CALL sp_mamba_dim_patient_identifier_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_person_name
(
    person_name_id     INT           NOT NULL UNIQUE PRIMARY KEY,
    person_id          INT           NOT NULL,
    preferred          TINYINT       NOT NULL,
    prefix             VARCHAR(50)   NULL,
    given_name         VARCHAR(50)   NULL,
    middle_name        VARCHAR(50)   NULL,
    family_name_prefix VARCHAR(50)   NULL,
    family_name        VARCHAR(50)   NULL,
    family_name2       VARCHAR(50)   NULL,
    family_name_suffix VARCHAR(50)   NULL,
    degree             VARCHAR(50)   NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_voided        DATETIME      NULL,
    changed_by         INT           NULL,
    voided             TINYINT(1)    NOT NULL,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_person_id (person_id),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_preferred (preferred),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_name', 'mamba_dim_person_name', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_person_name_create();
CALL sp_mamba_dim_person_name_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_name', 'mamba_dim_person_name', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounters
UPDATE mamba_dim_person_name dpn
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON dpn.person_name_id = im.incremental_table_pkey
    INNER JOIN openmrs.person_name pn
    ON dpn.person_name_id = pn.person_name_id
SET dpn.person_name_id     = pn.person_name_id,
    dpn.person_id          = pn.person_id,
    dpn.preferred          = pn.preferred,
    dpn.prefix             = pn.prefix,
    dpn.given_name         = pn.given_name,
    dpn.middle_name        = pn.middle_name,
    dpn.family_name_prefix = pn.family_name_prefix,
    dpn.family_name        = pn.family_name,
    dpn.family_name2       = pn.family_name2,
    dpn.family_name_suffix = pn.family_name_suffix,
    dpn.degree             = pn.degree,
    dpn.date_created       = pn.date_created,
    dpn.date_changed       = pn.date_changed,
    dpn.changed_by         = pn.changed_by,
    dpn.date_voided        = pn.date_voided,
    dpn.voided             = pn.voided,
    dpn.voided_by          = pn.voided_by,
    dpn.void_reason        = pn.void_reason,
    dpn.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_name_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_name_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_name_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('person_name', 'mamba_dim_person_name');
CALL sp_mamba_dim_person_name_incremental_insert();
CALL sp_mamba_dim_person_name_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_person_address
(
    person_address_id  INT           NOT NULL UNIQUE PRIMARY KEY,
    person_id          INT           NULL,
    preferred          TINYINT       NOT NULL,
    address1           VARCHAR(255)  NULL,
    address2           VARCHAR(255)  NULL,
    address3           VARCHAR(255)  NULL,
    address4           VARCHAR(255)  NULL,
    address5           VARCHAR(255)  NULL,
    address6           VARCHAR(255)  NULL,
    address7           VARCHAR(255)  NULL,
    address8           VARCHAR(255)  NULL,
    address9           VARCHAR(255)  NULL,
    address10          VARCHAR(255)  NULL,
    address11          VARCHAR(255)  NULL,
    address12          VARCHAR(255)  NULL,
    address13          VARCHAR(255)  NULL,
    address14          VARCHAR(255)  NULL,
    address15          VARCHAR(255)  NULL,
    city_village       VARCHAR(255)  NULL,
    county_district    VARCHAR(255)  NULL,
    state_province     VARCHAR(255)  NULL,
    postal_code        VARCHAR(50)   NULL,
    country            VARCHAR(50)   NULL,
    latitude           VARCHAR(50)   NULL,
    longitude          VARCHAR(50)   NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    date_voided        DATETIME      NULL,
    changed_by         INT           NULL,
    voided             TINYINT,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_person_id (person_id),
    INDEX mamba_idx_preferred (preferred),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('person_address', 'mamba_dim_person_address', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_person_address_create();
CALL sp_mamba_dim_person_address_insert();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('person_address', 'mamba_dim_person_address', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records
UPDATE mamba_dim_person_address mpa
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON mpa.person_address_id = im.incremental_table_pkey
    INNER JOIN openmrs.person_address pa
    ON mpa.person_address_id = pa.person_address_id
SET mpa.person_id          = pa.person_id,
    mpa.preferred          = pa.preferred,
    mpa.address1           = pa.address1,
    mpa.address2           = pa.address2,
    mpa.address3           = pa.address3,
    mpa.address4           = pa.address4,
    mpa.address5           = pa.address5,
    mpa.address6           = pa.address6,
    mpa.address7           = pa.address7,
    mpa.address8           = pa.address8,
    mpa.address9           = pa.address9,
    mpa.address10          = pa.address10,
    mpa.address11          = pa.address11,
    mpa.address12          = pa.address12,
    mpa.address13          = pa.address13,
    mpa.address14          = pa.address14,
    mpa.address15          = pa.address15,
    mpa.city_village       = pa.city_village,
    mpa.county_district    = pa.county_district,
    mpa.state_province     = pa.state_province,
    mpa.postal_code        = pa.postal_code,
    mpa.country            = pa.country,
    mpa.latitude           = pa.latitude,
    mpa.longitude          = pa.longitude,
    mpa.date_created       = pa.date_created,
    mpa.date_changed       = pa.date_changed,
    mpa.date_voided        = pa.date_voided,
    mpa.changed_by         = pa.changed_by,
    mpa.voided             = pa.voided,
    mpa.voided_by          = pa.voided_by,
    mpa.void_reason        = pa.void_reason,
    mpa.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_person_address_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_person_address_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_person_address_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('person_address', 'mamba_dim_person_address');
CALL sp_mamba_dim_person_address_incremental_insert();
CALL sp_mamba_dim_person_address_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CREATE TABLE mamba_dim_users
(
    user_id            INT           NOT NULL UNIQUE PRIMARY KEY,
    system_id          VARCHAR(50)   NOT NULL,
    username           VARCHAR(50)   NULL,
    creator            INT           NOT NULL,
    person_id          INT           NOT NULL,
    uuid               CHAR(38)      NOT NULL,
    email              VARCHAR(255)  NULL,
    retired            TINYINT(1)    NULL,
    date_created       DATETIME      NULL,
    date_changed       DATETIME      NULL,
    changed_by         INT           NULL,
    date_retired       DATETIME      NULL,
    retired_by         INT           NULL,
    retire_reason      VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_system_id (system_id),
    INDEX mamba_idx_username (username),
    INDEX mamba_idx_retired (retired),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('users', 'mamba_dim_users', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user;


~-~-
CREATE PROCEDURE sp_mamba_dim_user()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
    CALL sp_mamba_dim_user_create();
    CALL sp_mamba_dim_user_insert();
    CALL sp_mamba_dim_user_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert only new Records
CALL sp_mamba_dim_table_insert('users', 'mamba_dim_users', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Users
UPDATE mamba_dim_users u
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON u.user_id = im.incremental_table_pkey
    INNER JOIN openmrs.users us
    ON u.user_id = us.user_id
SET u.system_id          = us.system_id,
    u.username           = us.username,
    u.creator            = us.creator,
    u.person_id          = us.person_id,
    u.uuid               = us.uuid,
    u.email              = us.email,
    u.retired            = us.retired,
    u.date_created       = us.date_created,
    u.date_changed       = us.date_changed,
    u.changed_by         = us.changed_by,
    u.date_retired       = us.date_retired,
    u.retired_by         = us.retired_by,
    u.retire_reason      = us.retire_reason,
    u.incremental_record = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_user_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_user_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_user_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_user_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_etl_incremental_columns_index('users', 'mamba_dim_users');
CALL sp_mamba_dim_user_incremental_insert();
CALL sp_mamba_dim_user_incremental_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_relationship
(
    relationship_id    INT           NOT NULL UNIQUE PRIMARY KEY,
    person_a           INT           NOT NULL,
    relationship       INT           NOT NULL,
    person_b           INT           NOT NULL,
    start_date         DATETIME      NULL,
    end_date           DATETIME      NULL,
    creator            INT           NOT NULL,
    uuid               CHAR(38)      NOT NULL,
    date_created       DATETIME      NOT NULL,
    date_changed       DATETIME      NULL,
    changed_by         INT           NULL,
    date_voided        DATETIME      NULL,
    voided             TINYINT(1)    NOT NULL,
    voided_by          INT           NULL,
    void_reason        VARCHAR(255)  NULL,
    incremental_record INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_person_a (person_a),
    INDEX mamba_idx_person_b (person_b),
    INDEX mamba_idx_relationship (relationship),
    INDEX mamba_idx_incremental_record (incremental_record)

) CHARSET = UTF8MB3;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('relationship', 'mamba_dim_relationship', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_relationship_create();
CALL sp_mamba_dim_relationship_insert();
CALL sp_mamba_dim_relationship_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('relationship', 'mamba_dim_relationship', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only modified records
UPDATE mamba_dim_relationship r
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON r.relationship_id = im.incremental_table_pkey
    INNER JOIN openmrs.relationship rel
    ON r.relationship_id = rel.relationship_id
SET r.relationship       = rel.relationship,
    r.person_a           = rel.person_a,
    r.relationship       = rel.relationship,
    r.person_b           = rel.person_b,
    r.start_date         = rel.start_date,
    r.end_date           = rel.end_date,
    r.creator            = rel.creator,
    r.uuid               = rel.uuid,
    r.date_created       = rel.date_created,
    r.date_changed       = rel.date_changed,
    r.changed_by         = rel.changed_by,
    r.voided             = rel.voided,
    r.voided_by          = rel.voided_by,
    r.date_voided        = rel.date_voided,
    r.incremental_record = 1
WHERE im.incremental_table_pkey > 1;


-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_relationship_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_relationship_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_relationship_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_relationship_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_relationship_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('relationship', 'mamba_dim_relationship');
CALL sp_mamba_dim_relationship_incremental_insert();
CALL sp_mamba_dim_relationship_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_dim_orders
(
    order_id               INT           NOT NULL UNIQUE PRIMARY KEY,
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
    date_created           DATETIME      NOT NULL,
    creator                INT           NULL,
    voided                 TINYINT(1)    NOT NULL,
    voided_by              INT           NULL,
    date_voided            DATETIME      NULL,
    void_reason            VARCHAR(255)  NULL,
    incremental_record     INT DEFAULT 0 NOT NULL,

    INDEX mamba_idx_uuid (uuid),
    INDEX mamba_idx_order_type_id (order_type_id),
    INDEX mamba_idx_concept_id (concept_id),
    INDEX mamba_idx_patient_id (patient_id),
    INDEX mamba_idx_encounter_id (encounter_id),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('orders', 'mamba_dim_orders', FALSE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_orders_create();
CALL sp_mamba_dim_orders_insert();
CALL sp_mamba_dim_orders_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_table_insert('orders', 'mamba_dim_orders', TRUE);

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Modified Encounters
UPDATE mamba_dim_orders do
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON do.order_id = im.incremental_table_pkey
    INNER JOIN openmrs.orders o
    ON do.order_id = o.order_id
SET do.order_id               = o.order_id,
    do.uuid                   = o.uuid,
    do.order_type_id          = o.order_type_id,
    do.concept_id             = o.concept_id,
    do.patient_id             = o.patient_id,
    do.encounter_id           = o.encounter_id,
    do.accession_number       = o.accession_number,
    do.order_number           = o.order_number,
    do.orderer                = o.orderer,
    do.instructions           = o.instructions,
    do.date_activated         = o.date_activated,
    do.auto_expire_date       = o.auto_expire_date,
    do.date_stopped           = o.date_stopped,
    do.order_reason           = o.order_reason,
    do.order_reason_non_coded = o.order_reason_non_coded,
    do.urgency                = o.urgency,
    do.previous_order_id      = o.previous_order_id,
    do.order_action           = o.order_action,
    do.comment_to_fulfiller   = o.comment_to_fulfiller,
    do.care_setting           = o.care_setting,
    do.scheduled_date         = o.scheduled_date,
    do.order_group_id         = o.order_group_id,
    do.sort_weight            = o.sort_weight,
    do.fulfiller_comment      = o.fulfiller_comment,
    do.fulfiller_status       = o.fulfiller_status,
    do.date_created           = o.date_created,
    do.creator                = o.creator,
    do.voided                 = o.voided,
    do.voided_by              = o.voided_by,
    do.date_voided            = o.date_voided,
    do.void_reason            = o.void_reason,
    do.incremental_record     = 1
WHERE im.incremental_table_pkey > 1;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_orders_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_orders_incremental;


~-~-
CREATE PROCEDURE sp_mamba_dim_orders_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_orders_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_orders_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('orders', 'mamba_dim_orders');
CALL sp_mamba_dim_orders_incremental_insert();
CALL sp_mamba_dim_orders_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_agegroup_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_agegroup_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_agegroup_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_agegroup_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_agegroup_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_agegroup_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_load_agegroup();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_agegroup_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_agegroup_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_agegroup_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup;


~-~-
CREATE PROCEDURE sp_mamba_dim_agegroup()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_agegroup', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_agegroup', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_dim_agegroup_create();
CALL sp_mamba_dim_agegroup_insert();
CALL sp_mamba_dim_agegroup_update();
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_create;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_z_encounter_obs_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_z_encounter_obs_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CREATE TABLE mamba_z_encounter_obs
(
    obs_id                  INT           NOT NULL UNIQUE PRIMARY KEY,
    encounter_id            INT           NULL,
    visit_id                INT           NULL,
    person_id               INT           NOT NULL,
    order_id                INT           NULL,
    encounter_datetime      DATETIME      NOT NULL,
    obs_datetime            DATETIME      NOT NULL,
    location_id             INT           NULL,
    obs_group_id            INT           NULL,
    obs_question_concept_id INT DEFAULT 0 NOT NULL,
    obs_value_text          TEXT          NULL,
    obs_value_numeric       DOUBLE        NULL,
    obs_value_boolean       BOOLEAN       NULL,
    obs_value_coded         INT           NULL,
    obs_value_datetime      DATETIME      NULL,
    obs_value_complex       VARCHAR(1000) NULL,
    obs_value_drug          INT           NULL,
    obs_question_uuid       CHAR(38),
    obs_answer_uuid         CHAR(38),
    obs_value_coded_uuid    CHAR(38),
    encounter_type_uuid     CHAR(38),
    status                  VARCHAR(16)   NOT NULL,
    previous_version        INT           NULL,
    date_created            DATETIME      NOT NULL,
    date_voided             DATETIME      NULL,
    voided                  TINYINT(1)    NOT NULL,
    voided_by               INT           NULL,
    void_reason             VARCHAR(255)  NULL,
    incremental_record      INT DEFAULT 0 NOT NULL, -- whether a record has been inserted after the first ETL run

    INDEX mamba_idx_encounter_id (encounter_id),
    INDEX mamba_idx_visit_id (visit_id),
    INDEX mamba_idx_person_id (person_id),
    INDEX mamba_idx_encounter_datetime (encounter_datetime),
    INDEX mamba_idx_encounter_type_uuid (encounter_type_uuid),
    INDEX mamba_idx_obs_question_concept_id (obs_question_concept_id),
    INDEX mamba_idx_obs_value_coded (obs_value_coded),
    INDEX mamba_idx_obs_value_coded_uuid (obs_value_coded_uuid),
    INDEX mamba_idx_obs_question_uuid (obs_question_uuid),
    INDEX mamba_idx_status (status),
    INDEX mamba_idx_voided (voided),
    INDEX mamba_idx_date_voided (date_voided),
    INDEX mamba_idx_order_id (order_id),
    INDEX mamba_idx_previous_version (previous_version),
    INDEX mamba_idx_obs_group_id (obs_group_id),
    INDEX mamba_idx_incremental_record (incremental_record)
)
    CHARSET = UTF8MB4;

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_insert;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_insert()
BEGIN
    DECLARE total_records INT;
    DECLARE batch_size INT DEFAULT 1000000; -- 1 million batches
    DECLARE offset INT DEFAULT 0;

    SELECT COUNT(*)
    INTO total_records
    FROM openmrs.obs o
             INNER JOIN mamba_dim_encounter e ON o.encounter_id = e.encounter_id
             INNER JOIN (SELECT DISTINCT concept_id, concept_uuid
                         FROM mamba_concept_metadata) md ON o.concept_id = md.concept_id
    WHERE o.encounter_id IS NOT NULL;

    WHILE offset < total_records
        DO
            SET @sql = CONCAT('INSERT INTO mamba_z_encounter_obs (obs_id,
                                       encounter_id,
                                       visit_id,
                                       person_id,
                                       order_id,
                                       encounter_datetime,
                                       obs_datetime,
                                       location_id,
                                       obs_group_id,
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
                                       encounter_type_uuid,
                                       status,
                                       previous_version,
                                       date_created,
                                       date_voided,
                                       voided,
                                       voided_by,
                                       void_reason)
            SELECT o.obs_id,
                   o.encounter_id,
                   e.visit_id,
                   o.person_id,
                   o.order_id,
                   e.encounter_datetime,
                   o.obs_datetime,
                   o.location_id,
                   o.obs_group_id,
                   o.concept_id     AS obs_question_concept_id,
                   o.value_text     AS obs_value_text,
                   o.value_numeric  AS obs_value_numeric,
                   o.value_coded    AS obs_value_coded,
                   o.value_datetime AS obs_value_datetime,
                   o.value_complex  AS obs_value_complex,
                   o.value_drug     AS obs_value_drug,
                   md.concept_uuid  AS obs_question_uuid,
                   NULL             AS obs_answer_uuid,
                   NULL             AS obs_value_coded_uuid,
                   e.encounter_type_uuid,
                   o.status,
                   o.previous_version,
                   o.date_created,
                   o.date_voided,
                   o.voided,
                   o.voided_by,
                   o.void_reason
            FROM openmrs.obs o
                     INNER JOIN mamba_dim_encounter e ON o.encounter_id = e.encounter_id
                     INNER JOIN (SELECT DISTINCT concept_id, concept_uuid
                                 FROM mamba_concept_metadata) md ON o.concept_id = md.concept_id
            WHERE o.encounter_id IS NOT NULL
            ORDER BY o.obs_id ASC -- Use a unique column for ordering to avoid the duplicates error because of using offset
            LIMIT ', batch_size, ' OFFSET ', offset);

            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;

            SET offset = offset + batch_size;
        END WHILE;
END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_update;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_z_encounter_obs_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_z_encounter_obs_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- update obs_value_coded (UUIDs & Concept value names)
UPDATE mamba_z_encounter_obs z
    INNER JOIN mamba_dim_concept c
    ON z.obs_value_coded = c.concept_id
SET z.obs_value_text       = c.name,
    z.obs_value_coded_uuid = c.uuid
WHERE z.obs_value_coded IS NOT NULL;

-- update column obs_value_boolean (Concept values)
UPDATE mamba_z_encounter_obs z
SET obs_value_boolean =
        CASE
            WHEN obs_value_text IN ('FALSE', 'No') THEN 0
            WHEN obs_value_text IN ('TRUE', 'Yes') THEN 1
            ELSE NULL
            END
WHERE z.obs_value_coded IS NOT NULL
  AND obs_question_concept_id in
      (SELECT DISTINCT concept_id
       FROM mamba_dim_concept c
       WHERE c.datatype = 'Boolean');

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_z_encounter_obs', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_z_encounter_obs', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_z_encounter_obs_create();
CALL sp_mamba_z_encounter_obs_insert();
CALL sp_mamba_z_encounter_obs_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_incremental_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_incremental_insert;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_incremental_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_z_encounter_obs_incremental_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_z_encounter_obs_incremental_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Insert into mamba_z_encounter_obs
INSERT INTO mamba_z_encounter_obs (obs_id,
                                   encounter_id,
                                   visit_id,
                                   person_id,
                                   order_id,
                                   encounter_datetime,
                                   obs_datetime,
                                   location_id,
                                   obs_group_id,
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
                                   encounter_type_uuid,
                                   status,
                                   previous_version,
                                   date_created,
                                   date_voided,
                                   voided,
                                   voided_by,
                                   void_reason,
                                   incremental_record)
SELECT o.obs_id,
       o.encounter_id,
       e.visit_id,
       o.person_id,
       o.order_id,
       e.encounter_datetime,
       o.obs_datetime,
       o.location_id,
       o.obs_group_id,
       o.concept_id     AS obs_question_concept_id,
       o.value_text     AS obs_value_text,
       o.value_numeric  AS obs_value_numeric,
       o.value_coded    AS obs_value_coded,
       o.value_datetime AS obs_value_datetime,
       o.value_complex  AS obs_value_complex,
       o.value_drug     AS obs_value_drug,
       md.concept_uuid  AS obs_question_uuid,
       NULL             AS obs_answer_uuid,
       NULL             AS obs_value_coded_uuid,
       e.encounter_type_uuid,
       o.status,
       o.previous_version,
       o.date_created,
       o.date_voided,
       o.voided,
       o.voided_by,
       o.void_reason,
       1
FROM openmrs.obs o
         INNER JOIN mamba_etl_incremental_columns_index_new ic ON o.obs_id = ic.incremental_table_pkey
         INNER JOIN mamba_dim_encounter e ON o.encounter_id = e.encounter_id
         INNER JOIN (SELECT DISTINCT concept_id, concept_uuid
                     FROM mamba_concept_metadata) md ON o.concept_id = md.concept_id
WHERE o.encounter_id IS NOT NULL;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_incremental_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_incremental_update;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_incremental_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_z_encounter_obs_incremental_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_z_encounter_obs_incremental_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

-- Update only Modified Records

-- Update voided Obs (FINAL & AMENDED pair obs are incremental 1 though we shall not consider them in incremental flattening)
UPDATE mamba_z_encounter_obs z
    INNER JOIN mamba_etl_incremental_columns_index_modified im
    ON z.obs_id = im.incremental_table_pkey
    INNER JOIN openmrs.obs o
    ON z.obs_id = o.obs_id
SET z.encounter_id            = o.encounter_id,
    z.person_id               = o.person_id,
    z.order_id                = o.order_id,
    z.obs_datetime            = o.obs_datetime,
    z.location_id             = o.location_id,
    z.obs_group_id            = o.obs_group_id,
    z.obs_question_concept_id = o.concept_id,
    z.obs_value_text          = o.value_text,
    z.obs_value_numeric       = o.value_numeric,
    z.obs_value_coded         = o.value_coded,
    z.obs_value_datetime      = o.value_datetime,
    z.obs_value_complex       = o.value_complex,
    z.obs_value_drug          = o.value_drug,
    -- z.encounter_type_uuid     = o.encounter_type_uuid,
    z.status                  = o.status,
    z.previous_version        = o.previous_version,
    -- z.row_num            = o.row_num,
    z.date_created            = o.date_created,
    z.voided                  = o.voided,
    z.voided_by               = o.voided_by,
    z.date_voided             = o.date_voided,
    z.void_reason             = o.void_reason,
    z.incremental_record      = 1
WHERE im.incremental_table_pkey > 1;

-- update obs_value_coded (UUIDs & Concept value names) for only NEW Obs (not voided)
UPDATE mamba_z_encounter_obs z
    INNER JOIN mamba_dim_concept c
    ON z.obs_value_coded = c.concept_id
SET z.obs_value_text       = c.name,
    z.obs_value_coded_uuid = c.uuid
WHERE z.incremental_record = 1
  AND z.obs_value_coded IS NOT NULL;

-- update column obs_value_boolean (Concept values) for only NEW Obs (not voided)
UPDATE mamba_z_encounter_obs z
SET obs_value_boolean =
        CASE
            WHEN obs_value_text IN ('FALSE', 'No') THEN 0
            WHEN obs_value_text IN ('TRUE', 'Yes') THEN 1
            ELSE NULL
            END
WHERE z.incremental_record = 1
  AND z.obs_value_coded IS NOT NULL
  AND obs_question_concept_id in
      (SELECT DISTINCT concept_id
       FROM mamba_dim_concept c
       WHERE c.datatype = 'Boolean');

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_incremental  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_incremental;


~-~-
CREATE PROCEDURE sp_mamba_z_encounter_obs_incremental()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_z_encounter_obs_incremental', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_z_encounter_obs_incremental', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN

CALL sp_mamba_etl_incremental_columns_index('obs', 'mamba_z_encounter_obs');
CALL sp_mamba_z_encounter_obs_incremental_insert();
CALL sp_mamba_z_encounter_obs_incremental_update();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_drop_and_flatten  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_drop_and_flatten;


~-~-
CREATE PROCEDURE sp_mamba_data_processing_drop_and_flatten()

BEGIN

    CALL sp_mamba_system_drop_all_tables();

    CALL sp_mamba_dim_agegroup;

    CALL sp_mamba_dim_location;

    CALL sp_mamba_dim_patient_identifier_type;

    CALL sp_mamba_dim_concept_datatype;

    CALL sp_mamba_dim_concept_name;

    CALL sp_mamba_dim_concept;

    CALL sp_mamba_dim_concept_answer;

    CALL sp_mamba_dim_encounter_type;

    CALL sp_mamba_flat_table_config;

    CALL sp_mamba_concept_metadata;

    CALL sp_mamba_dim_report_definition;

    CALL sp_mamba_dim_encounter;

    CALL sp_mamba_dim_person_name;

    CALL sp_mamba_dim_person;

    CALL sp_mamba_dim_person_attribute_type;

    CALL sp_mamba_dim_person_attribute;

    CALL sp_mamba_dim_person_address;

    CALL sp_mamba_dim_user;

    CALL sp_mamba_dim_relationship;

    CALL sp_mamba_dim_patient_identifier;

    CALL sp_mamba_dim_orders;

    CALL sp_mamba_z_encounter_obs;

    CALL sp_mamba_obs_group;

    CALL sp_mamba_flat_encounter_table_create_all;

    CALL sp_mamba_flat_encounter_table_insert_all;

    CALL sp_mamba_flat_encounter_obs_group_table_create_all;

    CALL sp_mamba_flat_encounter_obs_group_table_insert_all;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_increment_and_flatten  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_increment_and_flatten;


~-~-
CREATE PROCEDURE sp_mamba_data_processing_increment_and_flatten()

BEGIN

    CALL sp_mamba_dim_location_incremental;

    CALL sp_mamba_dim_patient_identifier_type_incremental;

    CALL sp_mamba_dim_concept_datatype_incremental;

    CALL sp_mamba_dim_concept_name_incremental;

    CALL sp_mamba_dim_concept_incremental;

    CALL sp_mamba_dim_concept_answer_incremental;

    CALL sp_mamba_dim_encounter_type_incremental;

    CALL sp_mamba_flat_table_config_incremental;

    CALL sp_mamba_concept_metadata_incremental;

    CALL sp_mamba_dim_encounter_incremental;

    CALL sp_mamba_dim_person_name_incremental;

    CALL sp_mamba_dim_person_incremental;

    CALL sp_mamba_dim_person_attribute_type_incremental;

    CALL sp_mamba_dim_person_attribute_incremental;

    CALL sp_mamba_dim_person_address_incremental;

    CALL sp_mamba_dim_user_incremental;

    CALL sp_mamba_dim_relationship_incremental;

    CALL sp_mamba_dim_patient_identifier_incremental;

    CALL sp_mamba_dim_orders_incremental;

    CALL sp_mamba_z_encounter_obs_incremental;

    CALL sp_mamba_flat_table_incremental_create_all;

    CALL sp_mamba_flat_table_incremental_insert_all;

    CALL sp_mamba_flat_table_obs_incremental_insert_all;

    CALL sp_mamba_reset_incremental_update_flag_all;

END;
~-~-



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_covid  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_covid;


~-~-
CREATE PROCEDURE sp_mamba_data_processing_derived_covid()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_data_processing_derived_covid', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_data_processing_derived_covid', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_dim_client_covid;
CALL sp_mamba_fact_encounter_covid;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_hts  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_hts;


~-~-
CREATE PROCEDURE sp_mamba_data_processing_derived_hts()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_data_processing_derived_hts', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_data_processing_derived_hts', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_dim_client_hts;
CALL sp_mamba_fact_encounter_hts;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_pmtct  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_pmtct;


~-~-
CREATE PROCEDURE sp_mamba_data_processing_derived_pmtct()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_data_processing_derived_pmtct', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_data_processing_derived_pmtct', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_fact_exposedinfants;
CALL sp_mamba_fact_pregnant_women;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_etl  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_etl;


~-~-
CREATE PROCEDURE sp_mamba_data_processing_etl()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_data_processing_etl', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_data_processing_etl', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- add base folder SP here --

-- Flatten the tables first
CALL sp_mamba_data_processing_drop_and_flatten();

-- Call the ETL process
CALL sp_mamba_data_processing_derived_hts();
CALL sp_mamba_data_processing_derived_pmtct();
-- CALL sp_mamba_data_processing_derived_covid();

-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_covid_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_client_covid_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_client_covid_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_client_covid_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_client_covid_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_covid_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_client_covid_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_client_covid_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_client_covid_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_client_covid_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_covid_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_client_covid_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_client_covid_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_client_covid_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_client_covid_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_covid  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_client_covid;


~-~-
CREATE PROCEDURE sp_mamba_dim_client_covid()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_client_covid', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_client_covid', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_dim_client_covid_create();
CALL sp_mamba_dim_client_covid_insert();
CALL sp_mamba_dim_client_covid_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_covid_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_covid_create;


~-~-
CREATE PROCEDURE sp_mamba_fact_encounter_covid_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_encounter_covid_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_encounter_covid_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_covid_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_covid_insert;


~-~-
CREATE PROCEDURE sp_mamba_fact_encounter_covid_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_encounter_covid_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_encounter_covid_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_covid_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_covid_update;


~-~-
CREATE PROCEDURE sp_mamba_fact_encounter_covid_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_encounter_covid_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_encounter_covid_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_covid  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_covid;


~-~-
CREATE PROCEDURE sp_mamba_fact_encounter_covid()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_encounter_covid', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_encounter_covid', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_fact_encounter_covid_create();
CALL sp_mamba_fact_encounter_covid_insert();
CALL sp_mamba_fact_encounter_covid_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_covid  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_covid;


~-~-
CREATE PROCEDURE sp_mamba_data_processing_derived_covid()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_data_processing_derived_covid', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_data_processing_derived_covid', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_dim_client_covid;
CALL sp_mamba_fact_encounter_covid;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_hts_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_client_hts_create;


~-~-
CREATE PROCEDURE sp_mamba_dim_client_hts_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_client_hts_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_client_hts_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_hts_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_client_hts_insert;


~-~-
CREATE PROCEDURE sp_mamba_dim_client_hts_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_client_hts_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_client_hts_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_hts_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_client_hts_update;


~-~-
CREATE PROCEDURE sp_mamba_dim_client_hts_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_client_hts_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_client_hts_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_client_hts  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_dim_client_hts;


~-~-
CREATE PROCEDURE sp_mamba_dim_client_hts()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_dim_client_hts', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_dim_client_hts', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_dim_client_hts_create();
CALL sp_mamba_dim_client_hts_insert();
CALL sp_mamba_dim_client_hts_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_hts_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_hts_create;


~-~-
CREATE PROCEDURE sp_mamba_fact_encounter_hts_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_encounter_hts_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_encounter_hts_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_hts_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_hts_insert;


~-~-
CREATE PROCEDURE sp_mamba_fact_encounter_hts_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_encounter_hts_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_encounter_hts_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
INSERT INTO mamba_fact_encounter_hts
(encounter_id,
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
 recency_rtri_result)
SELECT hts.encounter_id,
       hts.client_id,
       hts.encounter_datetime                AS encounter_date,
       CAST(date_test_conducted AS DATE) AS date_tested,
       CASE consent_provided
           WHEN 'True' THEN 'Yes'
           WHEN 'False' THEN 'No'
           ELSE NULL END                 AS consent,
       CASE community_service_point
           WHEN 'mobile voluntary counseling and testing program' THEN 'Mobile VCT'
           WHEN 'Home based HIV testing program' THEN 'Homebased'
           WHEN 'Outreach Program' THEN 'Outreach'
           WHEN 'Voluntary counseling and testing center' THEN 'VCT'
           ELSE community_service_point
           END                           AS community_service_point,
       pop_type,
       CASE
           WHEN (key_pop_msm = 'Male who has sex with men') THEN 'MSM'
           WHEN (key_pop_fsw = 'Sex worker') THEN 'FSW'
           WHEN (key_pop_transgender = 'Transgender Persons') THEN 'TRANS'
           WHEN (key_pop_pwid = 'People Who Inject Drugs') THEN 'PWID'
           WHEN (key_pop_prisoners = 'Prisoners') THEN 'Prisoner'
           ELSE NULL
           END                           AS `keypop_category`,
       CASE
           WHEN (key_pop_AGYW = 'Adolescent Girls & Young Women') THEN 'AGYW'
           WHEN (key_pop_fisher_folk = 'Fisher Folk') THEN 'Fisher_folk'
           WHEN (key_pop_migrant_worker = 'Migrant Workers') THEN 'Migrant_worker'
           WHEN (key_pop_refugees = 'Refugees') THEN 'Refugees'
           WHEN (key_pop_truck_driver = 'Long distance truck driver') THEN 'Truck_driver'
           WHEN (key_pop_uniformed_forces = 'Uniformed Forces') THEN 'Uniformed_forces'
           ELSE NULL
           END                           AS `priority_pop`,
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
           END                           AS facility_service_point,
       CASE hts_approach
           WHEN 'Client Initiated Testing and Counselling' THEN 'CITC'
           WHEN 'Provider-initiated HIV testing and counseling' THEN 'PITC'
           ELSE hts_approach
           END                           AS hts_approach,
       pretest_counselling,
       type_pretest_counselling,
       reason_for_test,
       CASE ever_tested_hiv
           WHEN 'True' THEN 'Yes'
           WHEN 'False' THEN 'No'
           ELSE ever_tested_hiv
           END                           AS ever_tested_hiv,
       duration_since_last_test,
       couple_result,
       result_received_couple,
       test_conducted,
       initial_kit_name,
       initial_test_result,
       confirmatory_kit_name,
       last_test_result,
       CASE
           WHEN final_test_result IN ('+', 'POS', 'Positive') THEN 'Positive'
           WHEN final_test_result IN ('-', 'NEG', 'Negative') THEN 'Negative'
           WHEN final_test_result IN ('Indeterminate', 'Inconclusive') THEN 'Indeterminate'
           ELSE final_test_result
           END                           AS final_test_result,
       CASE
           WHEN given_result IN ('True', 'Yes') THEN 'Yes'
           WHEN given_result IN ('No', 'False') THEN 'No'
           WHEN given_result = 'Unknown' THEN 'Unknown'
           ELSE given_result
           END                           AS given_result,
       CAST(date_given_result AS DATE)   AS date_given_result,
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
FROM `mamba_flat_encounter_hts` `hts`
         inner join `mamba_flat_encounter_hts_1` `hts1` on `hts`.`encounter_id` = `hts1`.`encounter_id`;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_hts_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_hts_update;


~-~-
CREATE PROCEDURE sp_mamba_fact_encounter_hts_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_encounter_hts_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_encounter_hts_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_encounter_hts  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_encounter_hts;


~-~-
CREATE PROCEDURE sp_mamba_fact_encounter_hts()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_encounter_hts', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_encounter_hts', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_fact_encounter_hts_create();
-- CALL sp_mamba_fact_encounter_hts_insert();
CALL sp_mamba_fact_encounter_hts_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_txcurr_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_txcurr_create;


~-~-
CREATE PROCEDURE sp_mamba_fact_txcurr_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_txcurr_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_txcurr_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_txcurr_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_txcurr_insert;


~-~-
CREATE PROCEDURE sp_mamba_fact_txcurr_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_txcurr_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_txcurr_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
FROM `mamba_flat_encounter_hts` `hts`;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_txcurr_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_txcurr_update;


~-~-
CREATE PROCEDURE sp_mamba_fact_txcurr_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_txcurr_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_txcurr_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_txcurr  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_txcurr;


~-~-
CREATE PROCEDURE sp_mamba_fact_txcurr()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_txcurr', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_txcurr', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_fact_txcurr_create();
CALL sp_mamba_fact_txcurr_insert();
CALL sp_mamba_fact_txcurr_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_txcurr_query  ----------------------------
-- ---------------------------------------------------------------------------------------------




        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_hts  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_hts;


~-~-
CREATE PROCEDURE sp_mamba_data_processing_derived_hts()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_data_processing_derived_hts', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_data_processing_derived_hts', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_dim_client_hts;
CALL sp_mamba_fact_encounter_hts;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_exposedinfants_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_exposedinfants_create;


~-~-
CREATE PROCEDURE sp_mamba_fact_exposedinfants_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_exposedinfants_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_exposedinfants_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_exposedinfants_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_exposedinfants_insert;


~-~-
CREATE PROCEDURE sp_mamba_fact_exposedinfants_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_exposedinfants_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_exposedinfants_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
    LEFT JOIN mamba_dim_relationship a ON  ip.client_id = a.person_b
    WHERE   (ip.client_id in (SELECT person_b FROM mamba_dim_relationship a
                INNER JOIN mamba_flat_encounter_pmtct_anc anc
                    ON a.person_a = anc.client_id
                WHERE (anc.hiv_test_result ='HIV Positive'
                           OR anc.hiv_test_performed = 'Previously known positive'))
            OR ip.client_id in (SELECT person_b FROM mamba_dim_relationship a
                INNER JOIN mamba_flat_encounter_pmtct_labor_delivery ld
                    ON a.person_a = ld.client_id
                where (ld.result_of_hiv_test ='HIV Positive'
                           OR ld.hiv_test_performed = 'Previously known positive'
                           OR ld.anc_hiv_status_first_visit like '%Positive%'))
            OR ip.client_id in (SELECT person_b FROM mamba_dim_relationship a
                INNER JOIN mamba_flat_encounter_pmtct_mother_postnatal mp
                    ON a.person_a = mp.client_id
                where (mp.result_of_hiv_test like '%Positive%'
                           OR mp.hiv_test_performed = 'Previously known positive')))
;
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_exposedinfants_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_exposedinfants_update;


~-~-
CREATE PROCEDURE sp_mamba_fact_exposedinfants_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_exposedinfants_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_exposedinfants_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_exposedinfants  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_exposedinfants;


~-~-
CREATE PROCEDURE sp_mamba_fact_exposedinfants()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_exposedinfants', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_exposedinfants', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_fact_exposedinfants_create();
CALL sp_mamba_fact_exposedinfants_insert();
CALL sp_mamba_fact_exposedinfants_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_pregnant_women_create  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_pregnant_women_create;


~-~-
CREATE PROCEDURE sp_mamba_fact_pregnant_women_create()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_pregnant_women_create', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_pregnant_women_create', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_pregnant_women_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_pregnant_women_insert;


~-~-
CREATE PROCEDURE sp_mamba_fact_pregnant_women_insert()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_pregnant_women_insert', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_pregnant_women_insert', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

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
WHERE visit_type like 'New %'
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
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_pregnant_women_update  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_pregnant_women_update;


~-~-
CREATE PROCEDURE sp_mamba_fact_pregnant_women_update()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_pregnant_women_update', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_pregnant_women_update', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_pregnant_women  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_fact_pregnant_women;


~-~-
CREATE PROCEDURE sp_mamba_fact_pregnant_women()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_fact_pregnant_women', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_fact_pregnant_women', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_fact_pregnant_women_create();
CALL sp_mamba_fact_pregnant_women_insert();
CALL sp_mamba_fact_pregnant_women_update();
-- $END
END;
~-~-


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_pmtct  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_pmtct;


~-~-
CREATE PROCEDURE sp_mamba_data_processing_derived_pmtct()
BEGIN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1

    @message_text = MESSAGE_TEXT,
    @mysql_errno = MYSQL_ERRNO,
    @returned_sqlstate = RETURNED_SQLSTATE;

    CALL sp_mamba_etl_error_log_insert('sp_mamba_data_processing_derived_pmtct', @message_text, @mysql_errno, @returned_sqlstate);

    UPDATE _mamba_etl_schedule
    SET end_time                   = NOW(),
        completion_status          = 'ERROR',
        transaction_status         = 'COMPLETED',
        success_or_error_message   = CONCAT('sp_mamba_data_processing_derived_pmtct', ', ', @mysql_errno, ', ', @message_text)
        WHERE id = (SELECT last_etl_schedule_insert_id FROM _mamba_etl_user_settings ORDER BY id DESC LIMIT 1);

    RESIGNAL;
END;

-- $BEGIN
CALL sp_mamba_fact_exposedinfants;
CALL sp_mamba_fact_pregnant_women;
-- $END
END;
~-~-



-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_mother_hiv_status_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_mother_hiv_status_query;


~-~-
CREATE PROCEDURE sp_mamba_mother_hiv_status_query(IN ptracker_id VARCHAR(255), IN person_uuid VARCHAR(255))
BEGIN

SELECT pm.encounter_id, pm.client_id, DATE_FORMAT(pm.encounter_datetime, '%Y-%m-%d %H:%i:%s') AS encounter_datetime, pm.visit_type, pm.hiv_test_result AS hiv_test_result, pm.hiv_test_performed, pm.partner_hiv_tested, pm.previously_known_positive, DATE_FORMAT(pm.return_visit_date, '%Y-%m-%d') AS return_visit_date FROM mamba_flat_encounter_pmtct_anc pm INNER JOIN mamba_dim_person p ON pm.client_id = p.person_id WHERE p.uuid = person_uuid AND pm.ptracker_id = ptracker_id;

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_mother_hiv_status_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_mother_hiv_status_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_mother_hiv_status_columns_query(IN ptracker_id VARCHAR(255), IN person_uuid VARCHAR(255))
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_mother_hiv_status;
CREATE TABLE mamba_dim_mother_hiv_status AS
SELECT pm.encounter_id, pm.client_id, DATE_FORMAT(pm.encounter_datetime, '%Y-%m-%d %H:%i:%s') AS encounter_datetime, pm.visit_type, pm.hiv_test_result AS hiv_test_result, pm.hiv_test_performed, pm.partner_hiv_tested, pm.previously_known_positive, DATE_FORMAT(pm.return_visit_date, '%Y-%m-%d') AS return_visit_date FROM mamba_flat_encounter_pmtct_anc pm INNER JOIN mamba_dim_person p ON pm.client_id = p.person_id WHERE p.uuid = person_uuid AND pm.ptracker_id = ptracker_id
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

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_deliveries_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_deliveries_query;


~-~-
CREATE PROCEDURE sp_mamba_total_deliveries_query()
BEGIN

SELECT COUNT(*) AS total_deliveries FROM mamba_dim_encounter e inner join mamba_dim_encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '6dc5308d-27c9-4d49-b16f-2c5e3c759757' AND DATE(e.encounter_datetime) > CONCAT(YEAR(CURDATE()), '-01-01 00:00:00');

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_deliveries_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_deliveries_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_total_deliveries_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_total_deliveries;
CREATE TABLE mamba_dim_total_deliveries AS
SELECT COUNT(*) AS total_deliveries FROM mamba_dim_encounter e inner join mamba_dim_encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '6dc5308d-27c9-4d49-b16f-2c5e3c759757' AND DATE(e.encounter_datetime) > CONCAT(YEAR(CURDATE()), '-01-01 00:00:00')
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

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_hiv_exposed_infants_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_hiv_exposed_infants_query;


~-~-
CREATE PROCEDURE sp_mamba_total_hiv_exposed_infants_query()
BEGIN

SELECT COUNT(DISTINCT ei.infant_client_id) AS total_hiv_exposed_infants FROM mamba_fact_pmtct_exposedinfants ei INNER JOIN mamba_dim_person p ON ei.infant_client_id = p.person_id WHERE ei.encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW() AND birthdate BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW();

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_hiv_exposed_infants_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_hiv_exposed_infants_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_total_hiv_exposed_infants_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_total_hiv_exposed_infants;
CREATE TABLE mamba_dim_total_hiv_exposed_infants AS
SELECT COUNT(DISTINCT ei.infant_client_id) AS total_hiv_exposed_infants FROM mamba_fact_pmtct_exposedinfants ei INNER JOIN mamba_dim_person p ON ei.infant_client_id = p.person_id WHERE ei.encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW() AND birthdate BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW()
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_total_hiv_exposed_infants';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_hiv_exposed_infants';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_pregnant_women_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_pregnant_women_query;


~-~-
CREATE PROCEDURE sp_mamba_total_pregnant_women_query()
BEGIN

SELECT COUNT(DISTINCT pw.client_id) AS total_pregnant_women FROM mamba_fact_pmtct_pregnant_women pw WHERE visit_type like 'New%' AND encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW() AND DATE_ADD(date_of_last_menstrual_period, INTERVAL 40 WEEK) > NOW();

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_pregnant_women_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_pregnant_women_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_total_pregnant_women_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_total_pregnant_women;
CREATE TABLE mamba_dim_total_pregnant_women AS
SELECT COUNT(DISTINCT pw.client_id) AS total_pregnant_women FROM mamba_fact_pmtct_pregnant_women pw WHERE visit_type like 'New%' AND encounter_datetime BETWEEN DATE_FORMAT(NOW(), '%Y-01-01') AND NOW() AND DATE_ADD(date_of_last_menstrual_period, INTERVAL 40 WEEK) > NOW()
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

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_active_dr_cases_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_active_dr_cases_query;


~-~-
CREATE PROCEDURE sp_mamba_total_active_dr_cases_query()
BEGIN

SELECT COUNT(DISTINCT e.patient_id) AS total_active_dr FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND o.value_coded = (SELECT concept_id FROM openmrs.concept WHERE uuid ='160052AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled);

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_active_dr_cases_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_active_dr_cases_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_total_active_dr_cases_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_total_active_dr_cases;
CREATE TABLE mamba_dim_total_active_dr_cases AS
SELECT COUNT(DISTINCT e.patient_id) AS total_active_dr FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND o.value_coded = (SELECT concept_id FROM openmrs.concept WHERE uuid ='160052AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_total_active_dr_cases';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_active_dr_cases';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_active_ds_cases_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_active_ds_cases_query;


~-~-
CREATE PROCEDURE sp_mamba_total_active_ds_cases_query()
BEGIN

SELECT COUNT(DISTINCT e.patient_id) AS total_active_ds FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND o.value_coded = (SELECT concept_id FROM openmrs.concept WHERE uuid ='160541AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled);

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_active_ds_cases_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_active_ds_cases_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_total_active_ds_cases_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_total_active_ds_cases;
CREATE TABLE mamba_dim_total_active_ds_cases AS
SELECT COUNT(DISTINCT e.patient_id) AS total_active_ds FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id, max(value_datetime)date_enrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='161552AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) ed ON ed.patient_id = e.patient_id LEFT JOIN ( SELECT DISTINCT e.patient_id, max(value_datetime)date_disenrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='159431AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') group by e.patient_id ) dd ON dd.patient_id = e.patient_id WHERE et.uuid = '9a199b59-b185-485b-b9b3-a9754e65ae57' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid ='163775AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND o.value_coded = (SELECT concept_id FROM openmrs.concept WHERE uuid ='160541AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AND ( dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_total_active_ds_cases';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_active_ds_cases';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_mother_status_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_mother_status_query;


~-~-
CREATE PROCEDURE sp_mamba_mother_status_query(IN person_uuid VARCHAR(255))
BEGIN

SELECT cn.name as mother_status FROM openmrs.obs o INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.concept_name cn on o.value_coded = cn.concept_id WHERE p.uuid = person_uuid AND c.uuid = '1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.encounter_id = (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '6dc5308d-27c9-4d49-b16f-2c5e3c759757' AND c.uuid = '1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) AND cn.voided = 0 AND cn.locale_preferred = 1;

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_mother_status_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_mother_status_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_mother_status_columns_query(IN person_uuid VARCHAR(255))
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_mother_status;
CREATE TABLE mamba_dim_mother_status AS
SELECT cn.name as mother_status FROM openmrs.obs o INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.concept_name cn on o.value_coded = cn.concept_id WHERE p.uuid = person_uuid AND c.uuid = '1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.encounter_id = (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '6dc5308d-27c9-4d49-b16f-2c5e3c759757' AND c.uuid = '1856AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) AND cn.voided = 0 AND cn.locale_preferred = 1
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_mother_status';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='mother_status';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_estimated_date_of_delivery_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_estimated_date_of_delivery_query;


~-~-
CREATE PROCEDURE sp_mamba_estimated_date_of_delivery_query(IN person_uuid VARCHAR(255))
BEGIN

(SELECT CASE WHEN o.value_datetime >=curdate() THEN DATE_FORMAT(o.value_datetime, '%d %m %Y') ELSE '' END AS estimated_date_of_delivery FROM openmrs.obs o INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.concept c on o.concept_id = c.concept_id  WHERE p.uuid = person_uuid AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.encounter_id = (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) ORDER BY o.value_datetime DESC  LIMIT 1);

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_estimated_date_of_delivery_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_estimated_date_of_delivery_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_estimated_date_of_delivery_columns_query(IN person_uuid VARCHAR(255))
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_estimated_date_of_delivery;
CREATE TABLE mamba_dim_estimated_date_of_delivery AS
(SELECT CASE WHEN o.value_datetime >=curdate() THEN DATE_FORMAT(o.value_datetime, '%d %m %Y') ELSE '' END AS estimated_date_of_delivery FROM openmrs.obs o INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.concept c on o.concept_id = c.concept_id  WHERE p.uuid = person_uuid AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.encounter_id = (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY encounter_datetime desc  LIMIT 1) ORDER BY o.value_datetime DESC  LIMIT 1)
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_estimated_date_of_delivery';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='estimated_date_of_delivery';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_next_appointment_date_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_next_appointment_date_query;


~-~-
CREATE PROCEDURE sp_mamba_next_appointment_date_query(IN person_uuid VARCHAR(255))
BEGIN

(SELECT DATE_FORMAT(o.value_datetime, '%d %m %Y') as next_appointment FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid) a ON o.encounter_id = a.encounter_id LEFT JOIN (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id  INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '4362fd2d-1866-4ea0-84ef-5e5da9627440' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid)b  on o.encounter_id = b.encounter_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY  encounter_datetime DESC LIMIT 1);

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_next_appointment_date_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_next_appointment_date_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_next_appointment_date_columns_query(IN person_uuid VARCHAR(255))
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_next_appointment_date;
CREATE TABLE mamba_dim_next_appointment_date AS
(SELECT DATE_FORMAT(o.value_datetime, '%d %m %Y') as next_appointment FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid) a ON o.encounter_id = a.encounter_id LEFT JOIN (SELECT e.encounter_id FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id  INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '4362fd2d-1866-4ea0-84ef-5e5da9627440' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid)b  on o.encounter_id = b.encounter_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5096AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND p.uuid = person_uuid ORDER BY  encounter_datetime DESC LIMIT 1)
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_next_appointment_date';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='next_appointment_date';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_no_of_anc_visits_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_no_of_anc_visits_query;


~-~-
CREATE PROCEDURE sp_mamba_no_of_anc_visits_query(IN person_uuid VARCHAR(255))
BEGIN

SELECT COUNT(DISTINCT e.encounter_id)no_of_anc_visits FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.value_datetime >= curdate() AND p.uuid = person_uuid;

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_no_of_anc_visits_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_no_of_anc_visits_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_no_of_anc_visits_columns_query(IN person_uuid VARCHAR(255))
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_no_of_anc_visits;
CREATE TABLE mamba_dim_no_of_anc_visits AS
SELECT COUNT(DISTINCT e.encounter_id)no_of_anc_visits FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = '677d1a80-dbbe-4399-be34-aa7f54f11405' AND c.uuid = '5596AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' AND o.value_datetime >= curdate() AND p.uuid = person_uuid
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_no_of_anc_visits';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='no_of_anc_visits';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_active_tpt_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_active_tpt_query;


~-~-
CREATE PROCEDURE sp_mamba_total_active_tpt_query()
BEGIN

SELECT DISTINCT COUNT(DISTINCT e.patient_id) AS total_active_tpt FROM openmrs.obs o  INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id,max(value_datetime) date_enrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id  WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND o.concept_id =  (SELECT concept_id FROM openmrs.concept WHERE uuid = '162320AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') GROUP BY e.patient_id) ed ON ed.patient_id = e.patient_id LEFT JOIN(SELECT DISTINCT e.patient_id, max(value_datetime) date_disenrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid = '163284AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') GROUP BY e.patient_id) dd ON dd.patient_id = e.patient_id WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND (dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled);

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_total_active_tpt_columns_query  ----------------------------
-- ---------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_mamba_total_active_tpt_columns_query;


~-~-
CREATE PROCEDURE sp_mamba_total_active_tpt_columns_query()
BEGIN

-- Create Table to store report column names with no rows
DROP TABLE IF EXISTS mamba_dim_total_active_tpt;
CREATE TABLE mamba_dim_total_active_tpt AS
SELECT DISTINCT COUNT(DISTINCT e.patient_id) AS total_active_tpt FROM openmrs.obs o  INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id LEFT JOIN (SELECT DISTINCT e.patient_id,max(value_datetime) date_enrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id  WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND o.concept_id =  (SELECT concept_id FROM openmrs.concept WHERE uuid = '162320AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') GROUP BY e.patient_id) ed ON ed.patient_id = e.patient_id LEFT JOIN(SELECT DISTINCT e.patient_id, max(value_datetime) date_disenrolled FROM openmrs.obs o INNER JOIN openmrs.concept c on o.concept_id = c.concept_id INNER JOIN openmrs.person p on o.person_id = p.person_id INNER JOIN openmrs.encounter e on o.encounter_id = e.encounter_id INNER JOIN openmrs.encounter_type et on e.encounter_type = et.encounter_type_id WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND o.concept_id = (SELECT concept_id FROM openmrs.concept WHERE uuid = '163284AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') GROUP BY e.patient_id) dd ON dd.patient_id = e.patient_id WHERE et.uuid = 'dc6ce80c-83f8-4ace-a638-21df78542551' AND (dd.date_disenrolled IS NULL OR ed.date_enrolled > dd.date_disenrolled)
LIMIT 0;

-- Select report column names from Table
SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
INTO @column_names
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'mamba_dim_total_active_tpt';

-- Update Table with report column names
UPDATE mamba_dim_report_definition
SET result_column_names = @column_names
WHERE report_id='total_active_tpt';

END;
~-~-




-- ---------------------------------------------------------------------------------------------
-- ----------------------------  Setup the MambaETL Scheduler  ---------------------------------
-- ---------------------------------------------------------------------------------------------


-- Enable the event etl_scheduler
SET GLOBAL event_scheduler = ON;

~-~-

-- Drop/Create the Event responsible for firing up the ETL process
DROP EVENT IF EXISTS _mamba_etl_scheduler_event;

~-~-

-- Setup ETL configurations
CALL sp_mamba_etl_setup(?, ?, ?, ?, ?);
-- pass them from the runtime properties file

~-~-

CREATE EVENT IF NOT EXISTS _mamba_etl_scheduler_event
    ON SCHEDULE EVERY ? SECOND
        STARTS CURRENT_TIMESTAMP
    DO CALL sp_mamba_etl_schedule();

~-~-

