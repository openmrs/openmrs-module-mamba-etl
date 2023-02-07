DELIMITER //

DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_views_in_schema//

CREATE PROCEDURE sp_xf_system_drop_all_views_in_schema(
    IN database_name NVARCHAR(255)
)
BEGIN

    DECLARE tables_count INT;

    SELECT COUNT(1) INTO tables_count FROM information_schema.tables
    WHERE TABLE_TYPE = 'VIEW' AND TABLE_SCHEMA = database_name;

    IF tables_count > 0 THEN

        SET session group_concat_max_len = 20000;

        SET @tbls = (SELECT GROUP_CONCAT(database_name, '.', TABLE_NAME SEPARATOR ', ')
                     FROM information_schema.tables
                     WHERE TABLE_TYPE = 'VIEW'
                       AND TABLE_SCHEMA = database_name);

        SET @drop_views = CONCAT('DROP VIEW IF EXISTS ', @tbls);

        SET foreign_key_checks = 0; -- Remove check, so we don't have to drop views in the correct order, nor whether they actually exist.
        PREPARE drop_tbls FROM @drop_views;
        EXECUTE drop_tbls;
        DEALLOCATE PREPARE drop_tbls;
        SET foreign_key_checks = 1;

    END IF;

END//

DELIMITER ;