DELIMITER //

DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_procedures_in_schema;

CREATE PROCEDURE sp_xf_system_drop_all_stored_procedures_in_schema(
    IN database_name NVARCHAR(255)
)
BEGIN

    DELETE FROM `mysql`.`proc` WHERE `type` = 'PROCEDURE' AND `db` = database_name; -- works in mysql before v.8

END//

DELIMITER ;