USE analysis;

DROP TABLE IF EXISTS dim_agegroup;

-- $BEGIN
CREATE TABLE dim_agegroup
(
    dim_age_id      INT          NOT NULL AUTO_INCREMENT,
    age             INT          NULL,
    datim_agegroup  NVARCHAR(50) NULL,
    normal_agegroup NVARCHAR(50) NULL,
    PRIMARY KEY (dim_age_id)
);
-- $END
