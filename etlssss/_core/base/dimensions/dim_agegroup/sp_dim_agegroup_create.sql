USE analysis;

DROP TABLE IF EXISTS dim_agegroup;

-- $BEGIN
CREATE TABLE dim_agegroup
(
    dim_age_id      int auto_increment primary key,
    age             int          NULL,
    datim_agegroup  nvarchar(50) NULL,
    normal_agegroup nvarchar(50) NULL
);
-- $END