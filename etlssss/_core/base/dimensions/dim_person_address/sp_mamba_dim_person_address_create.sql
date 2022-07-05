USE analysis;

DROP TABLE IF EXISTS mamba_dim_person_address;

-- $BEGIN
CREATE TABLE mamba_dim_person_address
(
    mamba_person_address_id INT           NOT NULL AUTO_INCREMENT,
    person_address_id       INT           NOT NULL,
    person_id               INT,
    city_village            NVARCHAR(255) NULL,
    county_district         NVARCHAR(255) NULL,
    address1                NVARCHAR(255) NULL,
    address2                NVARCHAR(255) NULL,
    PRIMARY KEY (mamba_person_address_id)
);

CREATE INDEX index_person_address_id
    ON mamba_dim_person_address (person_address_id);

CREATE INDEX index_person_id
    ON mamba_dim_person_address (person_id);
-- $END
