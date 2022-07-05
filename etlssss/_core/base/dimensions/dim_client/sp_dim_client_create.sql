USE analysis;

DROP TABLE IF EXISTS dim_client;

-- $BEGIN
CREATE TABLE dim_client
(
    id            INT           NOT NULL AUTO_INCREMENT,
    client_id     INT,
    date_of_birth DATE          NULL,
    age           INT,
    sex           NVARCHAR(255) NULL,
    county        NVARCHAR(255) NULL,
    sub_county    NVARCHAR(255) NULL,
    ward          NVARCHAR(255) NULL,
    PRIMARY KEY (id)
);

CREATE INDEX index_client_id
    ON dim_client (client_id);
-- $END
