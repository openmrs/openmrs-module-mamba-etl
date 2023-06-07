-- $BEGIN
CREATE TABLE IF NOT EXISTS mamba_dim_client_hts
(
    id            INT           NOT NULL AUTO_INCREMENT,
    client_id     INT           NOT NULL,
    date_of_birth DATE          NULL,
    date_of_test  DATE          NULL,
    age_at_test   INT           NULL,
    sex           NVARCHAR(25)  NULL,
    county        NVARCHAR(255) NULL,
    sub_county    NVARCHAR(255) NULL,
    ward          NVARCHAR(255) NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;
-- $END

