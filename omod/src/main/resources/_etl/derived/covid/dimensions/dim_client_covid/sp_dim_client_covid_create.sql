-- $BEGIN

create table  dim_client_covid(
    id   int auto_increment,
    client_id     int null,
    date_of_birth date null,
    ageattest     int null,
    sex           nvarchar(50) null,
    county        nvarchar(255) null,
    sub_county    nvarchar(255) null,
    ward          nvarchar(255) null,
    PRIMARY KEY (id)
);

-- $END

