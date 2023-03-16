-- $BEGIN
INSERT INTO dim_client_hiv_hts (client_id,
                                date_of_birth,
                                ageattest,
                                sex,
                                county,
                                sub_county,
                                ward)
SELECT c.client_id,
       date_of_birth,
       DATEDIFF(date_test_conducted, date_of_birth) / 365 as ageattest,
       sex,
       county,
       sub_county,
       ward
FROM dim_client c
         INNER JOIN flat_encounter_hts hts
                    ON c.client_id = hts.client_id;
-- $END
