CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.microbiology_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    mt.order_code AS order_code,
    mt.order_name AS order_name,
    mt.sample_collected_datetime AS sample_collected_datetime,
    mt.sample_received_datetime AS sample_received_datetime,
    mt.result_datetime AS result_datetime,
    mt.specimen_site AS specimen_site,
    mt.organism AS organism,
    mt.result AS result
FROM hic_covid.microbiology_test mt
JOIN hic_covid.demographics d
    ON d.uhl_system_number = mt.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
