CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.bloods_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    bt.test_code AS test_code,
    bt.test_name AS test_name,
    bt.result AS result,
    bt.result_expansion AS result_expansion,
    bt.result_units AS result_units,
    bt.sample_collected_datetime AS sample_collected_datetime,
    bt.result_datetime AS result_datetime,
    bt.lower_range AS lower_range,
    bt.higher_range AS higher_range
FROM hic_covid.blood_test bt
JOIN hic_covid.demographics d
    ON d.uhl_system_number = bt.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
