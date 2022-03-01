CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.virology_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    COALESCE(v.laboratory_code, '') AS laboratory_code,
    COALESCE(v.order_code, '') AS order_code,
    COALESCE(v.order_name, '') AS order_name,
    COALESCE(v.test_code, '') AS test_code,
    COALESCE(v.test_name, '') AS test_name,
    COALESCE(v.test_result, '') AS test_result,
    COALESCE(v.sample_collected_date_time, '') AS sample_collected_date_time,
    COALESCE(v.sample_received_date_time, '') AS sample_received_date_time,
    COALESCE(v.sample_available_date_time, '') AS result_available_date_time
FROM hic_covid.virology v
JOIN hic_covid.demographics d
    ON d.uhl_system_number = v.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
