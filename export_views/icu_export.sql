CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.icu_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    ccp.ccp_id AS critical_care_id,
    ccp.treatment_function_name AS treatment_function_name,
    ccp.start_datetime AS start_datetime,
    ccp.basic_respiratory_support_days AS basic_respiratory_support_days,
    ccp.advanced_respiratory_support_days AS advanced_respiratory_support_days,
    ccp.basic_cardiovascular_support_days AS basic_cardiovascular_support_days,
    ccp.advanced_cardiovascular_support_days AS advanced_cardiovascular_support_days,
    ccp.renal_support_days AS renal_support_days,
    ccp.neurological_support_days AS neurological_support_days,
    ccp.dermatological_support_days AS dermatological_support_days,
    ccp.liver_support_days AS liver_support_days,
    ccp.critical_care_level_2_days AS critical_care_level_2_days,
    ccp.critical_care_level_3_days AS critical_care_level_3_days,
    ccp.discharge_datetime AS discharge_datetime
FROM hic_covid.critical_care_period ccp
JOIN hic_covid.demographics d
    ON d.uhl_system_number = ccp.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
