CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.episode_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    COALESCE(e_.spell_id, '') AS spell_id,
    COALESCE(e_.episode_id, '') AS episode_id,
    COALESCE(e_.admission_datetime, '') AS admission_datetime,
    COALESCE(e_.discharge_datetime, '') AS discharge_datetime,
    COALESCE(e_.order_no_of_episode, '') AS order_no_of_episode,
    COALESCE(e_.admission_method_code, '') AS admission_method_code,
    COALESCE(e_.admission_method_name, '') AS admission_method_name,
    COALESCE(e_.admission_source_code, '') AS admission_source_code,
    COALESCE(e_.admission_source_name, '') AS admission_source_name,
    COALESCE(e_.discharge_method_code, '') AS discharge_method_code,
    COALESCE(e_.discharge_method_name, '') AS discharge_method_name,
    COALESCE(e_.speciality_code, '') AS speciality_code,
    COALESCE(e_.speciality_name, '') AS speciality_name,
    COALESCE(e_.treatment_function_code, '') AS treatment_function_code,
    COALESCE(e_.treatment_function_name, '') AS treatment_function_name
FROM hic_covid.episode e_
JOIN hic_covid.demographics d
    ON d.uhl_system_number = e_.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
