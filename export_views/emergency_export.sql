CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.emergency_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    COALESCE(e_.arrival_datetime, '') AS arrival_datetime,
    COALESCE(e_.departure_datetime, '') AS departure_datetime,
    COALESCE(e_.arrival_mode_code, '') AS arrival_mode_code,
    COALESCE(e_.arrival_mode_text, '') AS arrival_mode_text,
    COALESCE(e_.departure_code, '') AS departure_code,
    COALESCE(e_.departure_text, '') AS departure_text,
    COALESCE(e_.complaint_code, '') AS complaint_code,
    COALESCE(e_.complaint_text, '') AS complaint_text
FROM hic_covid.emergency e_
JOIN hic_covid.demographics d
    ON d.uhl_system_number = e_.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
