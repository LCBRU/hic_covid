CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.observations_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    o.observation_datetime AS observation_datetime,
    o.observation_name AS observation_name,
    o.observation_value AS observation_value,
    o.observation_units AS observation_units,
    o.observation_ews AS observation_ews
FROM hic_covid.observation o
JOIN hic_covid.demographics d
    ON d.uhl_system_number = o.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
