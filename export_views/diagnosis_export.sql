CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.diagnosis_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    diag.spell_id AS spell_id,
    diag.episode_id AS episode_id,
    diag.diagnosis_number AS diagnosis_number,
    diag.diagnosis_code AS diagnosis_code,
    diag.diagnosis_name AS diagnosis_name
FROM hic_covid.diagnosis diag
JOIN hic_covid.demographics d
    ON d.uhl_system_number = diag.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
