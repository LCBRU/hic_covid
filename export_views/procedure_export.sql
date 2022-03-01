CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.procedure_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    p.spell_id AS spell_id,
    p.episode_id AS episode_id,
    COALESCE(p.procedure_number, '') AS procedure_number,
    COALESCE(p.procedure_code, '') AS procedure_code,
    COALESCE(p.procedure_name, '') AS procedure_name
FROM hic_covid.procedure p
JOIN hic_covid.demographics d
    ON d.uhl_system_number = p.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
