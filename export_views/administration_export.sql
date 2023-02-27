CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.administration_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    a.administration_datetime AS administration_datetime,
    a.medication_name AS medication_name,
    a.dose AS dose,
    a.dose_unit AS dose_unit,
    a.route_name AS route_name,
    a.form_name AS form_name
FROM wh_hic_covid.administration a
JOIN wh_hic_covid.demographics d
    ON d.uhl_system_number = a.uhl_system_number
JOIN wh_hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
