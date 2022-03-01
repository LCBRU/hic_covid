CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.prescribing_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    p.order_id AS order_id,
    p.method_name AS method_name,
    p.medication_name AS medication_name,
    p.min_dose AS min_dose,
    p.max_does AS max_does,
    p.does_units AS does_units,
    p.form AS form,
    p.frequency AS frequency,
    p.route AS route,
    p.ordered_datetime AS ordered_datetime
FROM hic_covid.prescribing p
JOIN hic_covid.demographics d
    ON d.uhl_system_number = p.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
