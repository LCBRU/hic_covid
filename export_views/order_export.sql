CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.order_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    o.request_datetime AS request_datetime,
    o.examination_code AS examination_code,
    o.examination_description AS examination_description,
    o.modality AS modality,
    o.snomed_code AS snomed_code
FROM hic_covid.order o
JOIN hic_covid.demographics d
    ON d.uhl_system_number = o.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
