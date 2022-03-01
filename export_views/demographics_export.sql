CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.demographics_exports AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    COALESCE(d.gp_practice, '') AS gp_practice,
    COALESCE(d.age, '') AS age,
    COALESCE(d.date_of_death, '') AS date_of_death,
    substring_index(COALESCE(d.postcode, ''), ' ', 1) AS postcode_outcode,
    COALESCE(d.sex, '') AS gender,
    COALESCE(d.ethnic_category, '') AS ethnic_category
FROM
    hic_covid.demographics d
WHERE
    (
        LENGTH(substring_index(COALESCE(d.postcode, ''), ' ', 1)) < 5
    ) AND  d.uhl_system_number IN (
        SELECT DISTINCT e_.uhl_system_number
        FROM hic_covid.episode e_
        WHERE e_.admission_datetime <= '20210630'
    )