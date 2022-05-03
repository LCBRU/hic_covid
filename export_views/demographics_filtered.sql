CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.demographics_filtered AS
SELECT *
FROM
    hic_covid.demographics d
WHERE d.uhl_system_number IN (
        SELECT DISTINCT e_.uhl_system_number
        FROM hic_covid.episode e_
        WHERE e_.admission_datetime <= STR_TO_DATE('20210630','%Y%m%d')
    )