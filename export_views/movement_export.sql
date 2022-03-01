CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW hic_covid.movement_export AS
SELECT
    'NIHR Leicester Biomedical Research Centre' AS BRC,
    'RWE' AS Organisation,
    d.participant_identifier AS Subject,
    COALESCE(t.spell_id, '') AS spell_id,
    COALESCE(t.transfer_datetime, '') AS start_datetime,
    COALESCE(t.transfer_type, '') AS start_type,
    COALESCE(t2.transfer_datetime, '') AS end_datetime,
    COALESCE(t2.transfer_type, '') AS end_type,
    COALESCE(t.hospital, '') AS hospital,
    COALESCE(t.ward_code, '') AS ward_code,
    COALESCE(t.ward_name, '') AS ward_name
FROM
    (
        (
            hic_covid.transfer t
        JOIN hic_covid.transfer t2 ON
            (
                (
                    (
                        t2.spell_id = t.spell_id
                    )
                        AND (
                            t2.transfer_datetime = (
                                SELECT
                                    min(t3.transfer_datetime)
                                FROM
                                    hic_covid.transfer t3
                                WHERE
                                    (
                                        (
                                            t3.spell_id = t.spell_id
                                        )
                                            AND (
                                                t3.transfer_datetime > t.transfer_datetime
                                            )
                                    )
                            )
                        )
                )
            )
        )
    )
JOIN hic_covid.demographics d
    ON d.uhl_system_number = t.uhl_system_number
JOIN hic_covid.demographics_exports de
    ON de.Subject = d.participant_identifier
ORDER BY
    d.participant_identifier,
    t.spell_id,
    t.transfer_datetime