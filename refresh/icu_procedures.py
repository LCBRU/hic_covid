from database import hic_conn, uhl_dwh_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.icu_procedures', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.icu_procedures;
		END;
'''

SQL_CREATE_TEMP_TABLE = '''
    SET NOCOUNT ON;

    SELECT
        REPLACE(REPLACE(t.ID, ' ', ''), ':', '') ID,
        t.to_ward
    INTO DWBRICCS.dbo.temp_hic_transfers
    FROM DWREPO.dbo.PATIENT p
    JOIN DWREPO.dbo.TRANSFERS t
        ON t.patient_id = p.id
        AND t.TRANSFER_DATE_TIME > '2020-01-01'
    WHERE p.SYSTEM_NUMBER IN (
        SELECT UHL_System_Number
        FROM DWBRICCS.dbo.all_suspected_covid
    );

    CREATE INDEX temp_hic_transfers_id_IDX ON DWBRICCS.dbo.temp_hic_transfers (id);
'''

SQL_CLEAN_UP = '''
	IF OBJECT_ID(N'dwbriccs.dbo.temp_hic_transfers', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dwbriccs.dbo.temp_hic_transfers;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.icu_procedures
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            p.SYSTEM_NUMBER AS uhl_system_number,
            ccp.id AS icu_encounter_identifier,
            proc_.PROCEDURE_DATE AS procedure_datetime,
            proc_.PROCEDURE_CODE AS procedure_type,
            opcs.PROCEDURE_DESCRIPTION AS procedure_desc,
            t.to_ward AS icu_ward
        FROM DWREPO.dbo.PATIENT p
        JOIN DWREPO.dbo.ADMISSIONS a
            ON a.PATIENT_ID = p.ID
        JOIN DWREPO.dbo.CONSULTANT_EPISODES ce
            ON ce.ADMISSIONS_ID = a.ID
        JOIN DWREPO.dbo.PROCEDURES proc_
            ON proc_.CONSULTANT_EPISODES_ID = ce.ID
        LEFT JOIN DWREPO.dbo.MF_OPCS4 opcs
            ON opcs.PROCEDURE_CODE = proc_.PROCEDURE_CODE
            AND opcs.LOGICALLY_DELETED_FLAG = 0
        JOIN DWREPO.dbo.WHO_INQUIRE_CRITICAL_CARE_PERIODS ccp
            ON ccp.ID = proc_.NONUNIQUE_ID
        LEFT JOIN DWBRICCS.dbo.temp_hic_transfers t
            ON t.ID = ccp.ID
        WHERE p.SYSTEM_NUMBER IN (
            SELECT UHL_System_Number
            FROM DWBRICCS.dbo.all_suspected_covid
        ) AND a.ADMISSION_DATE_TIME > '2020-01-01'
        AND proc_.NONUNIQUE_ID IN (
            SELECT ID FROM DWREPO.dbo.WHO_INQUIRE_CRITICAL_CARE_PERIODS
        )
        ;
    ");

    SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE icu_procedures ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX icu_procedures_uhl_system_number_IDX ON icu_procedures (uhl_system_number);
'''


def refresh_icu_procedures():
    print('refresh_icu_procedures: started')

    print('refresh_icu_procedures: extracting transfers')

    with uhl_dwh_conn() as con:
        con.execute(SQL_CLEAN_UP)
        con.execute(SQL_CREATE_TEMP_TABLE)

    print('refresh_icu_procedures: extracting')

    with hic_conn() as con:
        con.execute(SQL_DROP_TABLE)
        con.execute(SQL_INSERT)
        con.execute(SQL_ALTER_TABLE)
        con.execute(SQL_INDEXES)

    print('refresh_icu_procedures: cleaning up')

    with uhl_dwh_conn() as con:
        con.execute(SQL_CLEAN_UP)

    print('refresh_icu_procedures: ended')


# ICU Procedures

# brc_cv_covid_icu_procedures	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_icu_procedures	icu_encounter_identifier	ICU encounter identifier - link to other encounter data 
# brc_cv_covid_icu_procedures	procedure_datetime	date/time of procedure
# brc_cv_covid_icu_procedures	procedure_type	Type of procedure
# brc_cv_covid_icu_procedures	procedure_desc	Procedure description
# brc_cv_covid_icu_procedures	icu_ward	ICU ward
# brc_cv_covid_icu_procedures	brc_name	data submitting brc name


# Questions
# 1. Only have procedure date, not time.

SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.icu_encounter_identifier,
        a.admission_ward,
        a.wardstaydate,
        a.hospital_admission_datetime,
        a.hospital_discharge_datetime,
        a.level_2,
        a.level_3,
        a.basic_cardiovascular_support_day,
        a.advanced_cardiovascular_support_day,
        a.basic_respiratory_support_day,
        a.advanced_respiratory_support_day,
        a.renal_support_day,
        a.neurological_support_day,
        a.dermatological_support_day,
        a.liver_support_day,
        a.gastrointestinal_support_day
    FROM icu_organsupport a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
    ;
'''

def export_icu_procedures():
	export('icu_procedures', SQL_SELECT_EXPORT)
