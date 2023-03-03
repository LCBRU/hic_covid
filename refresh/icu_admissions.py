from database import hic_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.icu_admissions', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.icu_admissions;
		END;
'''

SQL_INSERT = '''
    SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.icu_admissions
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            p.SYSTEM_NUMBER AS uhl_system_number,
            ccp.ID AS icu_encounter_identifier,
            a.ADMISSION_DATE_TIME AS hospital_admission_datetime,
            a.DISCHARGE_DATE_TIME AS hospital_discharge_datetime,
            ccp.CCP_START_DATE_TIME AS icu_admission_datetime,
            ccp.CCP_END_DATE_TIME AS icu_discharge_datetime,
            ward.ward AS admission_ward
        FROM DWREPO.dbo.WHO_INQUIRE_CRITICAL_CARE_PERIODS ccp
        JOIN DWREPO.dbo.PATIENT p
            ON p.ID = ccp.PATIENT_ID
        JOIN DWREPO.dbo.ADMISSIONS a
            ON a.ID = ccp.ADMISSIONS_ID
        LEFT JOIN DWREPO.dbo.MF_WARD ward
            ON ward.CODE = a.ward_code
            AND ward.LOGICALLY_DELETED_FLAG = 0
        WHERE ccp.CCP_START_DATE >= '01 Jan 2020'
            AND p.SYSTEM_NUMBER IN (
                SELECT UHL_System_Number
                FROM DWBRICCS.dbo.all_suspected_covid
            ) AND ccp.CCP_START_DATE_TIME >= '01 Jan 2020'
        ;

	");

    SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE icu_admissions ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX icu_admissions_uhl_system_number_IDX ON icu_admissions (uhl_system_number);
'''


def refresh_icu_admissions():
	print('refresh_icu_admissions: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_icu_admissions: ended')


# ICU admissions

# brc_cv_covid_icu_admission	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_icu_admission	icu_encounter_identifier	ICU encounter identifier - link to other encounter data 
# brc_cv_covid_icu_admission	hospital_admission_datetime	DateTime admitted to hospital 
# brc_cv_covid_icu_admission	hospital_discharge_datetime	DateTime discharged from hospital
# brc_cv_covid_icu_admission	icu_admission_datetime	DateTime admitted to ICU 
# brc_cv_covid_icu_admission	icu_discharge_datetime	DateTime discharged from ICU
# brc_cv_covid_icu_admission	admission_ward	name of the ward
# brc_cv_covid_icu_admission	brc_name	data submitting brc name


SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.icu_encounter_identifier,
        a.hospital_admission_datetime,
        a.hospital_discharge_datetime,
        a.icu_admission_datetime,
        a.icu_discharge_datetime,
        a.admission_ward
    FROM icu_admissions a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
            AND COALESCE(a.hospital_admission_datetime, datefromparts(2021, 06, 30)) <=  datefromparts(2021, 06, 30)
            AND COALESCE(a.hospital_discharge_datetime, datefromparts(2021, 06, 30)) <=  datefromparts(2021, 06, 30)
            AND COALESCE(a.icu_admission_datetime, datefromparts(2021, 06, 30)) <=  datefromparts(2021, 06, 30)
            AND COALESCE(a.icu_discharge_datetime, datefromparts(2021, 06, 30)) <=  datefromparts(2021, 06, 30)
    ;
'''

def export_icu_admissions():
	export('icu_admissions', SQL_SELECT_EXPORT)
