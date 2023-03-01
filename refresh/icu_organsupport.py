from database import hic_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.icu_organsupport', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.icu_organsupport;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.icu_organsupport
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            p.SYSTEM_NUMBER AS uhl_system_number,
            ccp.ID AS icu_encounter_identifier,
            ward.ward AS admission_ward,
            ccp.CCP_START_DATE_TIME AS wardstaydate,
            a.ADMISSION_DATE_TIME AS hospital_admission_datetime,
            a.DISCHARGE_DATE_TIME AS hospital_discharge_datetime,
            ccp.CRITICAL_CARE_LEVEL2_DAYS AS level_2,
            ccp.CRITICAL_CARE_LEVEL3_DAYS AS level_3,
            ccp.BASIC_CARDIO_LEVEL_DAYS AS basic_cardiovascular_support_day,
            ccp.ADVANCED_CARDIO_LEVEL_DAYS AS advanced_cardiovascular_support_day,
            ccp.BASIC_RESP_LEVEL_DAYS AS basic_respiratory_support_day,
            ccp.ADVANCED_RESP_LEVEL_DAYS AS advanced_respiratory_support_day,
            ccp.RENAL_SUPPORT_DAYS AS renal_support_day,
            ccp.NEURO_SUPPORT_DAYS AS neurological_support_day,
            ccp.DERM_SUPPORT_DAYS AS dermatological_support_day,
            ccp.LIVER_SUPPORT_DAYS AS liver_support_day,
            NULL AS gastrointestinal_support_day
        FROM DWREPO_BASE.dbo.WHO_INQUIRE_CRITICAL_CARE_PERIODS ccp
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
	ALTER TABLE icu_organsupport ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX icu_organsupport_uhl_system_number_IDX ON icu_organsupport (uhl_system_number);
'''


def refresh_icu_organsupport():
	print('refresh_icu_organsupport: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_icu_organsupport: ended')


# ICU Organ Support

# brc_cv_covid_icu_organsupport	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_icu_organsupport	icu_encounter_identifier	ICU encounter identifier - link to other encounter data 
# brc_cv_covid_icu_organsupport	admission_ward	name of the ward
# brc_cv_covid_icu_organsupport	wardstaydate	date of ward stay
# brc_cv_covid_icu_organsupport	hospital_admission_datetime	date/time of hospital admission
# brc_cv_covid_icu_organsupport	hospital_discharge_datetime	date/time of hospital discharge
# brc_cv_covid_icu_organsupport	level_2	level 2 care days
# brc_cv_covid_icu_organsupport	level_3	level 3 care days
# brc_cv_covid_icu_organsupport	basic_cardiovascular_support_day	BASIC CARDIOVASCULAR SUPPORT DAYS is the total number of days that the PATIENT received basic cardiovascular support during a CRITICAL CARE PERIOD.
# brc_cv_covid_icu_organsupport	advanced_cardiovascular_support_day	ADVANCED CARDIOVASCULAR SUPPORT DAYS is the total number of days that the PATIENT received advanced cardiovascular support during a CRITICAL CARE PERIOD
# brc_cv_covid_icu_organsupport	basic_respiratory_support_day	BASIC RESPIRATORY SUPPORT DAYS is the total number of days that the PATIENT received basic respiratory support during a CRITICAL CARE PERIOD.
# brc_cv_covid_icu_organsupport	advanced_respiratory_support_day	ADVANCED RESPIRATORY SUPPORT DAYS is the total number of days that the PATIENT received advanced respiratory support during a CRITICAL CARE PERIOD.
# brc_cv_covid_icu_organsupport	renal_support_day	RENAL SUPPORT DAYS is the total number of days that the PATIENT received renal system support during a CRITICAL CARE PERIOD.
# brc_cv_covid_icu_organsupport	neurological_support_day	NEUROLOGICAL SUPPORT DAYS is the total number of days that the PATIENT received neurological system support during a CRITICAL CARE PERIOD.
# brc_cv_covid_icu_organsupport	dermatological_support_day	The total number of days that the PATIENT received dermatological system support during a CRITICAL CARE PERIOD.
# brc_cv_covid_icu_organsupport	liver_support_day	LIVER SUPPORT DAYS is the total number of days that the PATIENT received liver support during a CRITICAL CARE PERIOD.
# brc_cv_covid_icu_organsupport	gastrointestinal_support_day	The total number of days that the PATIENT received gastro-intestinal system support during a CRITICAL CARE PERIOD.
# brc_cv_covid_icu_organsupport	brc_name	data submitting brc name

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
            AND a.hospital_admission_datetime <= '20210630'
            AND a.hospital_discharge_datetime <= '20210630'
    ;
'''

def export_icu_organsupport():
	export('icu_organsupport', SQL_SELECT_EXPORT)
