from database import hic_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.emergency', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.emergency;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.emergency
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            fpp.PP_IDENTIFIER AS UHL_System_Number,
            fpp.visitid AS ae_visit_id,
            DATEADD(
                MINUTE,
                CONVERT(INT, SUBSTRING(fpp.ARRIVAL_TIME, 3, 2)),
                DATEADD(
                    HOUR,
                    CONVERT(INT, SUBSTRING(fpp.ARRIVAL_TIME, 1, 2)),
                    fpp.ARRIVAL_DATE
                )) AS arrival_date_time,
            fpp.PP_ARRIVAL_TRANS_MODE_CODE AS arrival_mode,
            fpp.PP_ARRIVAL_TRANS_MODE_NAME AS arrival_mode_desc,
            DATEADD(
                MINUTE,
                CONVERT(INT, SUBSTRING(fpp.DISCHARGE_TIME, 3, 2)),
                DATEADD(
                    HOUR,
                    CONVERT(INT, SUBSTRING(fpp.DISCHARGE_TIME, 1, 2)),
                    fpp.DISCHARGE_DATE
                )) AS departure_date_time,
            fpp.PP_DEP_DEST_ID AS discharge_destination,
            fpp.PP_DEP_DEST AS discharge_destination_desc,
            fpp.PP_PRESENTING_PROBLEM_CODE AS complaint
        FROM DWNERVECENTRE.dbo.F_PAT_PRESENT fpp
        WHERE fpp.PP_IDENTIFIER in (
            SELECT asc2.UHL_System_Number
            FROM DWBRICCS.dbo.all_suspected_covid asc2
        ) AND fpp.ARRIVAL_DATE >= '2020-01-01'
        ;
	");
	
	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE emergency ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX emergency_uhl_system_number_IDX ON emergency (uhl_system_number);
'''


def refresh_emergency():
	print('refresh_emergency: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_emergency: ended')


# brc_cv_covid_emergency	subject	anonymised/pseudonymised patient identifier	
# brc_cv_covid_emergency	ae_visit_id	anonymised/pseudonymised A&E encounter	
# brc_cv_covid_emergency	arrival_date_time	Date/time when the patient arrived in A&E	
# brc_cv_covid_emergency	arrival_mode	patient mode of arrival to a&e code	Enumerator
# 1	Brought in by ambulance (including helicopter/'Air Ambulance')
# 2	Other
# brc_cv_covid_emergency	arrival_mode_desc	patient mode of arrival to a&e description	Enumerator
# brc_cv_covid_emergency	departure_date_time	Date/time when the patient departed from A&E	
# brc_cv_covid_emergency	discharge_destination	patient discharge location from a&e code	Enumerator
# brc_cv_covid_emergency	discharge_destination_desc	patient discharge location from a&e description	Enumerator
# brc_cv_covid_emergency	complaint	SNOMED CT coded chief/other complaint/symptom data collected; comma delimited	
# brc_cv_covid_emergency	brc_name	data submitting brc name	

SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.ae_visit_id,
        a.arrival_date_time,
        a.arrival_mode,
        arrival_mode_desc,
        departure_date_time,
        discharge_destination,
        discharge_destination_desc,
        complaint
    FROM emergency a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
            AND a.arrival_date_time <= '20210630'
            AND a.departure_date_time <= '20210630'
    ;
'''

def export_emergency():
	export('emergency', SQL_SELECT_EXPORT)
