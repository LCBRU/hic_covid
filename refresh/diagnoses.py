from database import hic_conn
from refresh import export

# brc_cv_covid_episodes_diagnosis	subject	anonymised/pseudonymised patient identifier	
# brc_cv_covid_episodes_diagnosis	spell_identifier	patient unique inpatient spell identifier	
# brc_cv_covid_episodes_diagnosis	episode_identifier	patient unique inpatient episode identifier	
# brc_cv_covid_episodes_diagnosis	diagnosis_date_time	date/time of diagnosis	
# brc_cv_covid_episodes_diagnosis	diagnosis_position	inpatient diagnosis sequence	
# brc_cv_covid_episodes_diagnosis	diagnosis_code_icd	inpatient diagnosis code icd	
# brc_cv_covid_episodes_diagnosis	diagnosis_description_icd	inpatient diagnosis description icd	
# brc_cv_covid_episodes_diagnosis	diagnosis_code_snomed	inpatient diagnosis code snomed	
# brc_cv_covid_episodes_diagnosis	diagnosis_description_snomed	inpatient diagnosis description snomed	
# brc_cv_covid_episodes_diagnosis	brc_name	data submitting brc name	

# Questions
# 1. Don't have mapping to SNOMED Code

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.diagnosis', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.diagnosis;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.diagnosis
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT DISTINCT
            p.SYSTEM_NUMBER AS uhl_system_number,
            a.id AS spell_identifier,
            ce.ID AS episode_identifier,
            d.row_created AS diagnosis_date_time,
            d.DIAGNOSIS_NUMBER AS diagnosis_position,
            d.DIAGNOSIS_CODE AS diagnosis_code_icd,
            mf_d.DIAGNOSIS_DESCRIPTION AS diagnosis_description_icd,
            NULL AS diagnosis_code_snomed,
            NULL AS diagnosis_description_snomed
        FROM DWREPO.dbo.PATIENT p
        JOIN DWREPO.dbo.ADMISSIONS a
            ON a.PATIENT_ID = p.ID
        JOIN DWREPO.dbo.CONSULTANT_EPISODES ce
            ON ce.ADMISSIONS_ID = a.ID
        JOIN DWREPO.dbo.DIAGNOSES d
            ON d.CONSULTANT_EPISODES_ID = ce.ID
        LEFT JOIN DWREPO.dbo.MF_DIAGNOSIS mf_d
            ON mf_d.DIAGNOSIS_CODE = d.DIAGNOSIS_CODE
            AND mf_d.LOGICALLY_DELETED_FLAG = 0
        WHERE p.SYSTEM_NUMBER IN (
            SELECT UHL_System_Number
            FROM DWBRICCS.dbo.all_suspected_covid
        ) AND a.ADMISSION_DATE_TIME > '2020-01-01'
        ;
	");
	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE diagnosis ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX diagnosis_uhl_system_number_IDX ON diagnosis (uhl_system_number);
'''


def refresh_diagnosis():
	print('refresh_diagnosis: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_diagnosis: ended')

# brc_cv_covid_episodes_diagnosis	subject	anonymised/pseudonymised patient identifier	
# brc_cv_covid_episodes_diagnosis	spell_identifier	patient unique inpatient spell identifier	
# brc_cv_covid_episodes_diagnosis	episode_identifier	patient unique inpatient episode identifier	
# brc_cv_covid_episodes_diagnosis	diagnosis_date_time	date/time of diagnosis	
# brc_cv_covid_episodes_diagnosis	diagnosis_position	inpatient diagnosis sequence	
# brc_cv_covid_episodes_diagnosis	diagnosis_code_icd	inpatient diagnosis code icd	
# brc_cv_covid_episodes_diagnosis	diagnosis_description_icd	inpatient diagnosis description icd	
# brc_cv_covid_episodes_diagnosis	diagnosis_code_snomed	inpatient diagnosis code snomed	
# brc_cv_covid_episodes_diagnosis	diagnosis_description_snomed	inpatient diagnosis description snomed	
# brc_cv_covid_episodes_diagnosis	brc_name	data submitting brc name	

# Questions
# 1. Don't have mapping to SNOMED Code


SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.spell_identifier,
        a.episode_identifier,
        a.diagnosis_date_time,
        a.diagnosis_position,
        a.diagnosis_code_icd,
        a.diagnosis_description_icd,
        a.diagnosis_code_snomed,
        a.diagnosis_description_snomed
    FROM diagnosis a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
    ;
'''

def export_diagnoses():
	export('diagnoses', SQL_SELECT_EXPORT)
