from database import hic_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.procedures', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.procedures;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.procedures
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            p.SYSTEM_NUMBER AS uhl_system_number,
            a.id AS spell_identifier,
            ce.ID AS episode_identifier,
            proc.procedure_date AS procedure_date,
            proc_.PROCEDURE_NUMBER AS procedure_position,
            proc_.PROCEDURE_CODE AS procedure_code_opcs,
            opcs.PROCEDURE_DESCRIPTION AS procedure_name_opcs,
            NULL AS procedure_code_snomed,
            NULL AS procedure_name_snomed
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
        WHERE p.SYSTEM_NUMBER IN (
            SELECT UHL_System_Number
            FROM DWBRICCS.dbo.all_suspected_covid
        ) AND a.ADMISSION_DATE_TIME > '2020-01-01'
        ;
	");

	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE procedures ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX procedures_uhl_system_number_IDX ON procedures (uhl_system_number);
'''


def refresh_procedures():
	print('refresh_procedures: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_procedures: ended')


# brc_cv_covid_episodes_procedures	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_episodes_procedures	spell_identifier	patient unique inpatient spell identifier
# brc_cv_covid_episodes_procedures	episode_identifier	patient unique inpatient episode identifier
# brc_cv_covid_episodes_procedures	procedure_position	inpatient procedure code sequence
# brc_cv_covid_episodes_procedures	procedure_code_opcs	inpatient procedure code opcs
# brc_cv_covid_episodes_procedures	procedure_name_opcs	inpatient procedure description opcs
# brc_cv_covid_episodes_procedures	procedure_code_snomed	inpatient procedure code snomed ct
# brc_cv_covid_episodes_procedures	procedure_name_snomed	inpatient procedure description snomed ct
# brc_cv_covid_episodes_procedures	brc_name	data submitting brc name

# Questions
# 1. Do not have SNOMED CT coding

SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.spell_identifier,
        a.episode_identifier,
        a.procedure_position,
        a.procedure_code_opcs,
        a.procedure_name_opcs,
        a.procedure_code_snomed,
        a.procedure_name_snomed
    FROM procedures a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
            AND COALESCE(a.procedure_date, datefromparts(2021, 06, 30)) <=  datefromparts(2021, 06, 30)
    ;
'''

def export_procedures():
	export('procedures', SQL_SELECT_EXPORT)
