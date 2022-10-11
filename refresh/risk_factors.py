# Risk Factors

# brc_cv_covid_riskfactors	subject	anonymised/pseudonymised patient identifier	
# brc_cv_covid_riskfactors	icd_code	icd10 code	
# brc_cv_covid_riskfactors	snomed_code	snomed ct code	
# brc_cv_covid_riskfactors	code_description	icd/snomed descriptions	
# brc_cv_covid_riskfactors	risk_factor_date_time	date/time of risk factor	
# brc_cv_covid_riskfactors	risk_factor_name	Risk Factor name	Enumerator
# brc_cv_covid_riskfactors	brc_name	data submitting brc name	

# Questions
#
# What are the ICD 10 codes?

from itertools import groupby
from database import hic_conn, uhl_dwh_conn
from refresh import export
import csv


SQL_CREATE_TEMP_TABLE = '''
    SET NOCOUNT ON;

    CREATE TABLE dwbriccs.dbo.temp_hic_riskfactors (
        icd10 VARCHAR(50) PRIMARY KEY,
        description VARCHAR(500)
    );
'''

SQL_CLEAN_UP = '''
	IF OBJECT_ID(N'dwbriccs.dbo.temp_hic_riskfactors', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dwbriccs.dbo.temp_hic_riskfactors;
		END;
'''


SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.riskfactors', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.riskfactors;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.riskfactors
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            p.SYSTEM_NUMBER AS uhl_system_number,
            d.WHO_DIAGNOSIS_CODE AS icd_code,
            NULL AS snomed_code,
            i.DIAGNOSIS_DESCRIPTION AS code_description,
            CONVERT(DATETIME, d.DIAGNOSIS_DATE, 112) AS risk_factor_date_time,
            hrf.description AS risk_factor_name
        FROM DWREPO.dbo.PATIENT p
        JOIN DWREPO.dbo.ADMISSIONS a
            ON a.PATIENT_ID = p.ID
        JOIN DWREPO.dbo.CONSULTANT_EPISODES ce
            ON ce.ADMISSIONS_ID = a.ID
        JOIN DWREPO.dbo.DIAGNOSES d
            ON d.CONSULTANT_EPISODES_ID = ce.ID
        JOIN dwbriccs.dbo.temp_hic_riskfactors hrf
            ON hrf.icd10 = d.WHO_DIAGNOSIS_CODE
        LEFT JOIN DWREPO.dbo.MF_DIAGNOSIS i
            ON i.DIAGNOSIS_CODE = d.WHO_DIAGNOSIS_CODE
            AND i.LOGICALLY_DELETED_FLAG = 0
        WHERE p.SYSTEM_NUMBER IN (
            SELECT UHL_System_Number
            FROM DWBRICCS.dbo.all_suspected_covid
        ) AND a.ADMISSION_DATE_TIME > '2020-01-01'
        ;
	");
	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE riskfactors ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX riskfactors_uhl_system_number_IDX ON riskfactors (uhl_system_number);
'''


def refresh_riskfactors():
    print('refresh_riskfactors: started')

    print('refresh_riskfactors: loading codes')

    with open("refresh/risk_factors.csv") as csvfile, uhl_dwh_conn() as con:
        con.execute(SQL_CLEAN_UP)
        con.execute(SQL_CREATE_TEMP_TABLE)
        reader = csv.DictReader(csvfile)
        for code, rows in groupby(reader, lambda x: x['ICD10']):
            con.execute("insert into temp_hic_riskfactors(icd10, description) values (?, ?)", code, ', '.join([r['Risk_Factor'] for r in rows]))
        con.commit()

    print('refresh_riskfactors: extracting')

    with hic_conn() as con:
        con.execute(SQL_DROP_TABLE)
        con.execute(SQL_INSERT)
        con.execute(SQL_ALTER_TABLE)
        con.execute(SQL_INDEXES)

    print('refresh_riskfactors: cleaning up')

    with uhl_dwh_conn() as con:
        con.execute(SQL_CLEAN_UP)

    print('refresh_riskfactors: ended')


# brc_cv_covid_riskfactors	subject	anonymised/pseudonymised patient identifier	
# brc_cv_covid_riskfactors	icd_code	icd10 code	
# brc_cv_covid_riskfactors	snomed_code	snomed ct code	
# brc_cv_covid_riskfactors	code_description	icd/snomed descriptions	
# brc_cv_covid_riskfactors	risk_factor_date_time	date/time of risk factor	
# brc_cv_covid_riskfactors	risk_factor_name	Risk Factor name	Enumerator
# brc_cv_covid_riskfactors	brc_name	data submitting brc name	

# Questions
#
# What are the ICD 10 codes?


SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.icd_code,
        a.snomed_code,
        a.code_description,
        a.risk_factor_date_time,
        a.risk_factor_name
    FROM riskfactors a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
    ;
'''

def export_riskfactors():
	export('riskfactors', SQL_SELECT_EXPORT)
