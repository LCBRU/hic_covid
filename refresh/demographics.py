import requests
from urllib.parse import urlencode, urlparse, urlunparse
from database import hic_conn, uhl_dwh_conn
from environment import IDENTITY_API_KEY, IDENTITY_HOST
from refresh import export

SQL_CREATE_TEMP_TABLE = '''
    SET NOCOUNT ON;

	SELECT DISTINCT
        p.SYSTEM_NUMBER AS uhl_system_number,
        LAST_VALUE(d.WHO_DIAGNOSIS_CODE)
			OVER(
				PARTITION BY p.SYSTEM_NUMBER
				ORDER BY d.DIAGNOSIS_DATE, d.DIAGNOSIS_NUMBER, d.WHO_DIAGNOSIS_CODE
				ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
			) recent_smoking_status
	INTO temp_hic_smoking
    FROM DWREPO.dbo.PATIENT p
    JOIN DWREPO.dbo.ADMISSIONS a
        ON a.PATIENT_ID = p.ID
    JOIN DWREPO.dbo.CONSULTANT_EPISODES ce
        ON ce.ADMISSIONS_ID = a.ID
    JOIN DWREPO.dbo.DIAGNOSES d
        ON d.CONSULTANT_EPISODES_ID = ce.ID
    WHERE p.SYSTEM_NUMBER IN (
        SELECT UHL_System_Number
        FROM DWBRICCS.dbo.all_suspected_covid
    ) AND a.ADMISSION_DATE_TIME > '2020-01-01'
    AND d.WHO_DIAGNOSIS_CODE IN ('F17', 'F17.0', 'F17.1', 'F17.2', 'F17.3', 'F17.4', 'F17.7', 'F17.9', 'T65.2', 'Z72.0')
	AND p.SYSTEM_NUMBER IS NOT NULL
    ;

	CREATE INDEX temp_hic_smoking_uhl_system_number on temp_hic_smoking (uhl_system_number);

'''

SQL_CLEAN_UP = '''
	IF OBJECT_ID(N'temp_hic_smoking', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dwbriccs.dbo.temp_hic_smoking;
		END;
'''


SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.demographics', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.demographics;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.demographics
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            cv.uhl_system_number AS uhl_system_number,
            CASE p.Sex
                WHEN 'U' THEN '0'
                WHEN 'M' THEN '1'
                WHEN 'F' THEN '2'
                ELSE '9'
            END gender,
            CASE p.Sex
                WHEN 'U' THEN 'Not Known'
                WHEN 'M' THEN 'Male'
                WHEN 'F' THEN 'Female'
                ELSE 'Not Specified'
            END gender_desc,
            DWBRICCS.dbo.GetAgeAtDate(MIN(p.PATIENT_DATE_OF_BIRTH), MIN(cv.dateadded)) AS age,
            p.DATE_OF_DEATH AS death_date,
            p.ETHNIC_ORIGIN_CODE ethnicity,
            eo.ethnic_origin AS ethnicity_desc,
			s.recent_smoking_status AS Smoking_status,
			NULL AS BMI,
            p.Post_Code AS postcode,
            pc.IMD_DECILE AS IMD_Decile
        FROM DWBRICCS.dbo.all_suspected_covid cv
        JOIN DWREPO.dbo.PATIENT p
            ON p.SYSTEM_NUMBER = cv.uhl_system_number
		LEFT JOIN dwbriccs.dbo.temp_hic_smoking s
			ON s.uhl_system_number = cv.uhl_system_number
        LEFT JOIN DWREPO_BASE.dbo.MF_POSTCODE_REFERENCE_WHO pc
            ON pc.UNIT_POSTCODE = p.Post_Code
        LEFT JOIN DWREPO.dbo.MF_ETHNIC_ORIGIN eo
            ON eo.code = p.ETHNIC_ORIGIN_CODE
        WHERE cv.uhl_system_number IS NOT NULL
        GROUP BY
            cv.uhl_system_number,
            p.Sex,
            p.patient_date_of_birth,
            p.DATE_OF_DEATH,
            p.ETHNIC_ORIGIN_CODE,
            eo.ethnic_origin,
            p.Post_Code,
            pc.IMD_DECILE,
			s.recent_smoking_status
        ;
	");
	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE demographics ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	ALTER TABLE demographics ADD CONSTRAINT demographics_PK PRIMARY KEY (uhl_system_number);
'''

SQL_SELECT_MISSING_PARTICIPANT_IDS = '''
	SELECT uhl_system_number
	FROM demographics d
	WHERE uhl_system_number NOT IN (
		SELECT uhl_system_number
		FROM participant p
	)
	;
'''

def refresh_demographics():
	print('refresh_demographics: started')

	print('refresh_demographics: extract smoking status')

	with uhl_dwh_conn() as con:
		con.execute(SQL_CLEAN_UP)
		con.execute(SQL_CREATE_TEMP_TABLE)

	print('refresh_demographics: extract demographics')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

		cur = con.cursor()
		
		new_participants = list(cur.execute(SQL_SELECT_MISSING_PARTICIPANT_IDS).fetchall())

		if len(new_participants) > 0:
			ids = _allocate_ids(len(new_participants))

			cur.executemany(
				'INSERT INTO participant (participant_identifier, uhl_system_number) VALUES (?, ?)',
				[(id, uhls[0]) for id, uhls in zip(ids, new_participants)],
			)

			cur.commit()

	with uhl_dwh_conn() as con:
		con.execute(SQL_CLEAN_UP)

	print('refresh_demographics: ended')


def _allocate_ids(participant_count):
	url_parts = urlparse(IDENTITY_HOST)
	url_parts = url_parts._replace(
			query=urlencode({'api_key': IDENTITY_API_KEY}),
			path='api/create_pseudorandom_ids',
		)
	url = urlunparse(url_parts)

	# Do not verify locally signed certificate
	return requests.post(
			url,
			json={'prefix': 'HCVPt', 'id_count': participant_count},
			verify=False,
		).json()['ids']


# brc_cv_covid_demographics	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_demographics	gender	patient gender code	Enumerator
	# 0	Not Known
	# 1	Male
	# 2	Female
	# 9	Not Specified
# brc_cv_covid_demographics	gender_desc	patient gender description	Enumerator
# brc_cv_covid_demographics	age	patient age in years at time of COVID-19 test order	
# brc_cv_covid_demographics	death_date	Date of in-hospital death – or death updated from SPINE if record has been opened on Cerner post discharge	
# brc_cv_covid_demographics	ethnicity	patient ethnicity code	Enumerator
	# 99	Not known
	# A	British
	# B	Irish
	# C	Any other White background
	# D	White and Black Caribbeans
	# E	White and Black African
	# F	White and Asian
	# G	Any other mixed background
	# H	Indian
	# J	Pakistani
	# K	Bangladeshi
	# L	Any other Asian background
	# M	Caribbean
	# N	African
	# P	Any other Black background
	# R	Chinese
	# S	Any other ethnic group
	# Z	Not stated
# brc_cv_covid_demographics	ethnicity_desc	patient ethnicity description	Enumerator
# brc_cv_covid_demographics	Smoking_status	patient current smoking status
# brc_cv_covid_demographics	BMI	patient BMI
# brc_cv_covid_demographics	postcode	patient postcode	
# brc_cv_covid_demographics	IMD Decile	Decile of Index of Multiple Deprivation Score	
# brc_cv_covid_demographics	brc_name	data submitting brc name	

# Add
# 
# 1. Smoking status
# 2. BMI

# Questions
#
# 1. ICD10 Code mapping for smoking status
# 2. Can only find BMI for some ED patients

SQL_SELECT_EXPORT = '''
	SELECT
		p.participant_identifier AS Subject,
		d.gender,
		d.gender_desc,
		d.age,
		d.death_date,
		d.ethnicity,
		d.ethnicity_desc,
		d.Smoking_status,
		d.BMI,
		SUBSTRING(COALESCE(d.postcode, ' '), 1, CHARINDEX(' ', COALESCE(d.postcode, ' '))) AS postcode,
		d.IMD_Decile
	FROM demographics d
	JOIN participant p
		ON p.uhl_system_number = d.uhl_system_number
	WHERE   CHARINDEX(' ', COALESCE(d.postcode, '')) < 5
			AND d.uhl_system_number IN (
				SELECT  DISTINCT e_.uhl_system_number
				FROM    episodes e_
				WHERE   e_.admission_date_time <= '20210630'
			)
	;
'''

def export_demographics():
	export('demographics', SQL_SELECT_EXPORT)
