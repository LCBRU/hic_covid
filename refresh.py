import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode, urlparse, urlunparse
from sqlalchemy.sql import text
from database import hic_covid_session, uhl_dwh_databases_engine
from database import hic_connection, hic_cursor
from environment import IDENTITY_API_KEY, IDENTITY_HOST
from model import Demographics


DEMOGRAPHICS_SELECT_SQL = '''
    SELECT
        replace(p.NHS_NUMBER,' ','') AS nhs_number,
        cv.uhl_system_number AS uhl_system_number,
        p.CURRENT_GP_PRACTICE AS gp_practice,
        DWBRICCS.[dbo].[GetAgeAtDate](MIN(p.PATIENT_DATE_OF_BIRTH), MIN(cv.dateadded)) AS age,
        p.PATIENT_DATE_OF_BIRTH AS date_of_birth,
        p.DATE_OF_DEATH AS date_of_death,
        p.Post_Code AS postcode,
        CASE p.Sex
            WHEN 'U' THEN '0'
            WHEN 'M' THEN '1'
            WHEN 'F' THEN '2'
            ELSE '9'
        END sex,
        p.ETHNIC_ORIGIN_CODE ethnic_category
    FROM DWBRICCS.dbo.all_suspected_covid cv
    LEFT JOIN [DWREPO].[dbo].[PATIENT] p
        ON p.SYSTEM_NUMBER = cv.uhl_system_number
    WHERE cv.uhl_system_number IS NOT NULL
    GROUP BY
        p.NHS_NUMBER,
        cv.uhl_system_number,
        p.CURRENT_GP_PRACTICE,
        p.patient_date_of_birth,
        p.DATE_OF_DEATH,
        p.Post_Code,
        p.Sex,
        p.ETHNIC_ORIGIN_CODE
	;
'''


def refresh_demographics():
	print('refresh_demographics')

	inserts = []
	updates = []


	with hic_covid_session() as session:
		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(DEMOGRAPHICS_SELECT_SQL)

			for row in rs:
				d = session.query(Demographics).filter_by(uhl_system_number=row['uhl_system_number']).one_or_none()

				if d is None:
					d = Demographics(uhl_system_number=row['uhl_system_number'])
					inserts.append(d)
				else:
					updates.append(d)

				d.nhs_number = row['nhs_number']
				d.gp_practice = row['gp_practice']
				d.age = row['age']
				d.date_of_birth = row['date_of_birth']
				d.date_of_death = row['date_of_death']
				d.postcode = row['postcode']
				d.sex=row['sex']
				d.ethnic_category=row['ethnic_category']

		if len(inserts) > 0:
			url_parts = urlparse(IDENTITY_HOST)
			url_parts = url_parts._replace(
				query=urlencode({'api_key': IDENTITY_API_KEY}),
				path='api/create_pseudorandom_ids',
			)
			url = urlunparse(url_parts)

			# Do not verify locally signed certificate
			ids = requests.post(url, json={'prefix': 'HCVPt', 'id_count': len(inserts)}, verify=False)

			for id, d in zip(ids.json()['ids'], inserts):
				d.participant_identifier = id

		session.add_all(inserts)
		session.add_all(updates)
		session.commit()


VIROLOGY_SELECT_SQL = text('''
	SELECT
		p.Hospital_Number AS uhl_system_number,
		t.id AS test_id,
		o.Lab_Ref_No AS laboratory_code,
		t.Order_code order_code,
		t.Order_Code_Expan order_name,
		t.Test_code test_code,
		tc.Test_Expansion test_name,
		org.Organism organism,
		CASE
			WHEN t.Test_code = 'VBIR' THEN LTRIM(RTRIM(REPLACE(q.Quantity_Description, '*', '')))
			ELSE t.Result_Expansion
		END test_result,
		r.WHO_COLLECTION_DATE_TIME sample_collected_date_time,
		r.WHO_RECEIVE_DATE_TIME sample_received_date_time,
		t.WHO_TEST_RESULTED_DATE_TIME sample_available_date_time,
		t.Current_Status order_status
	FROM DWPATH.dbo.MICRO_TESTS t
	INNER JOIN	DWPATH.dbo.MICRO_RESULTS_FILE AS r
		ON t.Micro_Results_File = r.ISRN
	INNER JOIN	DWPATH.dbo.ORDERS_FILE AS o
		ON r.Order_No = o.Order_Number
	INNER JOIN	DWPATH.dbo.REQUEST_PATIENT_DETAILS AS p
		ON o.D_Level_Pointer = p.Request_Patient_Details
	LEFT JOIN DWPATH.dbo.MICRO_ORGANISMS org
		ON org.Micro_Tests = t.Micro_Tests
	LEFT OUTER JOIN DWPATH.dbo.MF_TEST_CODES_MICRO_WHO tc
		ON t.Test_Code_Key=tc.Test_Codes_Row_ID
	LEFT OUTER JOIN DWPATH.dbo.MF_QUANTITY_CODES q
		ON org.Quantifier=q.APEX_ID
	LEFT OUTER JOIN DWPATH.dbo.REQUEST_SOURCE_DETAILS s
		ON o.C_Level_Pointer = s.Request_Source_Details
	WHERE
		(
				t.Test_code IN  ( 'VCOV', 'VCOV3', 'VCOV4', 'VCOV5' )
			OR (t.Test_code = 'VBIR'AND org.Organism  LIKE  '%CoV%')
		)
		AND r.WHO_COLLECTION_DATE_TIME >= '01/01/2020 00:0:0'
		AND p.Hospital_Number in (
			SELECT asc2.UHL_System_Number
			FROM DWBRICCS.dbo.all_suspected_covid asc2
		)
	;
''')


VIROLOGY_INSERT_SQL = '''
	INSERT INTO `virology` (
		uhl_system_number,
		test_id,
		laboratory_code,
		order_code,
		order_name,
		test_code,
		test_name,
		organism,
		test_result,
		sample_collected_date_time,
		sample_received_date_time,
		sample_available_date_time,
		order_status
	) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_virology():
	print('refresh_virology')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `virology`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(VIROLOGY_SELECT_SQL)
			cur.executemany(VIROLOGY_INSERT_SQL, (list(r) for r in rs))
			hconn.commit()


EMERGENCY_SELECT_SQL = text('''
	SELECT
		fpp.visitid,
		fpp.PP_IDENTIFIER,
		fpp.ARRIVAL_DATE,
		fpp.ARRIVAL_TIME,
		fpp.DISCHARGE_DATE,
		fpp.DISCHARGE_TIME,
		fpp.PP_ARRIVAL_TRANS_MODE_CODE,
		fpp.PP_ARRIVAL_TRANS_MODE_NAME,
		fpp.PP_PRESENTING_DIAGNOSIS,
		fpp.PP_PRESENTING_PROBLEM,
		fpp.PP_PRESENTING_PROBLEM_CODE,
		fpp.PP_PRESENTING_PROBLEM_NOTES,
		fpp.PP_DEP_DEST_ID,
		fpp.PP_DEP_DEST,
		fpp.PP_DEPARTURE_NATIONAL_CODE 
	FROM DWNERVECENTRE.dbo.F_PAT_PRESENT fpp
	WHERE fpp.PP_IDENTIFIER in (
		SELECT asc2.UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid asc2
	) AND fpp.ARRIVAL_DATE >= '2020-01-01'
	;
''')


EMERGENCY_INSERT_SQL = '''
	INSERT INTO `emergency` (
		visitid,
		uhl_system_number,
		arrival_datetime,
		departure_datetime,
		arrival_mode_code,
		arrival_mode_text,
		departure_code,
		departure_text,
		complaint_code,
		complaint_text
	) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_emergency():
	print('refresh_emergency')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `emergency`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(EMERGENCY_SELECT_SQL)
			cur.executemany(EMERGENCY_INSERT_SQL, ([
				row['visitid'],
				row['PP_IDENTIFIER'],
				_date_and_time(row['ARRIVAL_DATE'], row['ARRIVAL_TIME']),
				_date_and_time(row['DISCHARGE_DATE'], row['DISCHARGE_TIME']),
				row['PP_ARRIVAL_TRANS_MODE_CODE'],
				row['PP_ARRIVAL_TRANS_MODE_NAME'],
				row['PP_DEP_DEST_ID'],
				row['PP_DEP_DEST'],
				row['PP_PRESENTING_PROBLEM_CODE'],
				row['PP_PRESENTING_PROBLEM'],
			] for row in rs))

			hconn.commit()


	def _date_and_time(self, date, time):
		if date is None:
			return None

		if not time:
			return date
		
		return date + timedelta(hours=int(time[0:2]), minutes=int(time[2:4]))


EPISODE_SELECT_SQL = text('''
	SELECT
		ce.ID AS episode_id,
		a.id AS spell_id,
		p.SYSTEM_NUMBER AS uhl_system_number,
		a.ADMISSION_DATE_TIME AS admission_datetime,
		a.DISCHARGE_DATE_TIME AS discharge_datetime,
		ROW_NUMBER() OVER (
			PARTITION BY a.ID
			ORDER BY ce.CONS_EPISODE_START_DATE_TIME
		) AS order_no_of_episode,
		moa.NC_ADMISSION_METHOD AS admission_method_code,
		moa.NC_ADMISSION_METHOD_NAME AS admission_method_name,
		soa.NC_SOURCE_OF_ADMISSION AS admission_source_code,
		soa.NC_SOURCE_OF_ADMISSION_NAME AS admission_source_name,
		mod_.NC_DISCHARGE_METHOD AS discharge_method_code,
		mod_.NC_DISCHARGE_METHOD_NAME AS discharge_method_name,
		spec.DHSS_CODE AS treatment_function_code,
		spec.NC_SPECIALTY_NAME AS treatment_function_name
	FROM DWREPO.dbo.PATIENT p
	JOIN DWREPO.dbo.ADMISSIONS a
		ON a.PATIENT_ID = p.ID
	JOIN DWREPO.dbo.CONSULTANT_EPISODES ce
		ON ce.ADMISSIONS_ID = a.ID
	JOIN DWREPO.dbo.MF_METHOD_OF_ADMISSION moa
		ON moa.CODE = a.METHOD_OF_ADMISSION_CODE
		AND moa.LOGICALLY_DELETED_FLAG = 0
	JOIN DWREPO.dbo.MF_SOURCE_OF_ADMISSION soa
		ON soa.CODE = a.SOURCE_OF_ADMISSION_CODE
		AND soa.LOGICALLY_DELETED_FLAG = 0
	JOIN DWREPO.dbo.MF_METHOD_OF_DISCHARGE mod_
		ON mod_.CODE = a.METHOD_OF_DISCHARGE_CODE
		AND mod_.LOGICALLY_DELETED_FLAG = 0
	JOIN DWREPO.dbo.MF_SPECIALTY spec
		ON spec.CODE = ce.SPECIALTY_CODE
		AND spec.LOGICALLY_DELETED_FLAG = 0
	WHERE p.SYSTEM_NUMBER IN (
		SELECT UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid
	) AND a.ADMISSION_DATE_TIME > '2020-01-01'
	ORDER BY p.SYSTEM_NUMBER, a.ID, ce.EPISODE_NUMBER
	;
''')


EPISODE_INSERT_SQL = '''
	INSERT INTO `episode` (
		episode_id,
		spell_id,
		uhl_system_number,
		admission_datetime,
		discharge_datetime,
		order_no_of_episode,
		admission_method_code,
		admission_method_name,
		admission_source_code,
		admission_source_name,
		discharge_method_code,
		discharge_method_name,
		treatment_function_code,
		treatment_function_name
	) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_episode():
	print('refresh_episode')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `episode`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(EPISODE_SELECT_SQL)
			cur.executemany(EPISODE_INSERT_SQL, (list(r) for r in rs))
			hconn.commit()


DIAGNOSIS_SELECT_SQL = text('''
	SELECT DISTINCT
		d.id AS diagnosis_id,
		a.id AS spell_id,
		ce.ID AS episode_id,
		p.SYSTEM_NUMBER AS uhl_system_number,
		d.DIAGNOSIS_NUMBER AS diagnosis_number,
		mf_d.DIAGNOSIS_DESCRIPTION AS diagnosis_name,
		d.DIAGNOSIS_CODE AS diagnosis_code,
		a.ADMISSION_DATE_TIME AS admission_datetime
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
''')


DISAGNOSIS_INSERT_SQL = '''
	INSERT INTO `diagnosis` (
		diagnosis_id,
		spell_id,
		episode_id,
		uhl_system_number,
		diagnosis_number,
		diagnosis_code,
		diagnosis_name,
		admission_datetime
	) values (%s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_diagnosis():
	print('refresh_diagnosis')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `diagnosis`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(DIAGNOSIS_SELECT_SQL)
			cur.executemany(DISAGNOSIS_INSERT_SQL, (list(r) for r in rs))
			hconn.commit()


PROCEDURE_SELECT_SQL = text('''
	SELECT
		proc_.ID AS procedure_id,
		a.id AS spell_id,
		ce.ID AS episode_id,
		p.SYSTEM_NUMBER AS uhl_system_number,
		proc_.PROCEDURE_NUMBER AS procedure_number,
		proc_.PROCEDURE_CODE AS procedure_code,
		opcs.PROCEDURE_DESCRIPTION AS procedure_name,
		a.ADMISSION_DATE_TIME AS admission_datetime
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
''')


PROCEDURE_INSERT_SQL = '''
	INSERT INTO `procedure` (
		procedure_id,
		spell_id,
		episode_id,
		uhl_system_number,
		procedure_number,
		procedure_code,
		procedure_name,
		admission_datetime
	) values (%s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_procedure():
	print('refresh_procedure')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `procedure`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(PROCEDURE_SELECT_SQL)
			cur.executemany(PROCEDURE_INSERT_SQL, (list(r) for r in rs))
			hconn.commit()


TRANSFERS_SELECT_SQL = text('''
	SELECT
		a.ID as transfer_id,
		'admission' AS transfer_type,
		a.id as spell_id,
		a.admission_datetime AS transfer_datetime,
		p.SYSTEM_NUMBER AS uhl_system_number,
		ward.CODE AS ward_code,
		ward.WARD AS ward_name,
		hospital.HOSPITAL AS hospital
	FROM DWREPO.dbo.PATIENT p
	JOIN DWREPO.dbo.ADMISSIONS a
		ON a.PATIENT_ID = p.ID
	LEFT JOIN DWREPO.dbo.MF_WARD ward
		ON ward.CODE = a.ward_code
		AND ward.LOGICALLY_DELETED_FLAG = 0
	LEFT JOIN DWREPO.dbo.MF_HOSPITAL hospital
		ON hospital.CODE = a.HOSPITAL_CODE
		AND hospital.LOGICALLY_DELETED_FLAG = 0
	WHERE p.SYSTEM_NUMBER IN (
		SELECT UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid
	) AND a.ADMISSION_DATE_TIME > '01-Jan-2020'

	UNION ALL

	SELECT
		t.ID as transfer_id,
		'transfer' AS transfer_type,
		a.id as spell_id,
		t.TRANSFER_DATE_TIME AS transfer_datetime,
		p.SYSTEM_NUMBER AS uhl_system_number,
		ward.CODE AS ward_code,
		ward.WARD AS ward_name,
		hospital.HOSPITAL AS hospital
	FROM DWREPO.dbo.PATIENT p
	JOIN DWREPO.dbo.ADMISSIONS a
		ON a.PATIENT_ID = p.ID
	JOIN DWREPO.dbo.TRANSFERS t
		ON t.ADMISSIONS_ID = a.ID
	LEFT JOIN DWREPO.dbo.MF_WARD ward
		ON ward.CODE = t.TO_WARD
		AND ward.LOGICALLY_DELETED_FLAG = 0
	LEFT JOIN DWREPO.dbo.MF_HOSPITAL hospital
		ON hospital.CODE = t.TO_HOSPITAL_CODE
		AND hospital.LOGICALLY_DELETED_FLAG = 0
	WHERE p.SYSTEM_NUMBER IN (
		SELECT UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid
	) AND a.ADMISSION_DATE_TIME > '01-Jan-2020'

	UNION ALL
	
	SELECT
		a.ID as transfer_id,
		'discharge' AS transfer_type,
		a.id as spell_id,
		a.discharge_date_time AS transfer_datetime,
		p.SYSTEM_NUMBER AS uhl_system_number,
		ward.CODE AS ward_code,
		ward.WARD AS ward_name,
		hospital.HOSPITAL AS hospital
	FROM DWREPO.dbo.PATIENT p
	JOIN DWREPO.dbo.ADMISSIONS a
		ON a.PATIENT_ID = p.ID
	LEFT JOIN DWREPO.dbo.MF_WARD ward
		ON ward.CODE = a.discharge_ward
		AND ward.LOGICALLY_DELETED_FLAG = 0
	LEFT JOIN DWREPO.dbo.MF_HOSPITAL hospital
		ON hospital.CODE = a.discharge_hospital
		AND hospital.LOGICALLY_DELETED_FLAG = 0
	WHERE p.SYSTEM_NUMBER IN (
		SELECT UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid
	) AND a.ADMISSION_DATE_TIME > '01-Jan-2020'
	;
''')


TRANSFER_INSERT_SQL = '''
	INSERT INTO `transfer` (
		transfer_id,
		spell_id,
		transfer_type,
		uhl_system_number,
		transfer_datetime,
		ward_code,
		ward_name,
		hospital
	) values (%s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_transfer():
	print('refresh_transfer')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `transfer`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(TRANSFERS_SELECT_SQL)
			cur.executemany(TRANSFER_INSERT_SQL, (list(r) for r in rs))
			hconn.commit()


BLOODS_SELECT_SQL = text('''
	SELECT
		t.id AS test_id,
		p.Hospital_Number AS uhl_system_number,
		tc.Test_Code AS test_code,
		tc.Test_Expansion AS test_name,
		t.[Result] AS result,
		t.Result_Expansion AS result_expansion,
		t.Units AS result_units,
		r.WHO_COLLECTION_DATE_TIME AS sample_collected_datetime,
		t.WHO_RESULTED_DATE_TIME AS result_datetime,
		r.WHO_RECEIVE_DATE_TIME AS receive_datetime,
		CASE WHEN CHARINDEX('{', t.Reference_Range) > 0 THEN
			CASE WHEN DWBRICCS.dbo.IsReallyNumeric(LEFT(t.Reference_Range, CHARINDEX('{', t.Reference_Range) - 1)) = 1 THEN
				CAST(LEFT(t.Reference_Range, CHARINDEX('{', t.Reference_Range) - 1) AS DECIMAL(18,5))
			END
		END AS lower_range,
		CASE WHEN CHARINDEX('{', t.Reference_Range) > 0 THEN
			CASE WHEN DWBRICCS.dbo.IsReallyNumeric(SUBSTRING(t.Reference_Range, CHARINDEX('{', t.Reference_Range) + 1, LEN(t.Reference_Range))) = 1 THEN
				CAST(SUBSTRING(t.Reference_Range, CHARINDEX('{', t.Reference_Range) + 1, LEN(t.Reference_Range)) AS DECIMAL(18,5))
			END
		END AS higher_range
	FROM DWPATH.dbo.HAEM_TESTS t
	INNER JOIN	DWPATH.dbo.HAEM_RESULTS_FILE AS r
		ON t.Haem_Results_File = r.ISRN
	INNER JOIN	DWPATH.dbo.ORDERS_FILE AS o
		ON r.Order_No = o.Order_Number
	INNER JOIN	DWPATH.dbo.REQUEST_PATIENT_DETAILS AS p
		ON o.D_Level_Pointer = p.Request_Patient_Details
	JOIN DWBRICCS.dbo.all_suspected_covid asc2
		ON asc2.UHL_System_Number = p.Hospital_Number
	LEFT OUTER JOIN DWPATH.dbo.MF_TEST_CODES_HAEM_WHO tc
		ON t.Test_Code_Key=tc.Test_Codes_Row_ID
	LEFT OUTER JOIN DWPATH.dbo.REQUEST_SOURCE_DETAILS s
		ON o.C_Level_Pointer = s.Request_Source_Details
	WHERE (
				T.Result_Suppressed_Flag = 'N'
			OR  T.Result_Suppressed_Flag IS NULL
		)
		AND r.WHO_RECEIVE_DATE_TIME >= '2020-01-01'
	;
''')


BLOODS_INSERT_SQL = '''
	INSERT INTO `blood_test` (
		test_id,
		uhl_system_number,
		test_code,
		test_name,
		result,
		result_expansion,
		result_units,
		sample_collected_datetime,
		result_datetime,
		lower_range,
		higher_range,
		receive_datetime
	) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_bloods():
	print('refresh_bloods')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `blood_test`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(BLOODS_SELECT_SQL)
			cur.executemany(BLOODS_INSERT_SQL, (list(r) for r in rs))
			hconn.commit()


MICROBIOLOGY_SELECT_SQL = text('''
	SELECT
		p.Hospital_Number AS uhl_system_number,
		t.id AS test_id,
		t.Order_code order_code,
		t.Order_Code_Expan order_name,
		t.Test_code test_code,
		tc.Test_Expansion test_name,
		org.Organism organism,
		COALESCE(org.Quantity_Description, t.Result_Expansion) AS test_result,
		r.WHO_COLLECTION_DATE_TIME sample_collected_date_time,
		r.WHO_RECEIVE_DATE_TIME sample_received_date_time,
		t.WHO_TEST_RESULTED_DATE_TIME result_datetime,
		r.specimen_site
	FROM DWPATH.dbo.MICRO_TESTS t
	INNER JOIN	DWPATH.dbo.MICRO_RESULTS_FILE AS r
		ON t.Micro_Results_File = r.ISRN
	INNER JOIN	DWPATH.dbo.ORDERS_FILE AS o
		ON r.Order_No = o.Order_Number
	INNER JOIN	DWPATH.dbo.REQUEST_PATIENT_DETAILS AS p
		ON o.D_Level_Pointer = p.Request_Patient_Details
	LEFT JOIN (
		SELECT
			org.Micro_Tests,
			org.Organism,
			q.Quantity_Description,
			CASE
				WHEN q.Rec_as_Signif_Growth = 'Y' THEN 'Yes'
				ELSE 'No'
			END AS Significant_Growth
		FROM DWPATH.dbo.MICRO_ORGANISMS org
		JOIN DWPATH.dbo.MF_ORGANISM_CODES oc
			ON oc.Organism_code = org.Organism_Code
			AND oc.Organism_category = 'O'
		JOIN DWPATH.dbo.MF_QUANTITY_CODES q
			ON org.Quantifier=q.APEX_ID
	) org ON org.Micro_Tests = t.Micro_Tests
	LEFT OUTER JOIN DWPATH.dbo.MF_TEST_CODES_MICRO_WHO tc
		ON t.Test_Code_Key=tc.Test_Codes_Row_ID
	LEFT OUTER JOIN DWPATH.dbo.REQUEST_SOURCE_DETAILS s
		ON o.C_Level_Pointer = s.Request_Source_Details
	WHERE
		r.WHO_RECEIVE_DATE_TIME >= '2020-01-01'
		AND	p.Hospital_Number IN (
			SELECT asc2.UHL_System_Number
			FROM DWBRICCS.dbo.all_suspected_covid asc2
		);
''')


MICROBIOLOGY_INSERT_SQL = '''
	INSERT INTO `microbiology_test` (
		uhl_system_number,
		test_id,
		order_code,
		order_name,
		test_code,
		test_name,
		organism,
		result,
		sample_collected_datetime,
		sample_received_datetime,
		result_datetime,
		specimen_site
	) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_microbiology():
	print('refresh_microbiology')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `microbiology_test`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(MICROBIOLOGY_SELECT_SQL)
			cur.executemany(MICROBIOLOGY_INSERT_SQL, (list(r) for r in rs))
			hconn.commit()


COVID_PRESCRIBING_SQL = text('''
	SELECT
		p.externalId AS uhl_system_number,
		d.id AS order_id,
		m.prescribeMethodName AS method_name,
		m.orderType AS order_type,
		m.medicationName AS medication_name,
		d.minDose AS min_dose,
		d.maxDose AS max_dose,
		freq.frequency_narrative AS frequency,
		form.Name AS form,
		d.doseUnit AS dose_units,
		route.name AS route,
		m.createdOn AS ordered_datetime
	FROM DWEPMA.dbo.tciMedication m
	JOIN DWEPMA.dbo.tciMedicationDose d
		ON d.medicationid = m.id
	JOIN DWEPMA.dbo.tciPerson p
		ON p.id = m.personId
	LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_FORM_CODES_WHO form
		ON form.CODE = d.formCode
	LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_FREQUENCY_WHO freq
		ON freq.CODE = d.frequency
	LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_ROUTE_OF_ADMINISTRATION_WHO route
		ON route.reference = d.roa
	WHERE m.createdOn > '2020-01-01'
		AND p.externalId IN (
			SELECT asc2.UHL_System_Number
			FROM DWBRICCS.dbo.all_suspected_covid asc2
		);
''')


PRESCRIBING_INSERT_SQL = '''
	INSERT INTO `prescribing` (
		uhl_system_number,
		order_id,
		method_name,
		order_type,
		medication_name,
		min_dose,
		max_does,
		frequency,
		form,
		does_units,
		route,
		ordered_datetime
	) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_prescribing():
	print('refresh_prescribing')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `prescribing`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(COVID_PRESCRIBING_SQL)
			cur.executemany(PRESCRIBING_INSERT_SQL, (list(r) for r in rs))
			hconn.commit()


ADMINISTRATION_SELECT_SQL = text('''
	SELECT
		p.externalId AS uhl_system_number,
		a.id AS administration_id,
		a.eventDateTime AS administration_datetime,
		m.medicationName AS medication_name,
		a.doseId AS dose_id,
		a.dose,
		a.doseUnit AS dose_unit,
		form.Name AS form_name,
		roa.name AS route_name
	FROM DWEPMA.dbo.tciAdminEvent a
	JOIN DWEPMA.dbo.tciMedication m
		ON m.id = a.medicationid
	JOIN DWEPMA.dbo.tciPerson p
		ON p.id = m.personId
	LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_FORM_CODES_WHO form
		ON form.CODE = a.formCode
	LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_ROUTE_OF_ADMINISTRATION_WHO roa
		ON roa.reference = a.roa
	WHERE p.externalId IN (
			SELECT asc2.UHL_System_Number
			FROM DWBRICCS.dbo.all_suspected_covid asc2
		) AND a.eventDateTime > '2020-01-01'
	;
''')


ADMINISTRATION_INSERT_SQL = '''
	INSERT INTO `administration` (
		uhl_system_number,
		administration_id,
		administration_datetime,
		medication_name,
		dose_id,
		dose,
		dose_unit,
		form_name,
		route_name
	) values (%s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_administration():
	print('refresh_administration')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `administration`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(ADMINISTRATION_SELECT_SQL)
			cur.executemany(ADMINISTRATION_INSERT_SQL, (list(r) for r in rs))
			hconn.commit()


OBSERVATION_SELECT_SQL = text('''
	SELECT *
	FROM DWNERVECENTRE.dbo.ObsExport oe
	WHERE [System Number > Patient ID] IN (
		SELECT asc2.UHL_System_Number
		FROM DWBRICCS.dbo.all_suspected_covid asc2
	) AND oe.Timestamp >= '2020-01-01';
''')


OBSERVATION_INSERT_SQL = '''
	INSERT INTO `observation` (
		observation_id,
		uhl_system_number,
		observation_datetime,
		observation_name,
		observation_value,
		observation_ews,
		observation_units
	) values (%s, %s, %s, %s, %s, %s, %s);
'''


def refresh_observation():
	print('refresh_observation')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `observation`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(OBSERVATION_SELECT_SQL)

			observation_names = [c[:-4] for c in rs.keys() if c.lower().endswith('_ews') and c[:-4] in rs.keys()]
			ews_names = {c[:-4]: c for c in rs.keys() if c.lower().endswith('_ews')}
			units_names = {c[:-6]: c for c in rs.keys() if c.lower().endswith('_units')}

			observations = [(o, ews_names[o], units_names[o]) for o in observation_names]

			for row in rs:
				obs = []

				observation_id = row['ObsId']
				uhl_system_number = row['System Number > Patient ID']
				observation_datetime = row['Timestamp']

				for observation_name, observation_ews, observation_units in observations:
					if row[observation_name] is not None or row[observation_ews] is not None:
						obs.append([
							observation_id,
							uhl_system_number,
							observation_datetime,
							observation_name,
							row[observation_name],
							row[observation_ews],
							row[observation_units],
						])

				cur.executemany(OBSERVATION_INSERT_SQL, obs)
			hconn.commit()


CLINICAL_CARE_PERIOD_SELECT_SQL = text('''
	SELECT
		p.SYSTEM_NUMBER,
		ccp.ID AS ccp_id,
		ccp.CCP_LOCAL_IDENTIFIER,
		spec.DHSS_CODE,
		spec.NC_SPECIALTY_NAME,
		ccp.CCP_START_DATE_TIME,
		BASIC_RESP_LEVEL_DAYS,
		ADVANCED_RESP_LEVEL_DAYS,
		BASIC_CARDIO_LEVEL_DAYS,
		ADVANCED_CARDIO_LEVEL_DAYS,
		RENAL_SUPPORT_DAYS,
		NEURO_SUPPORT_DAYS,
		DERM_SUPPORT_DAYS,
		LIVER_SUPPORT_DAYS,
		CRITICAL_CARE_LEVEL2_DAYS,
		CRITICAL_CARE_LEVEL3_DAYS,
		ccp.CCP_END_DATE_TIME
	FROM DWREPO_BASE.dbo.WHO_INQUIRE_CRITICAL_CARE_PERIODS ccp
	JOIN DWREPO.dbo.PATIENT p
		ON p.ID = ccp.PATIENT_ID
	JOIN DWREPO.dbo.MF_SPECIALTY spec
		ON spec.CODE = ccp.CCP_TREATMENT_FUNCTION_CODE
	JOIN DWREPO.dbo.MF_LOCATION_WHO loc
		ON loc.code = ccp.CCP_LOCATION_CODE
	WHERE ccp.CCP_START_DATE >= '01 Jan 2020'
		AND p.SYSTEM_NUMBER IN (
			SELECT UHL_System_Number
			FROM DWBRICCS.dbo.all_suspected_covid
		) AND ccp.CCP_START_DATE_TIME >= '01 Jan 2020'
	;
''')


CLINICAL_CARE_PERIOD_INSERT_SQL = '''
	INSERT INTO `critical_care_period` (
		uhl_system_number,
		ccp_id,
		local_identifier,
		treatment_function_code,
		treatment_function_name,
		start_datetime,
		basic_respiratory_support_days,
		advanced_respiratory_support_days,
		basic_cardiovascular_support_days,
		advanced_cardiovascular_support_days,
		renal_support_days,
		neurological_support_days,
		dermatological_support_days,
		liver_support_days,
		critical_care_level_2_days,
		critical_care_level_3_days,
		discharge_datetime
	) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_clinical_care_period():
	print('refresh_clinical_care_period')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `critical_care_period`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(CLINICAL_CARE_PERIOD_SELECT_SQL)
			cur.executemany(CLINICAL_CARE_PERIOD_INSERT_SQL, (list(r) for r in rs))
			hconn.commit()


COVID_ORDERS_SQL = text('''
	SELECT
		his.HIS_ID AS uhl_system_number,
		o.ORDER_ID,
		o.ORDER_KEY,
		o.SCHEDULE_DATE,
		ev.Request_Date_Time,
		o.EXAMINATION AS examination_code,
		exam_cd.NAME AS examination_description,
		SUBSTRING(exam_cd.SNOMEDCT, 1, CHARINDEX(',', exam_cd.SNOMEDCT + ',') - 1) AS snomed_code,
		modality.MODALITY
	FROM DWRAD.dbo.CRIS_EXAMS_TBL exam
	JOIN DWRAD.dbo.CRIS_EVENTS_TBL ev
		ON ev.EVENT_KEY = exam.EVENT_KEY
	JOIN DWRAD.dbo.CRIS_EXAMCD_TBL exam_cd
		ON exam_cd.CODE = exam.EXAMINATION
	JOIN DWRAD.dbo.CRIS_ORDERS_TBL o
		ON o.EXAM_KEY = exam.EXAM_KEY
	JOIN DWRAD.dbo.CRIS_HIS_TBL his
		ON his.PASLINK_KEY = o.PASLINK_KEY
	JOIN DWRAD.dbo.MF_CRISMODL modality
		ON modality.CODE = exam_cd.MODALITY
	WHERE ev.Request_Date_Time >= '2020-01-01'
		AND ev.Request_Date_Time < :current_datetime
		AND his.HIS_ID in (
			SELECT asc2.UHL_System_Number
			FROM DWBRICCS.dbo.all_suspected_covid asc2
		)
	;
''')


ORDERS_INSERT_SQL = '''
	INSERT INTO `order` (
		uhl_system_number,
		order_id,
		order_key,
		scheduled_datetime,
		request_datetime,
		examination_code,
		examination_description,
		snomed_code,
		modality
	) values (%s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


def refresh_orders():
	print('refresh_orders')

	with hic_cursor() as cur:
		cur.execute(f"TRUNCATE TABLE `order`;")

	with hic_connection() as hconn:
		cur = hconn.cursor()

		with uhl_dwh_databases_engine() as conn:
			rs = conn.execute(COVID_ORDERS_SQL, current_datetime=datetime.utcnow())
			cur.executemany(ORDERS_INSERT_SQL, (list(r) for r in rs))
			hconn.commit()


refresh_demographics()
refresh_orders()
refresh_clinical_care_period()
refresh_observation()
refresh_administration()
refresh_prescribing()
refresh_microbiology()
refresh_bloods()
refresh_transfer()
refresh_procedure()
refresh_diagnosis()
refresh_episode()
refresh_episode()
refresh_virology()
