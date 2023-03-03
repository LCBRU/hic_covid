from database import hic_conn, uhl_dwh_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.blood_test', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.blood_test;
		END;
'''

SQL_INSERT = '''
	SELECT *
	INTO wh_hic_covid.dbo.blood_test
	FROM OPENQUERY(
		uhldwh, '
		SELECT *
		FROM dwbriccs.dbo.temp_hic_bloods_xfer
	');
'''

SQL_CREATE_TEMP_TABLE = '''
	IF OBJECT_ID(N'dwbriccs.dbo.temp_hic_bloods_patients', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dwbriccs.dbo.temp_hic_bloods_patients;
		END;

	CREATE TABLE dwbriccs.dbo.temp_hic_bloods_patients (
		Request_Patient_Details VARCHAR(50) PRIMARY KEY,        
		UHL_System_Number VARCHAR(40)
	);

	INSERT INTO dwbriccs.dbo.temp_hic_bloods_patients (Request_Patient_Details, UHL_System_Number)
	SELECT DISTINCT p.Request_Patient_Details, UHL_System_Number
	FROM DWBRICCS.dbo.all_suspected_covid asc2
	JOIN DWPATH.dbo.REQUEST_PATIENT_DETAILS AS p
		ON p.Hospital_Number = asc2.UHL_System_Number
	WHERE UHL_System_Number IS NOT NULL;

	IF OBJECT_ID(N'dwbriccs.dbo.temp_hic_bloods_results', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dwbriccs.dbo.temp_hic_bloods_results;
		END;

	CREATE TABLE dwbriccs.dbo.temp_hic_bloods_results (
		ISRN VARCHAR(50) PRIMARY KEY,
		D_Level_Pointer VARCHAR(254),
		Lab_Ref_No VARCHAR(20),
		WHO_COLLECTION_DATE_TIME DATETIME,
		WHO_RECEIVE_DATE_TIME DATETIME
	);

	INSERT INTO dwbriccs.dbo.temp_hic_bloods_results (ISRN, D_Level_Pointer, Lab_Ref_No, WHO_COLLECTION_DATE_TIME, WHO_RECEIVE_DATE_TIME)
	SELECT
		r.ISRN,
		o.D_Level_Pointer,
		o.Lab_Ref_No,
		r.WHO_COLLECTION_DATE_TIME,
		r.WHO_RECEIVE_DATE_TIME
	FROM DWPATH.dbo.HAEM_RESULTS_FILE r
	INNER JOIN  DWPATH.dbo.ORDERS_FILE AS o
		ON r.Order_No = o.Order_Number
	WHERE r.WHO_RECEIVE_DATE_TIME >= CONVERT(DATETIME, '2020-01-01', 102);

	CREATE INDEX idx_results_D_Level_Pointer
	ON dwbriccs.dbo.temp_hic_bloods_results (D_Level_Pointer);

	IF OBJECT_ID(N'dwbriccs.dbo.temp_hic_bloods_patient_results', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dwbriccs.dbo.temp_hic_bloods_patient_results;
		END;

	CREATE TABLE dwbriccs.dbo.temp_hic_bloods_patient_results (
		ISRN VARCHAR(50) PRIMARY KEY,
		Lab_Ref_No VARCHAR(20),
		UHL_System_Number VARCHAR(40),
		WHO_COLLECTION_DATE_TIME DATETIME,
		WHO_RECEIVE_DATE_TIME DATETIME
	);

	INSERT INTO dwbriccs.dbo.temp_hic_bloods_patient_results(ISRN, Lab_Ref_No, UHL_System_Number, WHO_COLLECTION_DATE_TIME, WHO_RECEIVE_DATE_TIME)
	SELECT
		r.ISRN,
		r.Lab_Ref_No,
		p.UHL_System_Number,
		r.WHO_COLLECTION_DATE_TIME,
		r.WHO_RECEIVE_DATE_TIME
	FROM dwbriccs.dbo.temp_hic_bloods_results r
	JOIN dwbriccs.dbo.temp_hic_bloods_patients p
		ON p.Request_Patient_Details = r.D_Level_Pointer
	;

	DROP TABLE dwbriccs.dbo.temp_hic_bloods_results;
	DROP TABLE dwbriccs.dbo.temp_hic_bloods_patients;

	IF OBJECT_ID(N'dwbriccs.dbo.temp_hic_bloods_tests', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dwbriccs.dbo.temp_hic_bloods_tests;
		END;

	CREATE TABLE dwbriccs.dbo.temp_hic_bloods_tests (
		id BIGINT,
		[result] VARCHAR(8),
		result_expansion VARCHAR(60),
		units VARCHAR(12),
		WHO_RESULTED_DATE_TIME DATETIME,
		Reference_Range VARCHAR(13),
		Test_Code_Key VARCHAR(254),
		Haem_Results_File VARCHAR(50),
		Order_Code_Expansion VARCHAR(30),
		result_Flag VARCHAR(2)
	);

	INSERT INTO dwbriccs.dbo.temp_hic_bloods_tests (id, [result], result_expansion, units, WHO_RESULTED_DATE_TIME, Reference_Range, Test_Code_Key, Haem_Results_File, Order_Code_Expansion, result_Flag)
	SELECT
		t.id,
		t.[Result],
		t.Result_Expansion,
		t.Units,
		t.WHO_RESULTED_DATE_TIME,
		t.Reference_Range,
		t.Test_Code_Key,
		t.Haem_Results_File,
		t.Order_Code_Expansion,
		t.result_flag
	FROM DWPATH.dbo.HAEM_TESTS t
	WHERE (T.Result_Suppressed_Flag = 'N' OR T.Result_Suppressed_Flag IS NULL)
		AND t.WHO_Receive_Date >= CONVERT(DATETIME, '2020-01-01', 102)
	;


	IF OBJECT_ID(N'dwbriccs.dbo.temp_hic_bloods_xfer', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dwbriccs.dbo.temp_hic_bloods_xfer;
		END;

	SELECT
		pr.UHL_System_Number AS uhl_system_number,
		pr.Lab_Ref_No AS laboratory_department,
		t.Order_Code_Expansion AS order_name,
		tc.Test_Expansion AS test_name,
		t.[Result] AS test_result,
		t.Units AS test_result_unit,
		pr.WHO_COLLECTION_DATE_TIME AS sample_collected_date_time,
		t.WHO_RESULTED_DATE_TIME AS result_available_date_time,
		t.Result_Flag AS result_flag,
		CASE WHEN CHARINDEX('{', t.Reference_Range) > 0 THEN
			CASE WHEN DWBRICCS.dbo.IsReallyNumeric(LEFT(t.Reference_Range, CHARINDEX('{', t.Reference_Range) - 1)) = 1 THEN
				CAST(LEFT(t.Reference_Range, CHARINDEX('{', t.Reference_Range) - 1) AS DECIMAL(18,5))
			END
		END AS result_lower_range,
		CASE WHEN CHARINDEX('{', t.Reference_Range) > 0 THEN
			CASE WHEN DWBRICCS.dbo.IsReallyNumeric(SUBSTRING(t.Reference_Range, CHARINDEX('{', t.Reference_Range) + 1, LEN(t.Reference_Range))) = 1 THEN
				CAST(SUBSTRING(t.Reference_Range, CHARINDEX('{', t.Reference_Range) + 1, LEN(t.Reference_Range)) AS DECIMAL(18,5))
			END
		END AS result_upper_range
	INTO dwbriccs.dbo.temp_hic_bloods_xfer
	FROM dwbriccs.dbo.temp_hic_bloods_tests t
	JOIN dwbriccs.dbo.temp_hic_bloods_patient_results pr
		ON pr.ISRN = t.Haem_Results_File
	LEFT OUTER JOIN DWPATH.dbo.MF_TEST_CODES_HAEM_WHO tc
		ON t.Test_Code_Key=tc.Test_Codes_Row_ID
	;

	DROP TABLE dwbriccs.dbo.temp_hic_bloods_tests;
	DROP TABLE dwbriccs.dbo.temp_hic_bloods_patient_results;

'''

SQL_ALTER_TABLE = '''
	ALTER TABLE blood_test ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX blood_test_uhl_system_number_IDX ON blood_test (uhl_system_number);
'''

SQL_CLEANUP = '''
	IF OBJECT_ID(N'dwbriccs.dbo.temp_hic_bloods_xfer', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dwbriccs.dbo.temp_hic_bloods_xfer;
		END;

'''

def refresh_bloods():
	print('refresh_bloods: started')

	with uhl_dwh_conn() as con:
		con.execute(SQL_CREATE_TEMP_TABLE)

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	with uhl_dwh_conn() as con:
		con.execute(SQL_CLEANUP)

	print('refresh_bloods: ended')


# brc_cv_covid_pathology_blood	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_pathology_blood	laboratory_department	local laboratory type
# brc_cv_covid_pathology_blood	order_name	local laboratory order name
# brc_cv_covid_pathology_blood	test_name	local pathology test name
# brc_cv_covid_pathology_blood	test_result	test result
# brc_cv_covid_pathology_blood	test_result_unit	unit for results
# brc_cv_covid_pathology_blood	sample_collected_date_time	date/time blood sample collected
# brc_cv_covid_pathology_blood	result_available_date_time	date/time blood result available
# brc_cv_covid_pathology_blood	result_flag	local flag indicating high/low result
# brc_cv_covid_pathology_blood	result_lower_range	lower normal result range
# brc_cv_covid_pathology_blood	result_upper_range	upper normal result range
# brc_cv_covid_pathology_blood	brc_name	data submitting brc name

# Changes
# 1. Add local lab type
# 2. Add Order Name
# 3. Add result flag indicator

# Questions
# 1. result flag has values:
# A
# AD
# AM
# D
# DM
# M
# R
# RD
# RM
# T
# TD
# TM
# 2. No laboratory_department

SQL_SELECT_EXPORT = '''
	SELECT
		p.participant_identifier AS subject,
		a.laboratory_department,
		a.order_name,
		a.test_name,
		a.test_result,
		a.test_result_unit,
		a.sample_collected_date_time,
		a.result_available_date_time,
		a.result_flag,
		a.result_lower_range,
		a.result_upper_range
	FROM blood_test a
	JOIN participant p
		ON p.uhl_system_number = a.uhl_system_number
	WHERE   a.uhl_system_number IN (
				SELECT  DISTINCT e_.uhl_system_number
				FROM    episodes e_
				WHERE   e_.admission_date_time <= '20210630'
			)
		AND COALESCE(a.sample_collected_date_time, datefromparts(2021, 06, 30)) <=  datefromparts(2021, 06, 30)
		AND COALESCE(a.result_available_date_time, datefromparts(2021, 06, 30)) <= datefromparts(2021, 06, 30)
	;
'''

def export_bloods():
	export('bloods', SQL_SELECT_EXPORT)
