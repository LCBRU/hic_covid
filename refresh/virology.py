from database import hic_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.virology', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.virology;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.virology
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            p.Hospital_Number AS uhl_system_number,
            o.Lab_Ref_No AS laboratory_department,
            t.Order_Code_Expan order_name,
            tc.Test_Expansion test_name,
            CASE
                WHEN t.Test_code = 'VBIR' THEN LTRIM(RTRIM(REPLACE(q.Quantity_Description, '*', '')))
                ELSE t.Result_Expansion
            END test_result,
            NULL AS test_result_unit,
            NULL AS result_flag,
            r.WHO_COLLECTION_DATE_TIME sample_collected_date_time,
            r.WHO_RECEIVE_DATE_TIME sample_received_date_time,
            t.WHO_TEST_RESULTED_DATE_TIME result_available_date_time
        FROM DWPATH.dbo.MICRO_TESTS t
        INNER JOIN  DWPATH.dbo.MICRO_RESULTS_FILE AS r
            ON t.Micro_Results_File = r.ISRN
        INNER JOIN  DWPATH.dbo.ORDERS_FILE AS o
            ON r.Order_No = o.Order_Number
        INNER JOIN  DWPATH.dbo.REQUEST_PATIENT_DETAILS AS p
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
                OR (t.Test_code = 'VBIR' AND org.Organism  LIKE '%CoV%')
            )
            AND r.WHO_COLLECTION_DATE_TIME >= '01/01/2020 00:0:0'
            AND p.Hospital_Number in (
                SELECT asc2.UHL_System_Number
                FROM DWBRICCS.dbo.all_suspected_covid asc2
            )
        ;
	");

	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE virology ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX virology_uhl_system_number_IDX ON virology (uhl_system_number);
'''


def refresh_virology():
	print('refresh_virology: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_virology: ended')


# brc_cv_covid_virology	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_virology	laboratory_department	local laboratory type
# brc_cv_covid_virology	order_name	local laboratory order name
# brc_cv_covid_virology	test_name	local pathology test name
# brc_cv_covid_virology	test_result	result of the laboratory test
# brc_cv_covid_virology	test_result_unit	unit for results
# brc_cv_covid_virology	result_flag	local flag indicating high/low result
# brc_cv_covid_virology	sample_collected_date_time	date/time sample collected
# brc_cv_covid_virology	sample_received_date_time	date/time sample received
# brc_cv_covid_virology	result_available_date_time	date/time result available
# brc_cv_covid_virology	brc_name	data submitting brc name

SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.laboratory_department,
        a.order_name,
        a.test_name,
        a.test_result,
        a.test_result_unit,
        a.result_flag,
        a.sample_collected_date_time,
        a.sample_received_date_time,
        a.result_available_date_time
    FROM virology a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
            AND a.sample_collected_date_time <= '20210630'
            AND a.sample_received_date_time <= '20210630'
            AND a.result_available_date_time <= '20210630'
    ;
'''

def export_virology():
	export('virology', SQL_SELECT_EXPORT)
