from database import hic_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.microbiology_test', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.microbiology_test;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.microbiology_test
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            p.Hospital_Number AS uhl_system_number,
            t.Order_code AS order_code,
            r.WHO_COLLECTION_DATE_TIME AS collection_date_time,
            r.specimen_site AS site,
            org.Organism AS organism_or_bug,
            COALESCE(org.Quantity_Description, t.Result_Expansion) AS sensitivity
        FROM DWPATH.dbo.MICRO_TESTS t
        INNER JOIN  DWPATH.dbo.MICRO_RESULTS_FILE AS r
            ON t.Micro_Results_File = r.ISRN
        INNER JOIN  DWPATH.dbo.ORDERS_FILE AS o
            ON r.Order_No = o.Order_Number
        INNER JOIN  DWPATH.dbo.REQUEST_PATIENT_DETAILS AS p
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
        WHERE
            r.WHO_RECEIVE_DATE_TIME >= '2020-01-01'
            AND p.Hospital_Number IN (
                SELECT asc2.UHL_System_Number
                FROM DWBRICCS.dbo.all_suspected_covid asc2
            );

	");

	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE microbiology_test ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX microbiology_test_uhl_system_number_IDX ON microbiology_test (uhl_system_number);
'''


def refresh_microbiology():
	print('refresh_microbiology: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_microbiology: ended')


# brc_cv_covid_microbiology	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_microbiology	order_code	local laboratory order code
# brc_cv_covid_microbiology	collection_date_time	date/time microbiology collection
# brc_cv_covid_microbiology	site	body location sample was taken from
# brc_cv_covid_microbiology	organism_or_bug	organisms/bug tested for
# brc_cv_covid_microbiology	sensitivity	results of the test
# brc_cv_covid_microbiology	brc_name	data submitting brc name

SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.order_code,
        a.collection_date_time,
        a.site,
        a.organism_or_bug,
        a.sensitivity
    FROM microbiology_test a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
            AND a.collection_date_time <= '20210630'
    ;
'''

def export_microbiology():
	export('microbiology', SQL_SELECT_EXPORT)
