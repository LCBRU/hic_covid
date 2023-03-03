from database import hic_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.orders', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.orders;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.orders
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        DECLARE @now AS DATETIME

        SET @now = GETDATE()

        SELECT
            his.HIS_ID AS uhl_system_number,
            o.ORDER_ID AS order_id,
            exam_cd.CODE AS order_type,
            exam_cd.NAME AS order_description,
            o.SCHEDULE_DATE AS order_date_time
        FROM DWRAD.dbo.CRIS_EXAMS_TBL exam
        JOIN DWRAD.dbo.CRIS_EVENTS_TBL ev
            ON ev.EVENT_KEY = exam.EVENT_KEY
        JOIN DWRAD.dbo.CRIS_EXAMCD_TBL exam_cd
            ON exam_cd.CODE = exam.EXAMINATION
        JOIN DWRAD.dbo.CRIS_ORDERS_TBL o
            ON o.EXAM_KEY = exam.EXAM_KEY
        JOIN DWRAD.dbo.CRIS_HIS_TBL his
            ON his.PASLINK_KEY = o.PASLINK_KEY
        WHERE ev.Request_Date_Time >= '2020-01-01'
            AND ev.Request_Date_Time < @now
            AND his.HIS_ID in (
                SELECT asc2.UHL_System_Number
                FROM DWBRICCS.dbo.all_suspected_covid asc2
            )
        ;
	");

	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE orders ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX orders_uhl_system_number_IDX ON orders (uhl_system_number);
'''


def refresh_orders():
	print('refresh_orders: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_orders: ended')


# brc_cv_covid_orders	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_orders	order_id	anonymised/pseudonymised unique numeric local order identifier
# brc_cv_covid_orders	order_type	local pas order type
# brc_cv_covid_orders	order_description	local order name
# brc_cv_covid_orders	order_date_time	date/time of order
# brc_cv_covid_orders	brc_name	data submitting brc name

# Done

SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.order_id,
        a.order_type,
        a.order_description,
        a.order_date_time
    FROM orders a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
            AND COALESCE(a.order_date_time, datefromparts(2021, 06, 30)) <=  datefromparts(2021, 06, 30)
    ;
'''

def export_orders():
	export('orders', SQL_SELECT_EXPORT)
