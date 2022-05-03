from database import hic_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.transfer', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.transfer;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.transfer
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            p.SYSTEM_NUMBER AS uhl_system_number,
            a.id as spell_identifier,
            NULL AS episode_identifier,
            t.TRANSFER_DATE_TIME AS ward_start_date_time,
            t.TRANSFER_END_DATE_TIME AS ward_end_date_time,
            ward.WARD AS ward_name,
            t.to_bed AS bed_name
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
        ORDER BY p.SYSTEM_NUMBER, t.TRANSFER_DATE_TIME
        ;
	");

	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE transfer ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX transfer_uhl_system_number_IDX ON transfer (uhl_system_number);
	CREATE INDEX transfer_spell_identifier_ward_start_date_time_IDX ON transfer (spell_identifier, ward_start_date_time);
'''


def refresh_transfer():
	print('refresh_transfer: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_transfer: ended')


# Movements

# brc_cv_covid_movements	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_movements	spell_identifier	patient unique inpatient spell identifier
# brc_cv_covid_movements	episode_identifier	patient unique episode identifier
# brc_cv_covid_movements	ward_start_date_time	ward start date/time
# brc_cv_covid_movements	ward_end_date_time	ward end date/time
# brc_cv_covid_movements	ward_name	name code of the hospital ward
# brc_cv_covid_movements	bed_name	Identifier for the bed in which the patient stayed
# brc_cv_covid_movements	brc_name	data submitting brc name

# Questions
# Episode ID is not available for transfers
# Bed does not seem to be widely recorded


SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.spell_identifier,
        a.episode_identifier,
        a.ward_start_date_time,
        a.ward_end_date_time,
        a.ward_name,
        a.bed_name
    FROM transfer a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
    ;
'''

def export_transfer():
	export('transfer', SQL_SELECT_EXPORT)
