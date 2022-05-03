from database import hic_conn

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
            a.ID as transfer_id,
            'admission' AS transfer_type,
            p.SYSTEM_NUMBER AS uhl_system_number,
            a.id as spell_identifier,
            NULL AS episode_identifier,
            a.admission_datetime AS ward_start_date_time,
            ward.WARD AS ward_name,
            NULL AS bed_name
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
            p.SYSTEM_NUMBER AS uhl_system_number,
            a.id as spell_identifier,
            NULL AS episode_identifier,
            t.TRANSFER_DATE_TIME AS ward_start_date_time,
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

        UNION ALL
        
        SELECT
            a.ID as transfer_id,
            'discharge' AS transfer_type,
            p.SYSTEM_NUMBER AS uhl_system_number,
            a.id as spell_identifier,
            NULL AS episode_identifier,
            a.discharge_date_time AS ward_start_date_time,
            ward.WARD AS ward_name,
            NULL AS bed_name
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
