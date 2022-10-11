from database import hic_conn, uhl_dwh_databases_engine
from refresh import export

SQL_OBSERVATION_ROWS = '''
	SELECT TOP 1 *
	FROM DWNERVECENTRE.dbo.ObsExport oe
'''


SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.observations', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.observations;
		END;
'''

SQL_CREATE_TABLE = '''
	CREATE TABLE observations (
		uhl_system_number VARCHAR(50) NOT NULL,
		observation_id INT NOT NULL,
		observation_code VARCHAR(100) DEFAULT NULL,
		observation_name VARCHAR(100) DEFAULT NULL,
		observation_datetime DATETIME DEFAULT NULL,
		observation_start_datetime DATETIME DEFAULT NULL,
		observation_end_datetime DATETIME DEFAULT NULL,
		observation_result VARCHAR(50) DEFAULT NULL,
		observation_unit VARCHAR(50) DEFAULT NULL
	);
'''

SQL_INDEXES = '''
	CREATE INDEX observations_uhl_system_number_IDX ON observations (uhl_system_number);
'''

SQL_QUERY_START = '''
	SET QUOTED_IDENTIFIER OFF;

	INSERT INTO observations(
		uhl_system_number,
		observation_id,
		observation_code,
		observation_name,
		observation_datetime,
		observation_start_datetime,
		observation_end_datetime,
		observation_result,
		observation_unit
	)
	SELECT uhl_system_number, ObsId, o_n, o_n, observation_datetime, NULL, NULL, o_v, o_u
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;
		WITH obs AS (
			SELECT
				[System Number > Patient ID] AS uhl_system_number,
				Timestamp AS observation_datetime,
				oe.*
			FROM DWNERVECENTRE.dbo.ObsExport oe
			WHERE [System Number > Patient ID] IN (
				SELECT asc2.UHL_System_Number
				FROM DWBRICCS.dbo.all_suspected_covid asc2
			) AND oe.Timestamp >= '2020-01-01'
		)
'''

SQL_QUERY_END = '''
		;
	");
	SET QUOTED_IDENTIFIER ON;
'''

def refresh_observations():
	print('refresh_observations: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_CREATE_TABLE)

		for i, bit_chunks in enumerate(chunks(_get_observations_query_bits(), 20), 1):
			q = SQL_QUERY_START + ' UNION ALL '.join(bit_chunks) + SQL_QUERY_END

			print(f'Executing chunk {i}')

			con.execute(q)

		con.execute(SQL_INDEXES)

	print('refresh_observations: ended')


def _get_observations_query_bits():
	with uhl_dwh_databases_engine() as conn:
		rs = conn.execute(SQL_OBSERVATION_ROWS)

		observation_names = [c[:-4] for c in rs.keys() if c.lower().endswith('_ews') and c[:-4] in rs.keys()]

		qbits = []

		for o in observation_names:
			qbits.append(f'''SELECT uhl_system_number, ObsId, observation_datetime, '{o}' o_n, [{o}] o_v, [{o}_units] o_u FROM obs WHERE [{o}] IS NOT NULL ''')

		return qbits


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# brc_cv_covid_vitalsigns	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_vitalsigns	observation_code	observation code
# brc_cv_covid_vitalsigns	observation_name	observation name description
# brc_cv_covid_vitalsigns	observation_datetime	date/time of observation
# brc_cv_covid_vitalsigns	observation_start_datetime	date/time of observation started
# brc_cv_covid_vitalsigns	observation_end_datetime	date/time of observation ended
# brc_cv_covid_vitalsigns	observation_result	observation result text both numeric and textual results type
# brc_cv_covid_vitalsigns	observation_unit	observation units
# brc_cv_covid_vitalsigns	brc_name	data submitting brc name

# Done

SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.observation_code,
        a.observation_name,
        a.observation_datetime,
        a.observation_start_datetime,
        a.observation_end_datetime,
		a.observation_result,
		a.observation_unit
    FROM observations a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
    ;
'''

def export_observations():
	export('observations', SQL_SELECT_EXPORT)

