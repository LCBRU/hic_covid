from database import hic_conn, uhl_dwh_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.icu_medication', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.icu_medication;
		END;
'''

SQL_CREATE_TEMP_TABLE = '''
    SET NOCOUNT ON;

    SELECT
        person.externalId AS uhl_system_number,
        REPLACE(REPLACE(WHO_PRESCRIBED_WARDSTAY_ID, ' ', ''), ':', '') AS icu_encounter_identifier,
        m.medicationName AS medication_name,
        a.eventDateTime AS administration_datetime,
        a.dose,
        a.doseUnit AS dosage_unit,
        m.createdOn AS scheduled_datetime,
        roa.name AS route,
        form.Name AS form_name,
        NULL AS total_volume,
        NULL AS total_volume_unit,
        NULL AS base_volume,
        NULL AS base_volume_unit,
        a.rate AS rate_adm,
        a.rateUnit AS rate_adm_unit,
        NULL AS amount,
        NULL AS amount_unit
    INTO DWBRICCS.dbo.temp_hic_icu_medication
    FROM DWEPMA.dbo.tciAdminEvent a
    JOIN DWEPMA.dbo.tciMedication m
        ON m.id = a.medicationid
    LEFT JOIN DWREPO.dbo.TRANSFERS t
        ON t.ID = m.WHO_PRESCRIBED_WARDSTAY_ID
    LEFT JOIN DWEPMA.dbo.udmProduct product
        ON product.ID = m.udmProductID
    LEFT JOIN DWEPMA.dbo.udmDrugClassProduct dcp
        ON dcp.ProductID = product.ID
    LEFT JOIN DWEPMA.dbo.udmDrugClassBrand dcb
        ON dcb.ID = dcp.DrugClassBrandID
    LEFT JOIN DWEPMA.dbo.udmDrugClassAIS dcais
        ON dcais.ID = dcb.DrugClassActiveIngredientSetID
    JOIN DWEPMA.dbo.udmDrugClass dc
        ON dc.ID = dcais.DrugClassID
    JOIN DWEPMA.dbo.tciPerson person
        ON person.id = m.personId
    LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_ROUTE_OF_ADMINISTRATION_WHO roa
        ON roa.reference = a.roa
    LEFT JOIN DWEPMA.dbo.MF_DOSAGE_ASSIST_FORM_CODES_WHO form
        ON form.CODE = a.formCode
    WHERE person.externalId IN (
            SELECT asc2.UHL_System_Number
            FROM DWBRICCS.dbo.all_suspected_covid asc2
        ) AND a.eventDateTime > '2020-01-01'
        AND REPLACE(REPLACE(WHO_PRESCRIBED_WARDSTAY_ID, ' ', ''), ':', '') IN (
            SELECT ID
            FROM DWREPO.dbo.WHO_INQUIRE_CRITICAL_CARE_PERIODS
        )
    ;
'''

SQL_INSERT = '''
    SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.icu_medication
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT *
        FROM DWBRICCS.dbo.temp_hic_icu_medication;

	");

    SET QUOTED_IDENTIFIER ON;
'''

SQL_CLEAN_UP = '''
	IF OBJECT_ID(N'dwbriccs.dbo.temp_hic_icu_medication', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dwbriccs.dbo.temp_hic_icu_medication;
		END;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE icu_medication ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX icu_medication_uhl_system_number_IDX ON icu_medication (uhl_system_number);
'''


def refresh_icu_medication():
    print('refresh_icu_medication: started')

    print('refresh_icu_medication: extracting')

    with uhl_dwh_conn() as con:
        con.execute(SQL_CLEAN_UP)
        con.execute(SQL_CREATE_TEMP_TABLE)

    print('refresh_icu_medication: moving')

    with hic_conn() as con:
        con.execute(SQL_DROP_TABLE)
        con.execute(SQL_INSERT)
        con.execute(SQL_ALTER_TABLE)
        con.execute(SQL_INDEXES)

    print('refresh_icu_medication: cleaning up')

    with uhl_dwh_conn() as con:
        con.execute(SQL_CLEAN_UP)

    print('refresh_icu_medication: ended')


# ICU Medication

# brc_cv_covid_icu_medication	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_icu_medication	icu_encounter_identifier	ICU encounter identifier - link to other encounter data 
# brc_cv_covid_icu_medication	medication_name	name of the medication
# brc_cv_covid_icu_medication	administration_datetime	date/time of administration
# brc_cv_covid_icu_medication	dose	dose
# brc_cv_covid_icu_medication	dose_unit	unit of dose
# brc_cv_covid_icu_medication	scheduled_datetime	scheduled date/time
# brc_cv_covid_icu_medication	route	route
# brc_cv_covid_icu_medication	site	site
# brc_cv_covid_icu_medication	formulation	formulation
# brc_cv_covid_icu_medication	total_volume	total volume
# brc_cv_covid_icu_medication	total_volume_unit	total volume unit
# brc_cv_covid_icu_medication	base_volume	base volume
# brc_cv_covid_icu_medication	base_volume_unit	base volume unit
# brc_cv_covid_icu_medication	rate_adm	rate administration
# brc_cv_covid_icu_medication	rate_adm_unit	rate administration unit
# brc_cv_covid_icu_medication	amount	amount
# brc_cv_covid_icu_medication	amount_unit	amount unit
# brc_cv_covid_icu_medication	brc_name	data submitting brc name

# Questions
# 1. No site
# 2. Don't understand volumes - there are volumes but it doesn't seem to match
# 3. No amount fields


SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.icu_encounter_identifier,
        a.medication_name,
        a.administration_datetime,
        a.dose,
        a.dosage_unit,
        a.scheduled_datetime,
        a.route,
        NULL AS site,
        a.form_name AS formulation,
        a.total_volume,
        a.total_volume_unit,
        a.base_volume,
        a.base_volume_unit,
        a.rate_adm,
        a.rate_adm_unit,
        a.amount,
        a.amount_unit
    FROM icu_medication a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
            AND a.administration_datetime <= '20210630'
    ;
'''

def export_icu_medication():
	export('icu_medication', SQL_SELECT_EXPORT)
