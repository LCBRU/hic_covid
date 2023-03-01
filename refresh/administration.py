from database import hic_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.administration', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.administration;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.administration
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            person.externalId AS uhl_system_number,
            a.id AS prescription_order_id,
            STRING_AGG( ISNULL(dc.name, ' '), ', ') AS therapeutical_class,
            m.medicationName AS medication_name,
            a.eventDateTime AS administration_datetime,
            a.doseUnit AS dosage_unit,
            roa.name AS route
        FROM DWEPMA.dbo.tciAdminEvent a
        JOIN DWEPMA.dbo.tciMedication m
            ON m.id = a.medicationid
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
        WHERE person.externalId IN (
                SELECT asc2.UHL_System_Number
                FROM DWBRICCS.dbo.all_suspected_covid asc2
            ) AND a.eventDateTime > '2020-01-01'
        GROUP BY person.externalId,
                a.id,
                a.eventDateTime,
                m.medicationName,
                a.doseId,
                a.dose,
                a.doseUnit,
                roa.name
        ;
	");

	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE administration ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX administration_uhl_system_number_IDX ON administration (uhl_system_number);
'''


def refresh_administration():
	print('refresh_administration: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_administration: ended')


# brc_cv_covid_pharmacy_administration	subject	anonymised/pseudonymised patient identifier
# brc_cv_covid_pharmacy_administration	prescription_order_id	anonymised/pseudonymised order ID 
# brc_cv_covid_pharmacy_administration	therapeutical_class	therapeutic class
# brc_cv_covid_pharmacy_administration	medication_name	name of the medication
# brc_cv_covid_pharmacy_administration	administration_date_time	date/time of administration
# brc_cv_covid_pharmacy_administration	dosage_unit	administered dosage
# brc_cv_covid_pharmacy_administration	route	administered route
# brc_cv_covid_pharmacy_administration	brc_name	data submitting brc name


# New Query

# Changes
# 1. Add therapeutic classes

# Questions:
# 1. There are multiple therapeutic classes per medication

SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.prescription_order_id,
        a.therapeutical_class,
        a.medication_name,
        a.administration_datetime,
        a.dosage_unit,
        a.route
    FROM administration a
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

def export_administration():
	export('administration', SQL_SELECT_EXPORT)
