from database import hic_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.prescribing', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.prescribing;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.prescribing
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            p.externalId AS uhl_system_number,
            d.id AS prescription_order_id,
            m.createdOn AS order_date_time,
            m.prescribeMethodName AS prescription_type,
            dc.Name AS therapeutical_class,
            m.medicationName AS medication_name,
            d.minDose AS ordered_dose,
            freq.frequency_narrative AS ordered_frequency,
            form.Name AS ordered_drug_form,
            d.doseUnit AS ordered_unit,
            route.name AS ordered_route,
            NULL AS admission_medicine_y_n,
            NULL AS gp_to_continue
        FROM DWEPMA.dbo.tciMedication m
        JOIN DWEPMA.dbo.udmProduct prod
            ON prod.ID = m.udmProductID
        JOIN DWEPMA.dbo.udmDrugClassProduct dcp
            ON dcp.ProductID = prod.ID
        LEFT JOIN DWEPMA.dbo.udmDrugClassBrand dcb
            ON dcb.ID = dcp.DrugClassBrandID
        LEFT JOIN DWEPMA.dbo.udmDrugClassAIS dcais
            ON dcais.ID = dcb.DrugClassActiveIngredientSetID
        JOIN DWEPMA.dbo.udmDrugClass dc
            ON dc.ID = dcais.DrugClassID
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

	");

	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE prescribing ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX prescribing_uhl_system_number_IDX ON prescribing (uhl_system_number);
'''


def refresh_prescribing():
	print('refresh_prescribing: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_prescribing: ended')


# brc_cv_covid_pharmacy_prescribing	subject	anonymised/pseudonymised patient identifier	
# brc_cv_covid_pharmacy_prescribing	prescription_order_id	laboratory order id for the prescription	
# brc_cv_covid_pharmacy_prescribing	order_date_time	date/time order prescribed	
# brc_cv_covid_pharmacy_prescribing	prescription_type	type of prescription	Enumerator
	# At home
	# Compliance box (Dosett)
	# Dietetic Product
	# DischargeMeds
	# Dispense at discharge
	# Fill Later
	# FP10 (please write)
	# home medication
	# Inpatient
	# Medications on Admission
	# Non-urgent medication (GP to prescribe)
	# Normal
	# Normal Order
	# Not Set
	# Nurse / Midwife dispensing Pre-pack 
	# Obtain from Clinic/ Other hospital
	# Obtain from drug advisory service
	# On Admission
	# On Ward - DFD supply
	# On Ward - Patient's own supply
	# Patient declined
	# Patient to buy in community (OTC)
	# Patient's own supply
	# Prescription / Discharge Order
	# Print
	# Recorded / Home Meds
	# Relabel
	# satellite (normally ambulatory care)
	# Satellite (Super Bill) Meds  
	# Sufficient supply at home 
	# Supplied during admission
	# to take out discharge
	# Urgent/Complex medication (UCLH to dispense)
	# Ward pre-pack
# brc_cv_covid_pharmacy_prescribing	therapeutical_class	drug class	
# brc_cv_covid_pharmacy_prescribing	medication_name	name of the medication	
# brc_cv_covid_pharmacy_prescribing	ordered_dose	prescribed dosage	
# brc_cv_covid_pharmacy_prescribing	ordered_frequency	prescribed frequency of medication	
# brc_cv_covid_pharmacy_prescribing	ordered_drug_form	order drug form	
# brc_cv_covid_pharmacy_prescribing	ordered_unit	no of units prescribed	
# brc_cv_covid_pharmacy_prescribing	ordered_route	prescribed route	
# brc_cv_covid_pharmacy_prescribing	admission_medicine_y_n	patient admitted on this drug	
# brc_cv_covid_pharmacy_prescribing	gp_to_continue	gp to continue after patient discharge	
# brc_cv_covid_pharmacy_prescribing	brc_name	data submitting brc name	


# Questions
# 1. Do not have admission_medicine_y_n
# 2. Do not have gp_to_continue

SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.prescription_order_id,
        a.order_date_time,
        a.prescription_type,
        a.therapeutical_class,
        a.medication_name,
		a.ordered_dose,
		a.ordered_frequency,
		a.ordered_drug_form,
		a.ordered_unit,
		a.ordered_route,
		a.admission_medicine_y_n,
		a.gp_to_continue
    FROM prescribing a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
    ;
'''

def export_prescribing():
	export('prescribing', SQL_SELECT_EXPORT)
