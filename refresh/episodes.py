from database import hic_conn
from refresh import export

SQL_DROP_TABLE = '''
	IF OBJECT_ID(N'dbo.episodes', N'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.episodes;
		END;
'''

SQL_INSERT = '''
	SET QUOTED_IDENTIFIER OFF;

	SELECT *
	INTO wh_hic_covid.dbo.episodes
	FROM OPENQUERY(
		uhldwh, "
		SET NOCOUNT ON;

        SELECT
            p.SYSTEM_NUMBER AS uhl_system_number,
            a.id AS spell_identifier,
            ce.ID AS episode_identifier,
            NULL AS arrival_dt_tm,
            NULL AS departure_dt_tm,
            a.ADMISSION_DATE_TIME AS admission_date_time,
            a.DISCHARGE_DATE_TIME AS discharge_date_time,
            ce.CONS_EPISODE_START_DATE_TIME AS episode_start_time,
            ce.CONS_EPISODE_END_DATE_TIME AS episode_end_time,
            ROW_NUMBER() OVER (
                PARTITION BY a.ID
                ORDER BY ce.CONS_EPISODE_START_DATE_TIME
            ) AS order_no_of_episode,
            moa.NC_ADMISSION_METHOD AS admission_method,
            moa.NC_ADMISSION_METHOD_NAME AS admission_method_desc,
            soa.NC_SOURCE_OF_ADMISSION AS admission_source,
            soa.NC_SOURCE_OF_ADMISSION_NAME AS admission_source_desc,
            mod_.NC_DISCHARGE_METHOD AS discharge_method,
            mod_.NC_DISCHARGE_METHOD_NAME AS discharge_method_desc,
            spec.DHSS_CODE AS main_specialty_code,
            spec.NC_SPECIALTY_NAME AS main_specialty_code_desc,
            NULL AS treatment_function_code,
            NULL AS treatment_function_code_desc
        FROM DWREPO.dbo.PATIENT p
        JOIN DWREPO.dbo.ADMISSIONS a
            ON a.PATIENT_ID = p.ID
        JOIN DWREPO.dbo.CONSULTANT_EPISODES ce
            ON ce.ADMISSIONS_ID = a.ID
        JOIN DWREPO.dbo.MF_METHOD_OF_ADMISSION moa
            ON moa.CODE = a.METHOD_OF_ADMISSION_CODE
            AND moa.LOGICALLY_DELETED_FLAG = 0
        JOIN DWREPO.dbo.MF_SOURCE_OF_ADMISSION soa
            ON soa.CODE = a.SOURCE_OF_ADMISSION_CODE
            AND soa.LOGICALLY_DELETED_FLAG = 0
        JOIN DWREPO.dbo.MF_METHOD_OF_DISCHARGE mod_
            ON mod_.CODE = a.METHOD_OF_DISCHARGE_CODE
            AND mod_.LOGICALLY_DELETED_FLAG = 0
        JOIN DWREPO.dbo.MF_SPECIALTY spec
            ON spec.CODE = ce.SPECIALTY_CODE
            AND spec.LOGICALLY_DELETED_FLAG = 0
        WHERE p.SYSTEM_NUMBER IN (
            SELECT UHL_System_Number
            FROM DWBRICCS.dbo.all_suspected_covid
        ) AND a.ADMISSION_DATE_TIME > '2020-01-01'
        ORDER BY p.SYSTEM_NUMBER, a.ID, ce.EPISODE_NUMBER
        ;
	");

	SET QUOTED_IDENTIFIER ON;
'''

SQL_ALTER_TABLE = '''
	ALTER TABLE episodes ALTER COLUMN uhl_system_number varchar(30) COLLATE Latin1_General_CI_AS NOT NULL;
'''

SQL_INDEXES = '''
	CREATE INDEX episodes_uhl_system_number_IDX ON episodes (uhl_system_number);
'''


def refresh_episodes():
	print('refresh_episodes: started')

	with hic_conn() as con:
		con.execute(SQL_DROP_TABLE)
		con.execute(SQL_INSERT)
		con.execute(SQL_ALTER_TABLE)
		con.execute(SQL_INDEXES)

	print('refresh_episodes: ended')


# brc_cv_covid_episodes	subject	anonymised/pseudonymised patient identifier	
# brc_cv_covid_episodes	spell_identifier	patient unique inpatient spell identifier	
# brc_cv_covid_episodes	episode_identifier	patient unique inpatient episode identifier	
# brc_cv_covid_episodes	arrival_dt_tm	date/time patient arrived at hospital	
# brc_cv_covid_episodes	departure_dt_tm	date/time patient departed from hospital	
# brc_cv_covid_episodes	admission_date_time	date/time patient admitted to inpatient	
# brc_cv_covid_episodes	discharge_date_time	date/time patient discharged from inpatient	
# brc_cv_covid_episodes	episode_start_time	start date/time of the episodes within spell	
# brc_cv_covid_episodes	episode_end_time	end date/time of the episodes within spell	
# brc_cv_covid_episodes	order_no_of_episode	sequential episode number within a spell	
# brc_cv_covid_episodes	admission_method	method of admission to inpatient	Enumerator
# 11	Waiting list
# 12	Booked
# 13	Planned
# 21	Accident and emergency or dental casualty department of the Health Care Provider
# 22	General practitioner: after a request for immediate admission has been made direct to a Hospital Provider, i.e. not through a Bed bureau, by a GENERAL PRACTITIONER or deputy
# 23	Bed bureau
# 24	Consultant clinic, of this or another Health Care Provider
# 25	Admission via Mental Health Crisis Resolution Team
# 28	Other means
# 2A	Accident and Emergency Department of another provider where the PATIENT had not been admitted
# 2B	Transfer of an admitted PATIENT from another Hospital Provider in an emergency
# 2C	Baby born at home as intended
# 2D	Other emergency admission
# 31	Admitted ante-partum
# 32	Admitted post-partum
# 81	Transfer of any admitted PATIENT from other Hospital Provider other than in an emergency
# 82	The birth of a baby in this Health Care Provider
# 83	Baby born outside the Health Care Provider except when born at home as intended.
# 99	Not Known
# brc_cv_covid_episodes	admission_method_desc	method of admission to inpatient description	Enumerator
# brc_cv_covid_episodes	admission_source	source of admission to inpatient	Enumerator
# 1	Same NHS hospital site
# 2	Other NHS hospital site (same or different NHS Trust)
# 3	Independent Hospital Provider in the UK
# 4	Non-hospital source within the UK (e.g. home)
# 5	Non UK source such as repatriation or military personnel or foreign national
# brc_cv_covid_episodes	admission_source_desc	source of admission to inpatient description	Enumerator
# brc_cv_covid_episodes	discharge_method	method of discharge from inpatient	Enumerator
# 1	Patient discharged on clinical advice or with clinical consent
# 2	Patient discharged him/herself or was discharged by a relative or advocate
# 3	Patient discharged by mental health review tribunal, Home Secretary or court
# 4	Patient died
# 5	Stillbirth
# 8	Not applicable - hospital provider spell not yet finished (i.e. not discharged)
# 9	Not known: a validation error
# brc_cv_covid_episodes	discharge_method_desc	method of discharge from inpatient description	Enumerator
# brc_cv_covid_episodes	main_specialty_code	main speciality for episode of care	Enumerator
# 100	GENERAL SURGERY
# 101	UROLOGY
# 110	TRAUMA & ORTHOPAEDICS
# 120	ENT
# 130	OPHTHALMOLOGY
# 140	ORAL SURGERY
# 141	RESTORATIVE DENTISTRY
# 142	PAEDIATRIC DENTISTRY
# 143	ORTHODONTICS
# 145	ORAL & MAXILLO FACIAL SURGERY
# 146	ENDODONTICS
# 147	PERIODONTICS
# 148	PROSTHODONTICS
# 149	SURGICAL DENTISTRY
# 150	NEUROSURGERY
# 160	PLASTIC SURGERY
# 170	CARDIOTHORACIC SURGERY
# 171	PAEDIATRIC SURGERY
# 180	ACCIDENT & EMERGENCY
# 190	ANAESTHETICS
# 191	no longer in use
# 192	CRITICAL CARE MEDICINE
# 199	Non-UK provider; specialty function not known, treatment mainly surgical
# 300	GENERAL MEDICINE
# 301	GASTROENTEROLOGY
# 302	ENDOCRINOLOGY
# 303	CLINICAL HAEMATOLOGY
# 304	CLINICAL PHYSIOLOGY
# 305	CLINICAL PHARMACOLOGY
# 310	AUDIOLOGICAL MEDICINE
# 311	CLINICAL GENETICS
# 312	CLINICAL CYTOGENETICS and MOLECULAR GENETICS
# 313	CLINICAL IMMUNOLOGY and ALLERGY
# 314	REHABILITATION
# 315	PALLIATIVE MEDICINE
# 320	CARDIOLOGY
# 321	PAEDIATRIC CARDIOLOGY
# 330	DERMATOLOGY
# 340	RESPIRATORY MEDICINE (also known as thoracic medicine)
# 350	INFECTIOUS DISEASES
# 352	TROPICAL MEDICINE
# 360	GENITO-URINARY MEDICINE
# 361	NEPHROLOGY
# 370	MEDICAL ONCOLOGY
# 371	NUCLEAR MEDICINE
# 400	NEUROLOGY
# 401	CLINICAL NEURO-PHYSIOLOGY
# 410	RHEUMATOLOGY
# 420	PAEDIATRICS
# 421	PAEDIATRIC NEUROLOGY
# 430	GERIATRIC MEDICINE
# 450	DENTAL MEDICINE SPECIALTIES
# 460	MEDICAL OPHTHALMOLOGY
# 499	Non-UK provider; specialty function not known, treatment mainly medical
# 500	OBSTETRICS and GYNAECOLOGY
# 501	OBSTETRICS
# 502	GYNAECOLOGY
# 510	no longer in use
# 520	no longer in use
# 560	MIDWIFE EPISODE
# 600	GENERAL MEDICAL PRACTICE
# 601	GENERAL DENTAL PRACTICE
# 610	no longer in use
# 620	no longer in use
# 700	LEARNING DISABILITY
# 710	ADULT MENTAL ILLNESS
# 711	CHILD and ADOLESCENT PSYCHIATRY
# 712	FORENSIC PSYCHIATRY
# 713	PSYCHOTHERAPY
# 715	OLD AGE PSYCHIATRY
# 800	CLINICAL ONCOLOGY (previously RADIOTHERAPY)
# 810	RADIOLOGY
# 820	GENERAL PATHOLOGY
# 821	BLOOD TRANSFUSION
# 822	CHEMICAL PATHOLOGY
# 823	HAEMATOLOGY
# 824	HISTOPATHOLOGY
# 830	IMMUNOPATHOLOGY
# 831	MEDICAL MICROBIOLOGY
# 832	no longer in use
# 900	COMMUNITY MEDICINE
# 901	OCCUPATIONAL MEDICINE
# 902	COMMUNITY HEALTH SERVICES DENTAL
# 903	PUBLIC HEALTH MEDICINE
# 904	PUBLIC HEALTH DENTAL
# 950	NURSING EPISODE
# 960	ALLIED HEALTH PROFESSIONAL EPISODE
# 990	no longer in use
# brc_cv_covid_episodes	main_specialty_code_desc	main speciality for episode of care description	Enumerator
# brc_cv_covid_episodes	treatment_function_code	treatment function code of episode	Enumerator
# 100	General Surgery
# 101	Urology
# 102	Transplantation Surgery
# 103	Breast Surgery
# 104	Colorectal Surgery
# 105	Hepatobiliary & Pancreatic Surgery
# 106	Upper Gastrointestinal Surgery
# 107	Vascular Surgery
# 108	Spinal Surgery Service
# 110	Trauma & Orthopaedics
# 120	ENT
# 130	Ophthalmology
# 140	Oral Surgery
# 141	Restorative Dentistry
# 142	Paediatric Dentistry
# 143	Orthodontics
# 144	Maxillo-Facial Surgery
# 150	Neurosurgery
# 160	Plastic Surgery
# 161	Burns Care
# 170	Cardiothoracic Surgery
# 171	Paediatric Surgery
# 172	Cardiac Surgery
# 173	Thoracic Surgery
# 174	Cardiothoracic Transplantation
# 180	Accident & Emergency
# 190	Anaesthetics
# 191	Pain Management
# 192	Critical Care Medicine
# 211	Paediatric Urology
# 212	Paediatric Transplantation Surgery
# 213	Paediatric Gastrointestinal Surgery
# 214	Paediatric Trauma And Orthopaedics
# 215	Paediatric Ear Nose And Throat
# 216	Paediatric Ophthalmology
# 217	Paediatric Maxillo-Facial Surgery
# 218	Paediatric Neurosurgery
# 219	Paediatric Plastic Surgery
# 220	Paediatric Burns Care
# 221	Paediatric Cardiac Surgery
# 222	Paediatric Thoracic Surgery
# 223	Paediatric Epilepsy
# 241	Paediatric Pain Management
# 242	Paediatric Intensive Care
# 251	Paediatric Gastroenterology
# 252	Paediatric Endocrinology
# 253	Paediatric Clinical Haematology
# 254	Paediatric Audiological Medicine
# 255	Paediatric Clinical Immunology And Allergy
# 256	Paediatric Infectious Diseases
# 257	Paediatric Dermatology
# 258	Paediatric Respiratory Medicine
# 259	Paediatric Nephrology
# 260	Paediatric Medical Oncology
# 261	Paediatric Metabolic Disease
# 262	Paediatric Rheumatology
# 263	Paediatric Diabetic Medicine
# 264	Paediatric Cystic Fibrosis
# 280	Paediatric Interventional Radiology
# 290	Community Paediatrics
# 291	Paediatric Neuro-Disability
# 300	General Medicine
# 301	Gastroenterology
# 302	Endocrinology
# 303	Clinical Haematology
# 304	Clinical Physiology
# 305	Clinical Pharmacology
# 306	Hepatology
# 307	Diabetic Medicine
# 308	Blood And Marrow Transplantation
# 309	Haemophilia
# 310	Audiological Medicine
# 311	Clinical Genetics
# 313	Clinical Immunology And Allergy
# 314	Rehabilitation
# 315	Palliative Medicine
# 316	Clinical Immunology
# 317	Allergy
# 318	Intermediate Care
# 319	Respite Care
# 320	Cardiology
# 321	Paediatric Cardiology
# 322	Clinical Microbiology
# 323	Spinal Injuries
# 324	Anticoagulant Service
# 325	Sport And Exercise Medicine 
# 327	Cardiac Rehabilitation 
# 328	Stroke Medicine
# 329	Transient Ischaemic Attack
# 330	Dermatology
# 331	Congenital Heart Disease Service
# 340	Respiratory Medicine
# 341	Respiratory Physiology
# 342	Programmed Pulmonary Rehabilitation
# 343	Adult Cystic Fibrosis
# 344	Complex Specialised Rehabilitation Service
# 345	Specialist Rehabilitation Service
# 346	Local Specialist Rehabilitation Service
# 350	Infectious Diseases
# 352	Tropical Medicine
# 360	Genitourinary Medicine
# 361	Nephrology
# 370	Medical Oncology
# 371	Nuclear Medicine
# 400	Neurology
# 401	Clinical Neurophysiology
# 410	Rheumatology
# 420	Paediatrics
# 421	Paediatric Neurology
# 422	Neonatology
# 424	Well Babies
# 430	Geriatric Medicine
# 450	Dental Medicine Specialties
# 460	Medical Ophthalmology
# 501	Obstetrics
# 502	Gynaecology
# 503	Gynaecological Oncology
# 560	Midwife Episode
# 650	Physiotherapy
# 651	Occupational Therapy
# 652	Speech And Language Therapy
# 653	Podiatry
# 654	Dietetics
# 655	Orthoptics
# 656	Clinical Psychology
# 657	Prosthetics
# 658	Orthotics
# 659	Dramatherapy
# 660	Art Therapy
# 661	Music Therapy
# 662	Optometry
# 663	Podiatric Surgery
# 700	Learning Disability
# 710	Adult Mental Illness
# 711	Child And Adolescent Psychiatry
# 712	Forensic Psychiatry
# 713	Psychotherapy
# 715	Old Age Psychiatry
# 720	Eating Disorders
# 721	Addiction Services
# 722	Liaison Psychiatry
# 723	Psychiatric Intensive Care
# 724	Perinatal Psychiatry
# 725	Mental Health Recovery And Rehabiliation Service
# 726	Mental Health Dual Diagnosis Service
# 727	Dementia Assessment Service
# 800	Clinical Oncology (Previously Radiotherapy)
# 811	Interventional Radiology
# 812	Diagnostic Imaging
# 822	Chemical Pathology
# 834	Medical Virology
# 840	Audiology
# 920	Diabetic Education Service
# brc_cv_covid_episodes	treatment_function_code_desc	treatment function code of episode description	Enumerator
# brc_cv_covid_episodes	brc_name	data submitting brc name	

# Questions:
#
# 1. arrival_dt_tm and departure_dt_tm not collected
# 2. treatment_function_code not collected


SQL_SELECT_EXPORT = '''
    SELECT
        p.participant_identifier AS subject,
        a.spell_identifier,
        a.episode_identifier,
        a.arrival_dt_tm,
        a.departure_dt_tm,
        a.admission_date_time,
        a.discharge_date_time,
        a.episode_start_time,
        a.episode_end_time,
        a.order_no_of_episode,
        a.admission_method,
        a.admission_method_desc,
        a.admission_source,
        a.admission_source_desc,
        a.discharge_method,
        a.discharge_method_desc,
        a.main_specialty_code,
        a.main_specialty_code_desc,
        a.treatment_function_code,
        a.treatment_function_code_desc
    FROM episodes a
    JOIN participant p
        ON p.uhl_system_number = a.uhl_system_number
    WHERE   a.uhl_system_number IN (
                SELECT  DISTINCT e_.uhl_system_number
                FROM    episodes e_
                WHERE   e_.admission_date_time <= '20210630'
            )
    ;
'''

def export_episodes():
	export('episodes', SQL_SELECT_EXPORT)
