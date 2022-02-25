import pymysql
import csv
import os
from dotenv import load_dotenv

load_dotenv()

def export(table_name):
    db = pymysql.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        database=os.environ['DB_DATABASE'],
    )

    cur = db.cursor()

    sql = f'SELECT * FROM {table_name}'
    csv_file_path = f'export/{table_name}.csv'

    try:
        cur.execute(sql)

        with open(csv_file_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"')

            csvwriter.writerow([d[0] for d in cur.description])

            for row in cur.fetchall():
                csvwriter.writerow(row)
    finally:
        db.close()

export('administration_export')
export('bloods_export')
export('demographics_export')
export('diagnosis_export')
export('emergency_export')
export('episode_export')
export('icu_export')
export('microbiology_export')
export('movement_export')
export('observations_export')
export('order_export')
export('prescribing_export')
export('procedure_export')
export('virology_export')
