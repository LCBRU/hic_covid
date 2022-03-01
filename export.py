import csv
from database import hic_cursor


def export(table_name):
    print(f'Exporting {table_name}')

    sql = f'SELECT * FROM {table_name}'
    csv_file_path = f'export/{table_name}.csv'

    with hic_cursor() as cur:
        cur.execute(sql)

        with open(csv_file_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"')

            csvwriter.writerow([d[0] for d in cur.description])

            for i, row in enumerate(cur.fetchall_unbuffered(), start=1):
                if i % 1000 == 0:
                    print(f'exporting record {i:,}')

                csvwriter.writerow(row)


export('demographics_export')
export('administration_export')
export('bloods_export')
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
