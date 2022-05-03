import csv
from database import hic_conn, hic_cursor

def export(table_name, sql):
    print(f'Exporting {table_name}')

    csv_file_path = f'export/{table_name}.csv'

    with hic_conn() as con:
        cur = con.cursor()
        cur.execute(sql)

        with open(csv_file_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter='|', quotechar='"')

            csvwriter.writerow([d[0] for d in cur.description] + ['brc_name'])

            for i, row in enumerate(cur, start=1):
                if i % 1000 == 0:
                    print(f'exporting record {i:,}')

                csvwriter.writerow(list(row) + ['NIHR Leicester Biomedical Research Centre'])

    print(f'Exporting {table_name} completed')
