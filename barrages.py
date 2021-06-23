from datetime import datetime,date,timedelta
import tabula
import psycopg2
from dateutil.relativedelta import relativedelta

pdf_path = "http://81.192.10.228/wp-content/uploads/2021/06/"

today = date.today()

offset = max(0, (today.weekday() + 6) % 7 - 3)

timedelta = timedelta(offset)

most_recent = today - timedelta


dyn = (most_recent.day).__str__() + "_" + most_recent.month.__str__() + "_" + most_recent.year.__str__() + ".pdf"

dfs = tabula.read_pdf(pdf_path + dyn, stream=True, output_format='json', pages='all')

barrages = {
    'ALWAHDA': 'Al Wahda',
    'IDRISS 1 er': 'Idriss 1er',
    'EL KENSERA': 'El Kensera',
    'OUED EL MAKHAZINE': 'Oued El Makhazine',
    'BIN EL OUIDANE': 'Bin El Ouidane',
    'AHMED AL HANSSALI': 'Al Hanssali',
    'AL MASSIRA': 'Al Massira',
    'HASSAN II': 'Hassan II',
    'MOHAMED V': 'Mohamed V',
    'BARRAGE SUR OUED ZA': 'Oued ZA'
}
try:
    connection = psycopg2.connect(user="myuser" ,
                                  password="mypass",
                                  host="localhost",
                                  port="5432",
                                  database="barragedb")

    cursor = connection.cursor()
    for idx, page in enumerate(dfs):
       
        for bar in page['data']:
            print(bar[0]['text'])
            if bar[0]['text'] in barrages.keys():
                
                postgres_insert_query = """ INSERT INTO  barrage2 (name, normal_capacity, reserve, fill_rate, date) VALUES (%s,%s,%s,%s,%s)"""
                record_to_insert = (
                    barrages[bar[0]['text']], bar[1]['text'].replace(',', '.'), bar[2]['text'].replace(',', '.'),
                    bar[3]['text'].replace(',', '.'), date.today())
                cursor.execute(postgres_insert_query, record_to_insert)
                record_to_insert = (
                    barrages[bar[0]['text']], bar[1]['text'].replace(',', '.'), bar[4]['text'].replace(',', '.'),
                    bar[5]['text'].replace(',', '.'),
                    date.today() - relativedelta(years=1))
                cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    print(count, "Record inserted successfully into mobile table")                             

except (Exception, psycopg2.DatabaseError) as error:
    print("Error while creating PostgreSQL table", error)
finally:
    # closing database connection.
    if (connection):
        #cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

