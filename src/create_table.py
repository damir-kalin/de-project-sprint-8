import os
import psycopg2

HOST = 'localhost'
PORT = 5432
DB = 'de'
USER = 'jovyan'
PWD = 'jovyan'

with open(os.path.dirname(os.path.abspath(__file__)) + '/sql/create_table.sql', 'r') as file, psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=USER, password=PWD) as conn:
    with conn.cursor() as cur:
        cur.execute(file.read())
        conn.commit()
        print('Success')