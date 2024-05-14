import os
import psycopg2

from get_env import GetEnv

envs = GetEnv()

PG_HOST = envs.get_env('PG_HOST')
PG_PORT = int(envs.get_env('PG_PORT'))
PG_DB = envs.get_env('PG_DB')
PG_USER = envs.get_env('PG_USER')
PG_PWD = envs.get_env('PG_PWD')


with open(os.path.dirname(os.path.abspath(__file__)) + '/sql/create_table.sql', 'r') as file, \
        psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PWD) as conn:
    with conn.cursor() as cur:
        cur.execute(file.read())
        conn.commit()
        print('Success')