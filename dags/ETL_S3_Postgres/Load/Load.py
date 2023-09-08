import psycopg2
import pandas as pd
import os 
import time
import configparser

config = configparser.ConfigParser()
config.read_file(open('/home/truong/airflow/dags/project_batch/config.cfg')) # sua path
HOST = config.get('POSTGRES', 'HOST')
DB_NAME = config.get('POSTGRES', 'DB_NAME')
DB_USER = config.get('POSTGRES', 'DB_USER')
DB_PASSWORD = config.get('POSTGRES', 'DB_PASSWORD')


def Load_table(table_name, df, cur):
    df = df.convert_dtypes()
    records = df.to_records(index= False)

    column_names = ', '.join(df.columns)

    s_list = ', '.join(['%s'] * len(df.columns))

    query = f"""
        INSERT INTO {table_name} ({column_names}) VALUES ({s_list})
    """

    cur.executemany(query, records)

    print(f"Successfully insert data to table {table_name}")

def Load_schema():
    connect_params = {
        "host" : HOST,
        "dbname" : DB_NAME,
        "user" : DB_USER,
        "password" : DB_PASSWORD
    }
    conn = psycopg2.connect(**connect_params)
    cur = conn.cursor()
    conn.set_session(autocommit= True)

    table_orders = ['locations', 'customers', 'products', 'sales', 'shipments']
    root_dir = "/home/truong/airflow/dags/project_batch/Transformed_data" #sua path
    for table in table_orders:
        filePath = os.path.join(root_dir, table + ".csv")
        while(os.path.isfile(filePath) != True): time.sleep(3)

        df = pd.read_csv(filePath)
        Load_table(f"Sale_schema.{table}", df, cur)

    cur.close()
    conn.close()