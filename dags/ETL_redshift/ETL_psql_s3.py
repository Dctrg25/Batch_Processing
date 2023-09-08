import pandas as pd
import boto3
import psycopg2
import configparser
from datetime import datetime, timedelta
from io import BytesIO
import subprocess

config = configparser.ConfigParser()
config.read_file(open('/home/truong/airflow/dags/project_batch/config.cfg')) # sua path
HOST = config.get('POSTGRES', 'HOST')
DB_NAME = config.get('POSTGRES', 'DB_NAME')
DB_USER = config.get('POSTGRES', 'DB_USER')
DB_PASSWORD = config.get('POSTGRES', 'DB_PASSWORD')

KEY = config.get("AWS",'KEY')
SECRET = config.get("AWS", 'SECRET')

def Connect_postgres():
    connect_params = {
        "host" : HOST,
        "dbname" : DB_NAME,
        "user" : DB_USER,
        "password" : DB_PASSWORD
    }
    conn = psycopg2.connect(**connect_params)
    conn.set_session(autocommit= True)
    return conn

def Extract_from_postgres(df_dict):
    conn = Connect_postgres()
    cur = conn.cursor()

    table_list = ['customers', 'locations', 'products', 'sales', 'shipments']

    for table in table_list:
        query = """
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema = 'sale_schema'
            AND table_name = '{}'
        """.format(table)

        cur.execute(query)

        column_list = list(cur.fetchall())
        column_list = [column[0] for column in column_list]

        #query data
        data_query = """
            SELECT * FROM sale_schema.{}
        """.format(table)
        cur.execute(data_query)

        df_dict[table] = pd.DataFrame(columns=column_list, data=cur.fetchall())
    
    cur.close()
    conn.close()

def Generate_date_df():
    start_date = datetime(2022,1,1)
    end_date = datetime(2022,12,31)

    time_arr = [start_date + timedelta(days= i) for i in range((end_date - start_date).days + 1)]
    time_dict = {}
    time_dict['full_date'] = []
    time_dict['day'] = []
    time_dict['month'] = []
    time_dict['year'] = []

    for date in time_arr:
        time_dict['full_date'].append(date.date())
        time_dict['day'].append(date.day)
        time_dict['month'].append(date.month)
        time_dict['year'].append(date.year)

    return time_dict

def joining_df(df1, df2, left, right):
    return df1.merge(df2, left_on = left, right_on= right, how= 'inner')

def dropping_column(df, list_column):
    df.drop(columns= list_column, inplace= True)

def Transform(df_dict):
    df_dict['time'] = pd.DataFrame(Generate_date_df())

    # join shipments_df to sales_df on 'order_id'
    df_to_join = df_dict['shipments'][['order_id', 'shipment_id', 'shipping_cost', 'shipping_address_zipcode']]
    df_dict['sales'] = joining_df(df_dict['sales'], df_to_join, 'order_id', 'order_id')

    # join shipments_df to location_df on Shipping_adress_zipcode
    df_to_join = df_dict['shipments'][['shipping_address', 'shipping_address_zipcode']]
    df_dict['locations'] = joining_df(df_dict['locations'], df_to_join, 'address_postal_code', 'shipping_address_zipcode')

    # Drop unnecessary column
    dropping_column(df_dict['customers'], ['address', 'postal_code', 'address_postal_code'])
    dropping_column(df_dict['shipments'],['shipping_cost','order_id','shipping_address','shipping_address_zipcode'])
    dropping_column(df_dict['locations'],['shipping_address_zipcode'])

        # Rename column
    df_dict['sales'].rename(columns = {"order_id" : "sale_id", "total_cost" : "revenue"}, inplace = True)
    
    # Re-order columns to fit redshift table
    df_dict['sales'] = df_dict['sales'][['sale_id', 'revenue', 'profit', 'quantity', 'shipping_cost',\
                                'product_id', 'customer_id', 'shipping_address_zipcode', 'order_date', 'shipment_id']]

def upload_to_s3(s3, bucket_name, df, key):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index= False)
    csv_buffer.seek(0)

    s3.upload_fileobj(csv_buffer, bucket_name, key + '.csv')

def Load_S3(df_dict):
    session = boto3.Session(
        aws_access_key_id= KEY,
        aws_secret_access_key= SECRET
    )

    s3 = session.client("s3")
    bucket_name='ductruong-sale-bucket'

    try :
        response = s3.list_objects_v2(Bucket = bucket_name)
        for obj in response['Contents']:
            key = obj['Key']
            s3.delete_object(Bucket = bucket_name, Key = key)
    except :
        pass

    for table, df in df_dict.items():
        print(f"Loading {table} to S3")
        upload_to_s3(s3,bucket_name,df,table)
        print("Load successfully \n")

def ETL_s3():
    pd.set_option('display.max_columns', None)
    df_dict = {}
    Extract_from_postgres(df_dict)
    Transform(df_dict)
    Load_S3(df_dict)