import os
import psycopg2

import configparser
config = configparser.ConfigParser()
config.read_file(open('/home/truong/airflow/dags/project_batch/config.cfg'))

ARN = config.get("IAM_ROLE","ARN")

def connect_redshift():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    conn.set_session(autocommit=True)

    return conn

def create_redshift_schema(root_dir):
    conn = connect_redshift()
    cur = conn.cursor()

    path = os.path.join(root_dir, "create_redshift_schema.sql")
    with open(path, 'r') as file :
        redshift_sql = file.read()
    
    redshift_sql = redshift_sql.split(";")
    redshift_sql = [statement + ";" for statement in redshift_sql]

    for idx, statement in enumerate(redshift_sql) :
        if (statement == ";") : continue
        cur.execute(statement)

    print("Create redshift schema successfully")
    cur.close()
    conn.close()

def Load_s3_to_redshift() : # Load data from s3 to redshift
    conn = connect_redshift()
    cur = conn.cursor()
    table_list = ['customers', 'products', 'locations', 'time', 'shipments', 'sales']
    bucket_name = "ductruong-sale-bucket"
    schema = "warehouse_sales"

    for table in table_list :      
        query = f"""
            COPY {schema}.{table}
            FROM 's3://{bucket_name}/{table}.csv'
            IAM_ROLE '{ARN}'
            FORMAT AS CSV
            IGNOREHEADER 1
            FILLRECORD;
        """

        cur.execute(query)
    

    cur.close()
    conn.close()