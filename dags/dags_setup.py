from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from dags.ETL_S3_Postgres.Transform.Transform_customers import Transform_customers
from dags.ETL_S3_Postgres.Transform.Transform_products import Transform_products
from dags.ETL_S3_Postgres.Transform.Transform_sales import Transform_sales
from dags.ETL_S3_Postgres.Transform.Transform_shipments import Transform_shipments
from dags.ETL_S3_Postgres.Transform.Transform_location import Transform_locations
from dags.ETL_S3_Postgres.Load.Load import Load_schema
from dags.ETL_redshift.ETL_psql_s3 import ETL_s3
from dags.ETL_redshift.ETL_S3_redshift import create_redshift_schema, Load_s3_to_redshift

default_args = {
    'owner': 'dctrg',
    'start_date': datetime(2023, 9, 5),
    'retries' : 3,
    'retry_delay' : timedelta(minutes=2),
    'email_on_retry' : False,
    'depends_on_past' : False
}

dag = DAG('ETL_psql_redshift_dag',
          default_args= default_args,
          description= "ETL data to psql and redshift dag",
          schedule= '@daily',
          template_searchpath= '/home/truong/airflow/dags/project_batch/Create_Postgres_schema'
          )

create_psql_schema = PostgresOperator(
    task_id = 'create_psql_schema',
    dag = dag,
    postgres_conn_id='postgres_airflow_db',
    sql= 'create_postgres_schema.sql'
)

transform_customers_psql = PythonOperator(
    task_id = 'Transform_customers',
    dag = dag,
    python_callable=Transform_customers,
    op_kwargs={"Name" : "customers", "filePath": "Customers.csv"}
)

transform_products_psql = PythonOperator(
    task_id = 'Transform_products',
    dag = dag,
    python_callable=Transform_products,
    op_kwargs={"Name" : "products", "filePath": "Products.csv"}
)

transform_sales_psql = PythonOperator(
    task_id = 'Transform_sales',
    dag = dag,
    python_callable=Transform_sales,
    op_kwargs={"Name" : "sales", "filePath": "Sales.csv"}
)

transform_shipments_psql = PythonOperator(
    task_id = 'Transform_shipments',
    dag = dag,
    python_callable=Transform_shipments,
    op_kwargs={"Name" : "shipments", "filePath": "Shipments.csv"}
)

transform_locations_psql = PythonOperator(
    task_id = 'Transform_locations',
    dag = dag,
    python_callable=Transform_locations,
    op_kwargs={"Name" : "locations", "filePath": ""}
)

load_psql = PythonOperator(
    task_id = 'Load_psql',
    dag = dag,
    python_callable=Load_schema
)

load_to_s3 = PythonOperator(
    task_id='Load_psql_S3',
    dag=dag,
    python_callable=ETL_s3
)

Create_redshift_schema = PythonOperator(
    task_id='Create_redshift_schema',
    dag=dag,
    python_callable=create_redshift_schema,
    op_kwargs= {'root_dir' : '/home/truong/airflow/dags/project_batch/Create_Redshift_schema'}
)

load_s3_redshift = PythonOperator(
    task_id='load_s3_to_redshift',
    dag=dag,
    python_callable=Load_s3_to_redshift
)
create_psql_schema >> [transform_customers_psql, transform_locations_psql, transform_products_psql,
                       transform_sales_psql, transform_shipments_psql] >> load_psql >> load_to_s3 >> Create_redshift_schema >> load_s3_redshift