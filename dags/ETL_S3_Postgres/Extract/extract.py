import boto3
import os
import configparser

config = configparser.ConfigParser()
config.read_file(open('/home/truong/airflow/dags/project_batch/config.cfg')) # sua path
KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')

def Extract_from_s3():
    session = boto3.Session(
        aws_access_key_id= KEY,
        aws_secret_access_key= SECRET
    )

    s3 = session.client("s3")
    bucket_name = 'amazon-us-sale-bucket'

    # List all object in bucket
    response = s3.list_objects_v2(Bucket = bucket_name)

    write_dir = "project_batch/data"
    for obj in response['Contents'] :
        key = obj['Key']

        write_path = os.path.join(write_dir, key)
        with open(write_path, "wb") as file:
            s3.download_fileobj(bucket_name, key, file)