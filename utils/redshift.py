
from airflow.hooks.base_hook import BaseHook
import pandas_redshift as pr
from config.variable import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET


def insert_data(table_name, df, column_data_types=None):

    pr.connect_to_redshift(
        dbname=BaseHook.get_connection('redshift-dw').schema,
        host=BaseHook.get_connection('redshift-dw').host,
        port=BaseHook.get_connection('redshift-dw').port,
        user=BaseHook.get_connection('redshift-dw').login,
        password=BaseHook.get_connection('redshift-dw').password)

    pr.connect_to_s3(aws_access_key_id=AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                     bucket=AWS_S3_BUCKET,
                     subdirectory='temp')

    pr.pandas_to_redshift(data_frame=df,
                          redshift_table_name=table_name,
                          column_data_types=column_data_types)


def insert_data_by_records(hook, table_name, records, chunksize=10000):
    hook.insert_rows(table=table_name, rows=records, commit_every=chunksize)


def s3_parquet_to_redshift(hook, table_name, file_name):
    sql = f"""
        COPY {table_name}
        FROM 's3://{AWS_S3_BUCKET}/temp/{file_name}'
        credentials
        'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}'
        FORMAT PARQUET;
    """
    hook.run(sql)
    

def s3_json_to_redshift(hook, table_name, file_name):
    sql = f"""
        COPY {table_name}
        FROM 's3://{AWS_S3_BUCKET}/temp/{file_name}'
        credentials
        'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}'
        format as json 'auto';
    """
    hook.run(sql)


def s3_csv_to_redshift(hook, table_name, file_name):
    sql = f"""
        COPY {table_name}
        FROM 's3://{AWS_S3_BUCKET}/{file_name}'
        credentials
        'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}'
        format as csv;
    """
    hook.run(sql)
