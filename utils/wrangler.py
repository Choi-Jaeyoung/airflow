import awswrangler as wr
from config.boto3_config import getSession
from config.variable import AWS_S3_BUCKET


def getParquetData(folder_name='',
                   file_name=''):
    
    file_path = f's3://{AWS_S3_BUCKET}/{folder_name}/{file_name}'

    return wr.s3.read_parquet(path=file_path,
                              boto3_session=getSession())


def toFileFromDataFrame(df,
                        folder_name='',
                        file_name=''):
    
    file_path = f's3://{AWS_S3_BUCKET}/{folder_name}/{file_name}'

    # append 기본값
    wr.s3.to_parquet(df=df,
                     path=file_path,
                     dataset=True,
                     mode='append',
                     boto3_session=getSession())

    # overwrite 덮어쓰기
    # wr.s3.to_parquet(df=df,
    #                  path=file_path,
    #                  dataset=True,
    #                  mode='overwrite',
    #                  boto3_session=getSession())
