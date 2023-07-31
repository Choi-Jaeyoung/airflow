from utils.wrangler import getParquetData, toFileFromDataFrame
from airflow.operators.python import PythonOperator
import pandas as pd


def _select_data(**context):
    ti = context['ti']

    # file read
    df = getParquetData('no_touch/management_planning/raw_product', 'product.parquet')
    
    ti.xcom_push(key='df_values', value=df.values.tolist())
    ti.xcom_push(key='df_columns', value=df.columns.tolist())
    return 1


def select_parquet():
    return PythonOperator(
        task_id="select_parquet",
        provide_context=True,
        python_callable=_select_data  # 실행할 파이썬 함수
    )


def _create_file(**context):
    ti = context['ti']

    df_values = ti.xcom_pull(task_ids=['select_parquet'],
                             key='df_values')
    df_columns = ti.xcom_pull(task_ids=['select_parquet'],
                              key='df_columns')
    
    rows = list(df_values)[0]
    target_fields = list(df_columns)[0]  # 열 이름

    df = pd.DataFrame(rows, columns=target_fields)

    toFileFromDataFrame(df,
                        'work',
                        'test.parquet')

    return 1


def create_parquet():
    return PythonOperator(
        task_id="create_parquet",
        provide_context=True,
        python_callable=_create_file  # 실행할 파이썬 함수
    )
