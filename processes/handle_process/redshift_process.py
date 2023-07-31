
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
from config.variable import PATH_SCRIPTS


def _select_data(**context):
    ti = context['ti']

    # sql
    sql = open(PATH_SCRIPTS + 'handle_scripts/redshift_scripts.sql').read()

    # redshift
    redshift = RedshiftSQLHook(redshift_conn_id="dw_test")
    df = redshift.get_pandas_df(sql)

    ti.xcom_push(key='df', value=df.values.tolist())
    return 1


def select_product():
    return PythonOperator(
        task_id="select_product",
        provide_context=True,
        python_callable=_select_data  # 실행할 파이썬 함수
    )


def _insert_data(**context):
    ti = context['ti']

    df = ti.xcom_pull(task_ids=['select_product'],
                      key='df')

    redshift = RedshiftSQLHook(redshift_conn_id="dw_test")

    rows = list(df)[0]
    target_fields = ["temp_number", "temp_varchar"]  # 열 이름
    redshift.insert_rows(table="test.temp_table",
                         rows=rows,
                         target_fields=target_fields)

    return 1


def insert_temp_table():
    return PythonOperator(
        task_id="insert_temp_table",
        provide_context=True,
        python_callable=_insert_data  # 실행할 파이썬 함수
    )
