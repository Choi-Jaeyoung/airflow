
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from config.variable import PATH_SCRIPTS


def _select_data(**context):
    ti = context['ti']

    # sql
    sql = open(PATH_SCRIPTS + 'handle_scripts/mysql_scripts.sql').read()

    # mssql
    mysql = MySqlHook(mysql_conn_id="db_test", schema="temp_us_riman")
    df = mysql.get_pandas_df(sql)

    ti.xcom_push(key='df', value=df.values.tolist())
    return 1


def select_global_consumer():
    return PythonOperator(
        task_id="select_global_consumer",
        provide_context=True,
        python_callable=_select_data  # 실행할 파이썬 함수
    )


def _insert_data(**context):
    ti = context['ti']

    df = ti.xcom_pull(task_ids=['select_global_consumer'],
                      key='df')

    mysql = MySqlHook(mysql_conn_id="db_test", schema="temp_us_riman")

    rows = list(df)[0]
    target_fields = ["title", "address"]  # 열 이름
    mysql.insert_rows(table="airflow_result",
                      rows=rows,
                      target_fields=target_fields)

    return 1


def insert_airflow_result():
    return PythonOperator(
        task_id="insert_airflow_result",
        provide_context=True,
        python_callable=_insert_data  # 실행할 파이썬 함수
    )
