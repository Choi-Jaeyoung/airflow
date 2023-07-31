
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from config.variable import PATH_SCRIPTS


def _select_data(**context):
    ti = context['ti']

    # sql
    sql = open(PATH_SCRIPTS + 'handle_scripts/mssql_scripts.sql').read()

    # mssql
    mssql = MsSqlHook(mssql_conn_id="db_test2", schema="riman")
    df = mssql.get_pandas_df(sql)

    ti.xcom_push(key='df', value=df.values.tolist())
    return 1


def select_tbl_memberinfo():
    return PythonOperator(
        task_id="select_tbl_memberinfo",
        provide_context=True,
        python_callable=_select_data  # 실행할 파이썬 함수
    )


def _insert_data(**context):
    ti = context['ti']

    df = ti.xcom_pull(task_ids=['select_tbl_memberinfo'],
                      key='df')

    mssql = MsSqlHook(mssql_conn_id="db_test2", schema="riman")

    rows = list(df)[0]
    target_fields = ["test_number", "test_varchar"]  # 열 이름
    mssql.insert_rows(table="tbl_test",
                      rows=rows,
                      target_fields=target_fields)
    
    return 1


def insert_tbl_test():
    return PythonOperator(
        task_id="insert_tbl_test",
        provide_context=True,
        python_callable=_insert_data  # 실행할 파이썬 함수
    )
