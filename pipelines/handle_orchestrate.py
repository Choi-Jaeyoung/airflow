from airflow import DAG
from utils.slack_alert import SlackAlert
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

slack = SlackAlert()

# 디폴트 설정
default_args = {
    'owner': 'jychoi',
    'depends_on_past': False,
    # UTC로 설정한다.
    "start_date": datetime(2023, 6, 30),
    'on_success_callback': None,
    'on_failure_callback': slack.slack_fail_alert
}


# DAG 틀 설정
with DAG(dag_id="handle_orchestrate",
         # crontab 표현 사용 가능 https://crontab.guru/
         # UTC로 설정한다.
         schedule_interval=None,
         default_args=default_args,
         # 태그는 원하는대로
         tags=["jychoi"],
         # catchup을 True로 하면, start_date 부터 현재까지 못돌린 날들을 채운다
         catchup=False,
         # template_root_path 설정
         template_searchpath=['/opt/airflow/dags/scripts'],) as dag:

    start = DummyOperator(task_id="start")
    l0 = DummyOperator(task_id='l0')
    l1 = DummyOperator(task_id='l1')
    l2 = DummyOperator(task_id='l2')

    l0_t1 = TriggerDagRunOperator(
        trigger_dag_id='handle_http',
        task_id='handle_http',
        execution_date='{{ execution_date }}',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    l0_t2 = TriggerDagRunOperator(
        trigger_dag_id='handle_mssql',
        task_id='handle_mssql',
        execution_date='{{ execution_date }}',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    l1_t1 = TriggerDagRunOperator(
        trigger_dag_id='handle_mysql',
        task_id='handle_mysql',
        execution_date='{{ execution_date }}',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    l2_t1 = TriggerDagRunOperator(
        trigger_dag_id='handle_s3_parquet',
        task_id='handle_s3_parquet',
        execution_date='{{ execution_date }}',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    l2_t2 = TriggerDagRunOperator(
        trigger_dag_id='handle_redshift',
        task_id='handle_redshift',
        execution_date='{{ execution_date }}',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    start >> (l0_t1, l0_t2) >> l0 >> l1_t1 >> l1 >> (l2_t1, l2_t2) >> l2
