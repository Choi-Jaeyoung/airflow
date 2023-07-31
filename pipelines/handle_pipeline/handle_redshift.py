from airflow import DAG
from utils.slack_alert import SlackAlert
from datetime import datetime
from processes.handle_process.redshift_process import select_product, insert_temp_table

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
with DAG(dag_id="handle_redshift",
         # crontab 표현 사용 가능 https://crontab.guru/
         # UTC로 설정한다.
         schedule_interval=None,
         default_args=default_args,
         # 태그는 원하는대로
         tags=["jychoi"],
         # catchup을 True로 하면, start_date 부터 현재까지 못돌린 날들을 채운다
         catchup=False,
         # template_root_path 설정
         template_searchpath=['/opt/airflow/dags/scripts'],
         render_template_as_native_obj=True,) as dag:
    """
    - redshift data import
    - redshift data export
    """

    t1 = select_product()
    t2 = insert_temp_table()

    t1 >> t2
