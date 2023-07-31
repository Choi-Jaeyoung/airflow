from airflow import DAG
from utils.slack_alert import SlackAlert
from datetime import datetime
from processes.handle_process.http_process import is_api_available, crawl_api

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
with DAG(dag_id="handle_http",
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
    """
    - http 통신 가능 여부
    - http 통신 
    """

    t1 = is_api_available()
    t2 = crawl_api()

    t1 >> t2
