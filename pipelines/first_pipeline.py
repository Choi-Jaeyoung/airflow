from airflow import DAG
from processes.first_process import creating_table, is_api_available, crawl_api, preprocess, insert_data, print_complete

from utils.slack_alert import SlackAlert

from datetime import datetime

slack = SlackAlert()

# 디폴트 설정
default_args = {
    'owner': 'jychoi',
    'depends_on_past': False,
    # UTC로 설정한다.
    "start_date": datetime(2023, 3, 23),
    'on_success_callback': None,
    'on_failure_callback': slack.slack_fail_alert
}


# DAG 틀 설정
with DAG(dag_id="first-pipeline",
         # crontab 표현 사용 가능 https://crontab.guru/
         # UTC로 설정한다.
         # schedule_interval="@daily",
         schedule_interval=None,
         default_args=default_args,
         # 태그는 원하는대로
         tags=["jychoi"],
         # catchup을 True로 하면, start_date 부터 현재까지 못돌린 날들을 채운다
         catchup=False,
         # template_root_path 설정
         template_searchpath=['/opt/airflow/dags/scripts'],) as dag:

    t1 = creating_table()
    t2 = is_api_available()
    t3 = crawl_api()
    t4 = preprocess()
    t5 = insert_data()
    t6 = print_complete()

    # 파이프라인 구성하기
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
