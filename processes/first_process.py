import pandas as pd

# from airflow.hooks.base_hook import BaseHook

# 사용할 Operator Import
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.bash import BashOperator


def _preprocessing(ti):
    # ti(task instance) - dag 내의 task의 정보를 얻어 낼 수 있는 객체

    # xcom(cross communication) - Operator와 Operator 사이에 데이터를 전달할 수 있게끔 하는 도구
    api_result = ti.xcom_pull(task_ids=['crawl_api'], key='return_value')

    # xcom을 이용해 가지고 온 결과가 없는 경우
    if not len(api_result):
        raise ValueError('검색 결과 없음')

    ti.xcom_push(key='pd_data', value=api_result[0])


def _insert_data(ti):

    data = {'title': ['주소1', '주소22'], 'address': ['타이틀123', '타이드드']}
    df = pd.DataFrame.from_dict(data)
    print(df)


def _complete():
    print(" DAG 완료")


def creating_table():
    return MySqlOperator(
        task_id="creating_table",
        mysql_conn_id="db_test",  # 웹UI에서 connection을 등록해줘야 함.
        sql="first_sql.sql"
        # sql='''
        #     CREATE TABLE IF NOT EXISTS airflow_result(
        #         title TEXT,
        #         address TEXT21
        #     )
        # '''
    )


def is_api_available():
    """_summary_
    HTTP 센서를 이용해 응답 확인 (감지하는 오퍼레이터로 실제 데이터를 가져오는 것 X)
    Returns:
        _type_: HttpSensor
    """
    return HttpSensor(
        task_id="is_api_available",
        http_conn_id="api_test",
        endpoint="/v1/point/backoffice/order/consumer-point/consumers",
        # url - uri에서 Host 부분을 제외한 파트(~.com 까지가 host)
        # 요청 헤더, -H 다음에 오는 내용들
        # headers={
        #     "X-Naver-Client-Id": f"{NAVER_CLI_ID}",
        #     "X-Naver-Client-Secret": f"{NAVER_CLI_SECRET}",
        # },
        request_params={
            'startMonth': '2023-01',
            'endMonth': '2023-03',
            'pageSize': 20,
            'pageNumber': 1
        },  # 요청 변수
        method="GET",  # 통신 방식 GET, POST 등등 맞는 것으로
        response_check=lambda response: response  # 응답 확인
    )


def crawl_api():
    """_summary_
    api 결과를 가져올 오퍼레이터를 만든다.
    Returns:
        _type_: SimpleHttpOperator
    """
    return SimpleHttpOperator(
        task_id="crawl_api",
        http_conn_id="api_test",
        endpoint="/v1/point/backoffice/order/consumer-point/consumers",  # url 설정
        # headers={
        #     "X-Naver-Client-Id": f"{NAVER_CLI_ID}",
        #     "X-Naver-Client-Secret": f"{NAVER_CLI_SECRET}",
        # },  # 요청 헤더
        data={
            'startMonth': '2023-01',
            'endMonth': '2023-03',
            'pageSize': 20,
            'pageNumber': 1
        },  # 요청 변수
        method="GET",  # 통신 방식 GET, POST 등등 맞는 것으로
        response_filter=lambda res: res.json(),
        log_response=True
    )


def preprocess():
    """_summary_
    결과 전처리
    Returns:
        _type_: PythonOperator
    """
    return PythonOperator(
        task_id="preprocess",
        python_callable=_preprocessing  # 실행할 파이썬 함수
    )


def insert_data():
    """_summary_
    타겟 스토리지에 데이터 삽입
    Returns:
        _type_: PythonOperator
    """
    return PythonOperator(
        task_id="insert_data",
        python_callable=_insert_data  # 실행할 파이썬 함수
    )


def print_complete():
    """_summary_

    Returns:
        _type_: PythonOperator
    """
    return PythonOperator(
        task_id="print_complete",
        python_callable=_complete  # 실행할 파이썬 함수
    )
