
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator


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
