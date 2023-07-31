from airflow.hooks.base_hook import BaseHook
from airflow.operators.slack_operator import SlackAPIPostOperator
from config.variable import AIRFLOW_URL

import pendulum


class SlackAlert:
    def __init__(self):
        self.slack_channel = 'C05848S2P4J'
        self.slack_token = BaseHook.get_connection('slack').password
        self.timezone = pendulum.timezone("Asia/Seoul")

    def slack_fail_alert(self, context):
        alert = SlackAPIPostOperator(
            task_id='slack_failed',
            channel=self.slack_channel,
            token=self.slack_token,
            text=f"""
                :red_circle: Airflow 작업 실패
*Dag*: {context.get('task_instance').dag_id}
*Task*: {context.get('task_instance').task_id}
*Execution Time*: {context.get('logical_date').in_timezone(self.timezone).strftime("%Y/%m/%d, %H:%M:%S")}, KST
*Log Url*: {str(context.get('task_instance').log_url).replace('http://localhost:8080', AIRFLOW_URL)}
                """
        )
        return alert.execute(context=context)
