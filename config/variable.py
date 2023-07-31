from airflow.models import Variable

PATH_DAGS = Variable.get("PATH_DAGS")
PATH_SCRIPTS = PATH_DAGS + 'scripts/'

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_REGION = Variable.get("AWS_REGION")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = Variable.get("AWS_S3_BUCKET")

AIRFLOW_URL = Variable.get('AIRFLOW_URL')
