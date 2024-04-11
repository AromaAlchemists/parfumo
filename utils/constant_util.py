from airflow.models.variable import Variable

from datetime import datetime, timedelta
import os

BUCKET_NAME = Variable.get("BUCKET_NAME")
AIRFLOW_HOME = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
DOWNLOADS_DIR = os.path.join(AIRFLOW_HOME, 'downloads')
TRANSFORM_DIR = os.path.join(AIRFLOW_HOME, 'transform')
NOW_DATE = datetime.now().strftime("%Y-%m-%d")