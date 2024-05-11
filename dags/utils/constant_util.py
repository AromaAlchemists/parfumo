from airflow.models.variable import Variable

from datetime import datetime, timedelta
import os

BUCKET_NAME = Variable.get("BUCKET_NAME")
AIRFLOW_HOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
DOWNLOADS_DIR = os.path.join(AIRFLOW_HOME, 'downloads')
TRANSFORM_DIR = os.path.join(AIRFLOW_HOME, 'transform')
NOW_DATE = datetime.now().strftime("%Y-%m-%d")

# RDS
BUCKET_NAME = Variable.get("BUCKET_NAME")
HOST = Variable.get("HOST")
USER = Variable.get("USER")
PASSWORD = Variable.get('PASSWORD')
DB = Variable.get("DB")

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

total_main_accords = []
total_top_notes = []
total_heart_notes = []
total_base_notes = []

perfumers = []

ratings_list = []
ratings_count_list = []

gender_list = []

description_list = [] 

scent_rating_list = []
longevity_rating_list = []
sillage_rating_list = []
bottle_rating_list = []
vfm_rating_list = []

scent_rating_count_list = []
longevity_rating_count_list = []
sillage_rating_count_list = []
bottle_rating_count_list = []
vfm_rating_count_list = []

year_list= []

main_accords = []
top_notes = []
heart_notes = []
base_notes = []
gender = []

# HOST = 'database-2.ctoyoyo6m5ql.ap-northeast-2.rds.amazonaws.com'  # RDS ARN
# USER = 'admin'
# PASSWORD = 'Qkrwodnr12!'
# DB = 'parfumo'