from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable

import logging
import boto3
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import os
from datetime import datetime, timedelta

AIRFLOW_HOME = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
DOWNLOADS_DIR = os.path.join(AIRFLOW_HOME, 'downloads')
TRANSFORM_DIR = os.path.join(AIRFLOW_HOME, 'transform')
# NOW_DATE = datetime.now().strftime("%Y-%m-%d")
NOW_DATE = '2024-04-12'

# RDS
BUCKET_NAME = Variable.get("BUCKET_NAME")
HOST = Variable.get("HOST")
USER = Variable.get("USER")
PASSWORD = Variable.get('PASSWORD')
DB = Variable.get("DB")

def inserting_csv_to_db():
    tables = ["accord","chart","chart_feature","note","perfume"]
    for table in tables:
        # file_name = 'transform/accord/2024-04-12/2024-04-12_accord.csv'
        file_name = os.path.join(TRANSFORM_DIR,f"{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv")
        # S3에 접근하기 위한 클라이언트 생성
        s3 = boto3.client('s3')

        # S3에서 파일 다운로드
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
        df = pd.read_csv(obj['Body'])

        # 데이터베이스 연결 URI 설정
        db_connection_str = f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}'
        db_connection = create_engine(db_connection_str)

        # DataFrame을 MySQL 테이블에 저장
        df.to_sql(table , con=db_connection, if_exists='replace', index=False)
        logging.info(f"Inserting {table} to DB successfully!")

with DAG(dag_id="transform_parfumo_to_s3_dag",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )  
   
    inserting_csv_to_db_task = PythonOperator(
        task_id = "inserting_csv_to_db_task",
        python_callable=inserting_csv_to_db
    )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    start_task >> inserting_csv_to_db_task >> end_task