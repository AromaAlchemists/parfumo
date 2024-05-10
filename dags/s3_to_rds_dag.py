from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable

import glob
import logging
import boto3
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import os
from datetime import datetime, timedelta
from io import StringIO
from utils.constant_util import *


def accord_to_db():
    hook = S3Hook('aws_s3')

    table = 'accord'
    file_name = f'transform/{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv'
    #file_name = os.path.join(TRANSFORM_DIR,f"{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv")[2:]

    # S3에서 데이터를 문자열로 읽어옴
    data = hook.read_key(file_name, BUCKET_NAME)
    
    # 문자열 데이터를 StringIO 객체로 변환하고 pandas DataFrame으로 읽음
    data_string = StringIO(data)
    df = pd.read_csv(data_string)

    # 데이터베이스 연결 URI 설정
    connection_string = f"mysql+mysqlconnector://{USER}:{PASSWORD}@host.docker.internal:3306/{DB}"
    db_connection = create_engine(connection_string)

    # DataFrame을 MySQL 테이블에 저장
    df.to_sql(table , con=db_connection, if_exists='replace', index=False)

def chart_to_db():
    hook = S3Hook('aws_s3')

    table = 'chart'
    file_name = f'transform/{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv'
    #file_name = os.path.join(TRANSFORM_DIR,f"{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv")[2:]

    # S3에서 데이터를 문자열로 읽어옴
    data = hook.read_key(file_name, BUCKET_NAME)
    
    # 문자열 데이터를 StringIO 객체로 변환하고 pandas DataFrame으로 읽음
    data_string = StringIO(data)
    df = pd.read_csv(data_string)

    # 데이터베이스 연결 URI 설정
    connection_string = f"mysql+mysqlconnector://{USER}:{PASSWORD}@host.docker.internal:3306/{DB}"
    db_connection = create_engine(connection_string)

    # DataFrame을 MySQL 테이블에 저장
    df.to_sql(table , con=db_connection, if_exists='replace', index=False)

def chart_feature_to_db():
    hook = S3Hook('aws_s3')

    table = 'chart_feature'
    file_name = f'transform/{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv'
    #file_name = os.path.join(TRANSFORM_DIR,f"{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv")[2:]

    # S3에서 데이터를 문자열로 읽어옴
    data = hook.read_key(file_name, BUCKET_NAME)
    
    # 문자열 데이터를 StringIO 객체로 변환하고 pandas DataFrame으로 읽음
    data_string = StringIO(data)
    df = pd.read_csv(data_string)

    # 데이터베이스 연결 URI 설정
    connection_string = f"mysql+mysqlconnector://{USER}:{PASSWORD}@host.docker.internal:3306/{DB}"
    db_connection = create_engine(connection_string)

    # DataFrame을 MySQL 테이블에 저장
    df.to_sql(table , con=db_connection, if_exists='replace', index=False)

def note_to_db():
    hook = S3Hook('aws_s3')

    table = 'note'
    file_name = f'transform/{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv'
    #file_name = os.path.join(TRANSFORM_DIR,f"{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv")[2:]

    # S3에서 데이터를 문자열로 읽어옴
    data = hook.read_key(file_name, BUCKET_NAME)
    
    # 문자열 데이터를 StringIO 객체로 변환하고 pandas DataFrame으로 읽음
    data_string = StringIO(data)
    df = pd.read_csv(data_string)

    # 데이터베이스 연결 URI 설정
    connection_string = f"mysql+mysqlconnector://{USER}:{PASSWORD}@host.docker.internal:3306/{DB}"
    db_connection = create_engine(connection_string)

    # DataFrame을 MySQL 테이블에 저장
    df.to_sql(table , con=db_connection, if_exists='replace', index=False)

def perfume_to_db():
    hook = S3Hook('aws_s3')

    table = 'perfume'
    file_name = f'transform/{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv'
    #file_name = os.path.join(TRANSFORM_DIR,f"{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv")[2:]

    # S3에서 데이터를 문자열로 읽어옴
    data = hook.read_key(file_name, BUCKET_NAME)
    
    # 문자열 데이터를 StringIO 객체로 변환하고 pandas DataFrame으로 읽음
    data_string = StringIO(data)
    df = pd.read_csv(data_string)

    # 데이터베이스 연결 URI 설정
    connection_string = f"mysql+mysqlconnector://{USER}:{PASSWORD}@host.docker.internal:3306/{DB}"
    db_connection = create_engine(connection_string)

    # DataFrame을 MySQL 테이블에 저장
    df.to_sql(table , con=db_connection, if_exists='replace', index=False)

def rating_to_db():
    hook = S3Hook('aws_s3')

    table = 'rating'
    file_name = f'transform/{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv'
    #file_name = os.path.join(TRANSFORM_DIR,f"{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv")[2:]

    # S3에서 데이터를 문자열로 읽어옴
    data = hook.read_key(file_name, BUCKET_NAME)
    
    # 문자열 데이터를 StringIO 객체로 변환하고 pandas DataFrame으로 읽음
    data_string = StringIO(data)
    df = pd.read_csv(data_string)

    # 데이터베이스 연결 URI 설정
    connection_string = f"mysql+mysqlconnector://{USER}:{PASSWORD}@host.docker.internal:3306/{DB}"
    db_connection = create_engine(connection_string)

    # DataFrame을 MySQL 테이블에 저장
    df.to_sql(table , con=db_connection, if_exists='replace', index=False)

def review_to_db():
    hook = S3Hook('aws_s3')

    table = 'review'
    #file_name = f'transform/{table}/{NOW_DATE}/{NOW_DATE}_{table}.csv'

    concat_df = pd.DataFrame(columns = ['perfume_id','date','title','contents'])
    path = f'transform/{table}/{NOW_DATE}/*.csv'
    file_paths = glob.glob(path)
    for file_path in file_paths:    
        # S3에서 데이터를 문자열로 읽어옴
        data = hook.read_key(file_path, BUCKET_NAME)
        
        # 문자열 데이터를 StringIO 객체로 변환하고 pandas DataFrame으로 읽음
        data_string = StringIO(data)
        df = pd.read_csv(data_string)

        concat_df = pd.concat([concat_df,df])

    # 데이터베이스 연결 URI 설정
    connection_string = f"mysql+mysqlconnector://{USER}:{PASSWORD}@host.docker.internal:3306/{DB}"
    db_connection = create_engine(connection_string)

    # DataFrame을 MySQL 테이블에 저장
    concat_df.to_sql(table , con=db_connection, if_exists='replace', index=False)




with DAG(dag_id="s3_to_rds_dag",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )  
   
    accord_to_db_task = PythonOperator(
        task_id = "accord_to_db_task",
        python_callable=accord_to_db
    )

    chart_to_db_task = PythonOperator(
        task_id = "chart_to_db_task",
        python_callable=chart_to_db
    )

    chart_feature_to_db_task = PythonOperator(
        task_id = "chart_feature_to_db_task",
        python_callable=chart_feature_to_db
    )

    note_to_db_task = PythonOperator(
        task_id = "note_to_db_task",
        python_callable=note_to_db
    )

    perfume_to_db_task = PythonOperator(
        task_id = "perfume_to_db_task",
        python_callable=perfume_to_db
    )

    rating_to_db_task = PythonOperator(
        task_id = "rating_to_db_task",
        python_callable=rating_to_db
    )

    review_to_db_task = PythonOperator(
        task_id = "review_to_db_task",
        python_callable=review_to_db
    )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    start_task >> [accord_to_db_task, chart_to_db_task, chart_feature_to_db_task, note_to_db_task, perfume_to_db_task, rating_to_db_task, review_to_db_task] >> end_task