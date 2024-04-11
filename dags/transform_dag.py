from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable

from urllib.request import Request, urlopen
import time
import pandas as pd
from typing import List, Tuple
import logging
import os
from datetime import datetime, timedelta
import glob
import ast
from urllib.parse import quote
from utils.constant_util import *
from utils import common_util

# 항상 transform은 크롤링 완성본인 review 파일로 진행


def transform_chart():
    file_path = os.path.join(DOWNLOADS_DIR, f'review/{NOW_DATE}')
    filename = glob.glob(os.path.join(file_path,'*.csv'))
    df = pd.read_csv(filename[0])

    df = df[['perfume','Type','Occasion','Season','Audience']].T
    
    dic = {column : [] for column in ['perfume','name','vote']}
    for i in range(len(df.columns)):
        dic['vote'] += df.iloc[1:,i].tolist()
        dic['perfume'] += [df.iloc[0,i]] * 4
    dic['name'] = list(df.index)[1:] * len(df.columns)
    
    transform_df = pd.DataFrame(dic)
    
    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'chart/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, f'{NOW_DATE}_chart.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)


def transform_chart_feature():
    file_path = os.path.join(DOWNLOADS_DIR, f'review/{NOW_DATE}')
    filename = glob.glob(os.path.join(file_path,'*.csv'))
    df = pd.read_csv(filename[0])  
    df = df[['perfume','Season', 'Spring', 'Summer', 'Fall',
       'Winter', 'Audience','Youthful',
       'Mature', 'Feminine', 'Masculine', 'Occasion','Evening', 'Business', 'Night Out', 'Leisure',
       'Sport', 'Daily', 'Type','Animal', 'Aquatic', 'Floral', 'Chypre',
       'Creamy', 'Earthy', 'Fougère', 'Fresh', 'Fruity', 'Gourmand', 'Green',
       'Resinous', 'Woody', 'Leathery', 'Oriental', 'Powdery', 'Smoky',
       'Sweet', 'Synthetic', 'Spicy', 'Citrus']]
    
    df.drop(['Season','Audience', 'Type', 'Occasion'], axis=1, inplace = True)
    df = df.T
    
    dic = {column : [] for column in ['name','percentage']}
    for i in range(len(df.columns)):
        dic['percentage'] += df.iloc[1:,i].tolist()
    dic['name'] = list(df.index)[1:] * len(df.columns)
    
    transform_df = pd.DataFrame(dic)
    
    season_list = ['Spring', 'Summer', 'Fall','Winter']
    audience_list = ['Youthful','Mature', 'Feminine', 'Masculine']
    occasion_list = ['Evening', 'Business', 'Night Out', 'Leisure','Sport', 'Daily']

    lst = []
    for name in list(transform_df['name']):
        if name in season_list:
            lst.append('Season')
        elif name in audience_list:
            lst.append('Audience')
        elif name in occasion_list:
            lst.append('Occasion')
        else:
            lst.append('Occasion')
    transform_df['parent'] = lst

    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'chart_feature/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, f'{NOW_DATE}_chart_feature.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)



def transform_notes():
    file_path = os.path.join(DOWNLOADS_DIR, f'review/{NOW_DATE}')
    filename = glob.glob(os.path.join(file_path,'*.csv'))
    df = pd.read_csv(filename[0])
    df = df[['perfume','top_notes','heart_notes','base_notes']]
    
    df['top_notes'] = df['top_notes'].apply(ast.literal_eval)
    df['heart_notes'] = df['heart_notes'].apply(ast.literal_eval)
    df['base_notes'] = df['base_notes'].apply(ast.literal_eval)
    
    type_dict = {column : [] for column in ['perfume', 'note', 'name']}
    columns = ['perfume', 'note', 'name']
    transform_df = pd.DataFrame(columns = columns)
    for row in range(len(df.index)):
        dic = dict(df.iloc[row,1:])
        for key in list(dic.keys()):
            type_dict['name'] += dic[key]
            type_dict['note'] += ([key] * len(dic[key]))
            type_dict['perfume'] = [df.iloc[row,0]] * len(type_dict['name'])
        temp_df = pd.DataFrame(type_dict)
        transform_df = pd.concat([transform_df, temp_df])

    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'note/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, f'{NOW_DATE}_note.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)


def transform_rating():
    file_path = os.path.join(DOWNLOADS_DIR, f'review/{NOW_DATE}')
    filename = glob.glob(os.path.join(file_path,'*.csv'))
    df = pd.read_csv(filename[0])
    df = df[['perfume', 'scent', 'longevity', 'sillage', 'bottle', 'vfm', 
         'scent_count', 'longevity_count', 'sillage_count', 'bottle_count', 'vfm_count']]
    
    columns = ['perfume' ,'name', 'rating', 'vote']
    rating_dict = {column : [] for column in columns}
    transform_df = pd.DataFrame(columns = columns)
    for row in range(len(df.index)):
        temp_dict = dict(df.iloc[row,1:6])

        rating_dict['name'] += list(temp_dict.keys())
        rating_dict['rating'] += list(temp_dict.values())
        rating_dict['vote'] += list(df.iloc[row,6:])
        rating_dict['perfume'] += ([df.iloc[row,0]] * 5)
        
    transform_df = pd.DataFrame(rating_dict)

    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'note/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, f'{NOW_DATE}_note.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)

def transform_accord():
    file_path = os.path.join(DOWNLOADS_DIR, f'review/{NOW_DATE}')
    filename = glob.glob(os.path.join(file_path,'*.csv'))
    df = pd.read_csv(filename[0])
    df = df[['perfume','main_accords']]
    df['main_accords'] = df['main_accords'].apply(ast.literal_eval)

    columns = ['perfume', 'name']
    accord_dict = {column : [] for column in columns}
    transform_df = pd.DataFrame(columns = columns)
    for row in range(len(df.index)):
        accord_dict['name'] += df.iloc[row,1]
        accord_dict['perfume'] += ([df.iloc[row,0]] * len(list(df.iloc[row,1])))

        temp_df = pd.DataFrame(accord_dict)
        transform_df = pd.concat([transform_df, temp_df])

    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'accord/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, f'{NOW_DATE}_accord.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)


def transform_perfume():
    file_path = os.path.join(DOWNLOADS_DIR, f'review/{NOW_DATE}')
    filename = glob.glob(os.path.join(file_path,'*.csv'))
    df = pd.read_csv(filename[0])
    df = df[['perfume','brand','gender','rating','description', 'url', 'img', 'perfumers']]
    df['perfumers'] = df['perfumers'].apply(ast.literal_eval)
    df=df.fillna('')
    
    columns = ['perfume','brand','gender','rating','description', 'url', 'img']
    transform_df = pd.DataFrame(columns = columns)
    for row in range(len(df.index)):
        if df.iloc[row]['perfumers'] == ['']:
            tmp_df = df[columns].iloc[[row]]
            tmp_df['perfumer'] = ''
            transform_df = pd.concat([transform_df, tmp_df])
        else:
            for perfumer in df.iloc[row]['perfumers']:
                tmp_df = df[columns].iloc[[row]]
                tmp_df['perfumer'] = perfumer
                transform_df = pd.concat([transform_df, tmp_df])
    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'perfume/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, f'{NOW_DATE}_perfume.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)


def upload_transform_files_to_s3(bucket_name: str) -> None:
    hook = S3Hook(aws_conn_id="aws_s3")
    accord_src_path = os.path.join(TRANSFORM_DIR, f'perfume_product/{NOW_DATE}')
    chart_src_path = os.path.join(TRANSFORM_DIR, f'perfume_detail/{NOW_DATE}')
    chart__feature_src_path = os.path.join(TRANSFORM_DIR, f'review/{NOW_DATE}')
    note_src_path = os.path.join(TRANSFORM_DIR, f'review/{NOW_DATE}')
    review_src_path = os.path.join(TRANSFORM_DIR, f'review/{NOW_DATE}')

    src_paths = [accord_src_path, chart_src_path, chart__feature_src_path, note_src_path, review_src_path]
    for src_path in src_paths:
        src_files = glob.glob(os.path.join(src_path, '*.json'))
        for src_file in src_files:
            key = src_file.replace(TRANSFORM_DIR, '')     
            key = os.path.join('transform', key[1:])    #s3 경로에 맞게 경로명 수정
            hook.load_file(filename=src_file, key=key, replace=True, bucket_name=bucket_name)




with DAG(dag_id="spotify_chart_to_s3",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    transform_chart_task = PythonOperator(
        task_id = "transform_chart_task",
        python_callable=transform_chart
    )

    transform_chart_feature_task = PythonOperator(
        task_id = "transform_chart_feature_task",
        python_callable=transform_chart_feature
    )

    transform_notes_task = PythonOperator(
        task_id = "transform_notes_task",
        python_callable=transform_notes
    )
    
    transform_rating_task = PythonOperator(
        task_id = "transform_rating_task",
        python_callable=transform_rating
    )

    transform_accord_task = PythonOperator(
        task_id = "transform_accord_task",
        python_callable=transform_accord
    )

    transform_perfume_task = PythonOperator(
        task_id = "transform_perfume_task",
        python_callable=transform_perfume
    )

    
    upload_transform_files_to_s3_task = PythonOperator(
        task_id = "upload_transform_files_to_s3_task",
        python_callable= upload_transform_files_to_s3,
        op_kwargs= {
            "bucket_name": BUCKET_NAME
        }
    )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    start_task >> transform_chart_task >> transform_chart_feature_task >> transform_notes_task >> transform_rating_task
    
    transform_rating_task >> transform_accord_task >> transform_perfume_task >> upload_transform_files_to_s3_task >> end_task