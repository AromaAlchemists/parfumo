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
import json
from utils.constant_util import *
from utils import common_util
# 항상 transform은 크롤링 완성본인 review 파일로 진행

def transform_chart(df):
    #df = common_util.concat_csv()
    df = df[['perfume_id','Type','Occasion','Season','Audience']].T

    columns = ['perfume_id','name','vote']
    dic = {column : [] for column in columns}
    for i in range(len(df.columns)):
        dic['vote'] += df.iloc[1:,i].tolist()
        dic['perfume_id'] += [df.iloc[0,i]] * 4
    dic['name'] = list(df.index)[1:] * len(df.columns)

    transform_df = pd.DataFrame(dic)  
    transform_df.insert(0, 'chart_id', range(1, len(transform_df) + 1))

    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'chart/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, f'transform_chart.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)


def transform_chart_feature(df):
    #df = common_util.concat_csv()
    df = df[['perfume_id','Season', 'Spring', 'Summer', 'Fall',
    'Winter', 'Audience','Youthful',
    'Mature', 'Feminine', 'Masculine', 'Occasion','Evening', 'Business', 'Night Out', 'Leisure',
    'Sport', 'Daily', 'Type','Animal', 'Aquatic', 'Floral', 'Chypre',
    'Creamy', 'Earthy', 'Fougère', 'Fresh', 'Fruity', 'Gourmand', 'Green',
    'Resinous', 'Woody', 'Leathery', 'Oriental', 'Powdery', 'Smoky',
    'Sweet', 'Synthetic', 'Spicy', 'Citrus']]

    df.drop(['Season','Audience', 'Type', 'Occasion'], axis=1, inplace = True)
    df = df.T

    dic = {column : [] for column in ['perfume_id' , 'name','percentage']}
    for i in range(len(df.columns)):
        dic['percentage'] += df.iloc[1:,i].tolist()
        dic['perfume_id'] += ([df.iloc[0,i]] * (len(df.index) - 1))
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
    transform_df.insert(0, 'chart_feature_id', range(1, len(transform_df) + 1))

    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'chart_feature/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, 'transform_chart_feature.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)



def transform_notes(df):
    #df = common_util.concat_csv()
    df = df[['perfume_id','top_notes','heart_notes','base_notes']]

    df['top_notes'] = df['top_notes'].apply(ast.literal_eval)
    df['heart_notes'] = df['heart_notes'].apply(ast.literal_eval)
    df['base_notes'] = df['base_notes'].apply(ast.literal_eval)

    columns = ['perfume_id', 'note', 'name']
    type_dict = {column : [] for column in columns}
    transform_df = pd.DataFrame(columns = columns)
    for row in range(len(df.index)):
        dic = dict(df.iloc[row,1:])
        for key in list(dic.keys()):
            type_dict['name'] += dic[key]
            type_dict['note'] += ([key] * len(dic[key]))
            type_dict['perfume_id'] = [df.iloc[row,0]] * len(type_dict['name'])
        temp_df = pd.DataFrame(type_dict)
        transform_df = pd.concat([transform_df, temp_df])
    transform_df.insert(0, 'note_id', range(1, len(transform_df) + 1))

    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'note/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, 'transform_note.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)


def transform_rating(df):
    #df = common_util.concat_csv()
    df = df[['perfume_id' , 'scent', 'longevity', 'sillage', 'bottle', 'vfm', 
        'scent_count', 'longevity_count', 'sillage_count', 'bottle_count', 'vfm_count']]
    
    columns = ['perfume_id' ,'name', 'rating', 'vote']
    rating_dict = {column : [] for column in columns}
    transform_df = pd.DataFrame(columns = columns)
    for row in range(len(df.index)):
        temp_dict = dict(df.iloc[row,1:6])

        rating_dict['name'] += list(temp_dict.keys())
        rating_dict['rating'] += list(temp_dict.values())
        rating_dict['vote'] += list(df.iloc[row,6:])
        rating_dict['perfume_id'] += ([df.iloc[row,0]] * 5)
        
    transform_df = pd.DataFrame(rating_dict)
    transform_df.insert(0, 'rating_id', range(1, len(transform_df) + 1))

    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'rating/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, 'transform_rating.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)

def transform_accord(df):
    #df = common_util.concat_csv()
    df = df[['perfume_id','main_accords']]
    df['main_accords'] = df['main_accords'].apply(ast.literal_eval)

    columns = ['perfume_id', 'name']
    transform_df = pd.DataFrame(columns = columns)
    for row in range(len(df.index)):
        accord_dict = {column : [] for column in columns}
        
        accord_dict['name'] += df.iloc[row,1]
        accord_dict['perfume_id'] += ([df.iloc[row,0]] * len(list(df.iloc[row,1])))
        
        temp_df = pd.DataFrame(accord_dict)
        transform_df = pd.concat([transform_df, temp_df])
    transform_df.insert(0, 'accord_id', range(1, len(transform_df) + 1))


    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'accord/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, 'transform_accord.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)


def transform_perfume(df):
    #df = common_util.concat_csv()
    columns = ['perfume_id' ,'brand','gender','rating', 'released_year','description', 'url', 'img_url', 'perfumers']
    df = df[columns]
    df['gender'] = df['gender'].apply(ast.literal_eval)
    df['perfumers'] = df['perfumers'].apply(ast.literal_eval)
    df=df.fillna('')
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
    transform_df = transform_df[['perfume_id' ,'brand','gender','rating', 'released_year','description', 'url', 'img_url', 'perfumer']]
    transform_df['gender'] = transform_df['gender'].apply(lambda x: x[0])
    
    dst_dir_path =  os.path.join(TRANSFORM_DIR, f'perfume/{NOW_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_path = os.path.join(dst_dir_path, 'transform_perfume.csv')

    transform_df.to_csv(dst_path, encoding='utf-8-sig',index=False)

def transform_review(df):
    path = os.path.join(DOWNLOADS_DIR, f'review/{NOW_DATE}/2020')
    json_files = glob.glob(os.path.join(path,"*.json"))
    ref_df = df
    columns = ['perfume_id' , 'date','title','contents']
    concat_df = pd.DataFrame(columns = columns)
    for json_file in json_files:
        with open(json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
            perfume_name = list(data.keys())[0]
            reviews = data[list(data.keys())[0]]['reviews']
            df = pd.DataFrame(reviews, columns = ['date','title','contents'])
            perfume_id = list(ref_df[ref_df['perfume'] == perfume_name]['perfume_id'])[0]
            df.insert(0, 'perfume_id', perfume_id * len(df))

            concat_df = pd.concat([concat_df, df])
            
    concat_df.insert(0, 'review_id', range(1, len(concat_df) + 1))        
    src_dir_path = os.path.join(TRANSFORM_DIR, f'review/{NOW_DATE}')
    os.makedirs(src_dir_path, exist_ok=True)

    src_path = os.path.join(src_dir_path, f'transform_review.csv')
    concat_df.to_csv(src_path, encoding= 'utf-8', index = False)

def upload_transform_files_to_s3(bucket_name: str) -> None:
    hook = S3Hook(aws_conn_id="aws_s3")
    accord_src_path = os.path.join(TRANSFORM_DIR, 'accord', NOW_DATE)
    chart_src_path = os.path.join(TRANSFORM_DIR, 'chart', NOW_DATE)
    chart_feature_src_path = os.path.join(TRANSFORM_DIR, 'chart_feature', NOW_DATE)
    note_src_path = os.path.join(TRANSFORM_DIR, 'note', NOW_DATE)
    perfume_src_path = os.path.join(TRANSFORM_DIR, 'perfume', NOW_DATE)
    review_src_path = os.path.join(TRANSFORM_DIR, 'review', NOW_DATE)
    rating_src_path = os.path.join(TRANSFORM_DIR, 'rating', NOW_DATE)

    src_paths = [accord_src_path, chart_src_path, chart_feature_src_path, note_src_path, perfume_src_path, review_src_path, rating_src_path]
    for src_path in src_paths:
        src_files = glob.glob(os.path.join(src_path, '*.csv'))
        if not src_files:
            logging.warning(f"No CSV files found in {src_path}")
        for src_file in src_files:
            key = src_file.replace(TRANSFORM_DIR, '').lstrip('/')
            key = os.path.join('transform', key)
            try:
                hook.load_file(filename=src_file, key=key, replace=True, bucket_name=bucket_name)
                logging.info(f"Successfully uploaded {src_file} to {key} in bucket {bucket_name}.")
            except Exception as e:
                logging.error(f"Failed to upload {src_file} to {key} in bucket {bucket_name}: {e}")





with DAG(dag_id="transform_parfumo_to_s3_dag",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    transform_chart_task = PythonOperator(
        task_id = "transform_chart_task",
        python_callable=transform_chart,
        op_kwargs= {
            "df": common_util.concat_csv()
        }
    )

    transform_chart_feature_task = PythonOperator(
        task_id = "transform_chart_feature_task",
        python_callable=transform_chart_feature,
        op_kwargs= {
            "df": common_util.concat_csv()
        }
    )

    transform_notes_task = PythonOperator(
        task_id = "transform_notes_task",
        python_callable=transform_notes,
        op_kwargs= {
            "df": common_util.concat_csv()
        }
    )
    
    transform_rating_task = PythonOperator(
        task_id = "transform_rating_task",
        python_callable=transform_rating,
        op_kwargs= {
            "df": common_util.concat_csv()
        }
    )

    transform_accord_task = PythonOperator(
        task_id = "transform_accord_task",
        python_callable=transform_accord,
        op_kwargs= {
            "df": common_util.concat_csv()
        }
    )

    transform_perfume_task = PythonOperator(
        task_id = "transform_perfume_task",
        python_callable=transform_perfume,
        op_kwargs= {
            "df": common_util.concat_csv()
        }
    )

    transform_review_task = PythonOperator(
        task_id = "transform_review_task",
        python_callable=transform_review,
        op_kwargs= {
            "df": common_util.concat_csv()
        }
    )

    
    upload_transform_files_to_s3_task = PythonOperator(
        task_id = "upload_transform_files_to_s3_task",
        python_callable= upload_transform_files_to_s3,
        op_kwargs= {
            "bucket_name": BUCKET_NAME
        }
    )

    call_trigger_task = TriggerDagRunOperator(
        task_id='call_trigger',
        trigger_dag_id='s3_to_rds_dag',
        trigger_run_id=None,
        execution_date=None,
        reset_dag_run=False,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=None,
    )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    start_task >> [transform_chart_task, transform_chart_feature_task, transform_notes_task, transform_rating_task,
    
    transform_rating_task, transform_accord_task, transform_perfume_task, transform_review_task] >> call_trigger_task >> upload_transform_files_to_s3_task >> end_task