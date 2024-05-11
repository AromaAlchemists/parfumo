import os
import logging
from typing import List
import pandas as pd
import glob
import json
from utils.constant_util import *

def concat_chart_feature(YEAR):
    file_path = os.path.join(DOWNLOADS_DIR, f'perfume_product/{NOW_DATE}/{YEAR}')
    filename = glob.glob(os.path.join(file_path,'*.csv'))

    df = pd.read_csv(filename[0])

    #type
    df['Type'] = 0
    df['Animal'] = 0
    df['Aquatic'] = 0
    df['Floral'] = 0
    df['Chypre'] = 0
    df['Creamy'] = 0
    df['Earthy'] = 0
    df['Fougère'] = 0
    df['Fresh'] = 0
    df['Fruity'] = 0
    df['Gourmand'] = 0
    df['Green'] = 0
    df['Resinous'] = 0
    df['Woody'] = 0
    df['Leathery'] = 0
    df['Oriental'] = 0
    df['Powdery'] = 0
    df['Smoky'] = 0
    df['Sweet'] = 0
    df['Synthetic'] = 0
    df['Spicy'] = 0
    df['Citrus'] = 0
    
    #audience
    df['Audience'] = 0
    df['Youthful'] = 0
    df['Mature'] = 0
    df['Feminine'] = 0
    df['Masculine'] = 0
    
    #season
    df['Season'] = 0
    df['Spring'] = 0
    df['Summer'] = 0
    df['Fall'] = 0
    df['Winter'] = 0
    
    #occasion
    df['Occasion'] = 0
    df['Evening'] = 0
    df['Business'] = 0
    df['Night Out'] = 0
    df['Leisure'] = 0
    df['Sport'] = 0
    df['Daily'] = 0
    
    return df    

def generate_json(perfume_name, num_reviews_list, titles_list, contents_list, dates_list):
    data = {}

    num_reviews = num_reviews_list
    reviews = []
    for j in range(num_reviews):
        review = {
                "title": titles_list[j],
                "contents": contents_list[j],
                "date": dates_list[j]
        }
        reviews.append(review)
    data[perfume_name] = {"reviews": reviews}

    return data

def save_json(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=3)

def append_json(data, filename):
    with open(filename, 'r+') as file:
        file.seek(0, 2)  # 파일 끝으로 이동
        file.seek(file.tell() - 1, 0)  # 마지막 문자 위치에서 1만큼 이전으로 이동
        file.truncate()  # 마지막 줄의 마지막 중괄호 삭제

        file.write(',')  # 마지막 줄에 쉼표 추가

        json.dump(data, file, indent=3)
        file.write('}')  # JSON 데이터의 마지막 중괄호 추가