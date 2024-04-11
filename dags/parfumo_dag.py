from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable

from bs4 import BeautifulSoup 
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
import boto3
from selenium import webdriver

from urllib.request import Request, urlopen
import time
import pandas as pd
from typing import List, Tuple
import logging
import os
from datetime import datetime, timedelta
import pendulum
import glob
from urllib.parse import quote
import re
from utils.constant_util import *
from utils import common_util


# TODO : 크롤링한 코드 추가
def crawling_perfume_product():
    perfume_name = []
    brand_name = []
    url_list = []
    img_list = []   
    for year in range(2020,2021):
        for page in range(10) :
            url = "https://www.parfumo.com/Release_Years/{}?current_page={}&order=nv_desc&hide_order_year=1&show_order_brand=1".format(year, page)
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            webpage = urlopen(req).read()

            soup = BeautifulSoup(webpage, 'html.parser')
            ul = soup.select('div.name')
            if(ul) :
                for name in ul : 
                    perfume_name.append(name.find('a').text)
                    brand_name.append(name.find('span', class_='brand').find('a').text)
                    url_list.append(name.find('a')['href'])
            else :
                break        

            img_elements = soup.select('div.col.col-normal')
            for img_element in img_elements :
                img_list.append(img_element.find('img')['src']) 

    df_perfume = pd.DataFrame({'perfume' : perfume_name, 'brand' : brand_name, 'url' : url_list, 'img' : img_list})  

    src_dir_path =  os.path.join(DOWNLOADS_DIR, f'perfume_product/{NOW_DATE}')
    os.makedirs(src_dir_path, exist_ok=True)
    src_path = os.path.join(src_dir_path, f'{NOW_DATE}_perfume_product.csv')    
    df_perfume.to_csv(src_path, encoding='utf-8-sig',index=False)

def crawling_perfume_detail():
    total_main_accords = total_top_notes = total_heart_notes = total_base_notes = perfumers = []
    ratings_list = gender_list = description_list = total_review_list = scent_rating_list =[]
    longevity_rating_list = sillage_rating_list = bottle_rating_list = vfm_rating_list = description_list = []

    file_path = os.path.join(DOWNLOADS_DIR, f'perfume_product/{NOW_DATE}')
    filename = glob.glob(os.path.join(file_path,'*.csv'))
    df = pd.read_csv(filename[0])

    url_list = df['url'].tolist()

    for url in url_list:
        main_accords = []
        top_notes = []
        heart_notes = []
        base_notes = []
        gender = []
        perfumer_name = []

        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req).read()
        soup = BeautifulSoup(webpage, 'html.parser')
        ######################################################
        '''
        frags = soup.select_one('div.notes_list.mb-2')
        if(frags) :
            spans = frags.find_all('span', class_='nowrap pointer')
            for span in spans:
                fragnance_notes.append(span.text)
        else :
            fragnance_notes.append('')
        '''
        #######################################################
        female = soup.find('i', class_='fa fa-venus')
        male = soup.find('i', class_='fa fa-mars')
        unisex = soup.find('i', class_='fa fa-venus-mars')
        #print(female, male, unisex)
        if(female) :
            gender.append("female")
        elif(male):
            gender.append("male")
        elif(unisex) :
            gender.append("unisex")
        ########################################################    
        main = soup.select_one('div.mb-3.pointer')
        if(main) :
            spans = main.find_all('div', class_='text-xs grey')
            for span in spans:
                main_accords.append(span.text)
        else :
            main_accords.append('')
        #############################################################
        tops = soup.select_one('div.pyramid_block.nb_t.w-100.mt-2 > div.right')
        if(tops) :
            spans = tops.find_all('span', class_='nowrap pointer')
            for span in spans:
                #print(span.text)
                top_notes.append(span.text)

            hearts = soup.select_one('div.pyramid_block.nb_m.w-100.mt-2 > div.right')
            spans = hearts.find_all('span', class_='nowrap pointer')
            for span in spans:
                heart_notes.append(span.text)

            bases = soup.select_one('div.pyramid_block.nb_b.w-100.mt-2 > div.right')
            spans = bases.find_all('span', class_='nowrap pointer')
            for span in spans:
                base_notes.append(span.text)
        else :
            frags = soup.select_one('div.notes_list.mb-2')
            if(frags) :
                spans = frags.find_all('span', class_='nowrap pointer')
                for span in spans:
                    base_notes.append(span.text)
            else :
                base_notes.append('')
            top_notes.append('')
            heart_notes.append('')
        ###########################################################
        '''
        ratings = soup.select_one('div.mb-3.pointer')
        if(main) :
            spans = main.find_all('div', class_='text-xs grey')
            for span in spans:
                ratings.append(span.text)
        else :
            ratings.append('')
        '''
        ###########################################################
        perfumer = soup.select_one('div.w-100.mt-0-5.mb-3')
        if(perfumer) :
            a_tags = perfumer.find_all('a')
            for a_tag in a_tags:
                perfumer_name.append(a_tag.get_text())
        else :
            perfumer_name.append('')
        #############################################################     
            
        gender_list.append(gender)    
        total_main_accords.append(main_accords)
        total_top_notes.append(top_notes)
        total_heart_notes.append(heart_notes)
        total_base_notes.append(base_notes)
        perfumers.append(perfumer_name)  
        
        
        scent_rating = soup.select_one('span.pr-1.text-lg.bold.blue')
        if(scent_rating) :
            scent_rating_list.append(float(scent_rating.string))
        else :
            scent_rating_list.append('')
        #############################################################
        longevity_rating = soup.select_one('span.pr-1.text-lg.bold.pink')
        if(longevity_rating) :
            longevity_rating_list.append(float(longevity_rating.string))
        else :
            longevity_rating_list.append('')
        #############################################################
        sillage_rating = soup.select_one('span.pr-1.text-lg.bold.purple')
        if(sillage_rating) :
            sillage_rating_list.append(float(sillage_rating.string))
        else :
            sillage_rating_list.append('')
        #############################################################
        bottle_rating = soup.select_one('span.pr-1.text-lg.bold.green')
        if(bottle_rating) :
            bottle_rating_list.append(float(bottle_rating.string))
        else :
            bottle_rating_list.append('')
        #############################################################
        vfm_rating = soup.select_one('span.pr-1.text-lg.bold.grey')
        if(vfm_rating) :
            vfm_rating_list.append(float(vfm_rating.string))
        else :
            vfm_rating_list.append('')
        #############################################################
        description_ = soup.select_one('div.p_details_desc.grey-box.text-sm.leading-6.mt-1.mb-2')

        if(description_) :
            sstt = description_.text.replace("Pronunciation", '').replace("\n","")
            description_list.append(sstt)
        else : description_list.append("")
    df['description'] = description_list

    df['gender'] = gender_list

    df['perfumers'] = perfumers

    df['main_accords'] = total_main_accords
    df['top_notes'] = total_top_notes
    df['heart_notes'] = total_heart_notes
    df['base_notes'] = total_base_notes

    df['scent'] = scent_rating_list
    df['longevity'] = longevity_rating_list
    df['sillage'] = sillage_rating_list
    df['bottle'] = bottle_rating_list
    df['vfm'] = vfm_rating_list
    
    src_path = os.path.join(DOWNLOADS_DIR, f'perfume_detail/{NOW_DATE}_perfume_detail')    
    os.makedirs(src_path, exist_ok=True)
    df.to_csv(src_path, encoding='utf-8-sig',index=False)

# TO DO: 리뷰 데이터 코드 추가
# common_utils에서 chart_feature 추가
def crawling_perfume_chart_review():
    file_path = os.path.join(DOWNLOADS_DIR, f'perfume_detail/{NOW_DATE}')
    filename = glob.glob(os.path.join(file_path,'*.csv'))
    df = pd.read_csv(filename[0])

    url_list = df['url'].tolist()
    total_review = []
    pattern = re.compile(r'\d+')
    
    driver = webdriver.Chrome()
    driver.maximize_window()

    for url_count, url in enumerate(url_list[:5]):
        driver.get(url)
        time.sleep(2)
        try : 
            review_number = int(driver.find_element(By.XPATH, '/html/body/div[5]/div/div[1]/nav/div[2]/span').text)
            for i in range(review_number//5):
                driver.execute_script('document.querySelector("#reviews_holder > div.w-100.text-center > span").click()')
                time. sleep ( 1 )
            #more review 버튼 클릭
            elements = driver.find_elements(By.CLASS_NAME, 
                                    'pointer.review-hover')
            
            for element in elements :
                    #element = driver.find_element(By.CLASS_NAME, 
                    #                        'pointer.review-hover')
                    #print(element.text)
                    driver.execute_script("arguments[0].click();", element)
                    #print("1")
                    # 찾은 모든 요소에 대해 반복하여 클릭
            
            title_1 = driver.find_elements(By.XPATH, '//div[@itemprop="reviewBody"]')
            review_list = []
            for title in title_1 :
                review_list.append(title.text)
            print(review_list)
            
            total_review.append(review_list)
        except :
            total_review.append("")
        
        ####################
        try : 
            element = driver.find_element(By.ID, "chartdiv1")
            # 해당 요소로 스크롤하기
            driver.execute_script("arguments[0].scrollIntoView();", element)
            time.sleep(3)
            ####################
            chart_counts = driver.find_elements(By.CLASS_NAME, 'col.mb-2')
            
            time.sleep(3)
            
            for count in chart_counts :   
                #print(chart_counts.text)
                items = count.text.split("\n")
                for item in items :
                    #print(item)
                    feature, num = item.split(maxsplit=1)
                    percentage = pattern.search(num).group()
                    #print(feature, " : ", pattern.search(num).group())
                    if feature in df.columns:
                        df[feature] = df[feature].astype(object)  # 형 변환을 방지하기 위해 object로 설정
                        df.loc[url_count, feature] = percentage       
        except :
            pass



def upload_raw_files_to_s3(bucket_name: str) -> None:
    hook = S3Hook(aws_conn_id="aws_s3")
    src_path = os.path.join(DOWNLOADS_DIR, f'spotify/charts/{NOW_DATE}/*.csv')

    filenames = glob.glob(src_path)
    logging.info(filenames[0])

    for filename in filenames:
        key = filename.replace(DOWNLOADS_DIR, '')     
        key = os.path.join('downloads', key[1:])    #s3 경로에 맞게 경로명 수정
        hook.load_file(filename=filename, key=key, replace = True, bucket_name=bucket_name)




with DAG(dag_id="spotify_chart_to_s3",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    crawling_perfume_product_task = PythonOperator(
        task_id = "crawling_perfume_product",
        python_callable=crawling_perfume_product
    )

    crawling_perfume_detail_task = PythonOperator(
        task_id = "crawling_perfume_detail",
        python_callable=crawling_perfume_detail
    )

    crawling_perfume_chart_review_task = PythonOperator(
        task_id = "crawling_perfume_chart_review_task",
        python_callable=crawling_perfume_chart_review
    )
    
    upload_raw_files_to_s3_task = PythonOperator(
        task_id = "upload_raw_files_to_s3_task",
        python_callable= upload_raw_files_to_s3,
        op_kwargs= {
            "bucket_name": BUCKET_NAME
        }
    )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    start_task >> crawling_perfume_product_task >> crawling_perfume_detail_task >> crawling_perfume_chart_review_task >> upload_raw_files_to_s3_task >> end_task