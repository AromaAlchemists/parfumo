from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable

from bs4 import BeautifulSoup 
from seleniumbase import Driver
from seleniumbase import page_actions
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import datetime
import time
from selenium.webdriver.common.by import By
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

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
import json
import time
from utils.constant_util import *
from utils import common_util

# 5천개 기분 약 3분
def crawling_perfume_product():
    perfume_name = []
    brand_name = []
    url_list = []
    img_list = []   
    for year in range(2022,2023):
        for page in range(2) :
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

    df_perfume = pd.DataFrame({'perfume' : perfume_name, 'brand' : brand_name, 'url' : url_list, 'img_url' : img_list})  

    src_dir_path =  os.path.join(DOWNLOADS_DIR, f'perfume_product/{NOW_DATE}')
    os.makedirs(src_dir_path, exist_ok=True)
    src_path = os.path.join(src_dir_path, f'{NOW_DATE}_perfume_product.csv')    
    df_perfume.to_csv(src_path, encoding='utf-8-sig',index=False)

# 5천개 기준 1시간
def crawling_perfume_detail():
    df = common_util.concat_chart_feature()
    #df = common_util.concat_chart_feature()
    url_list = df['url'].tolist()

    for url in url_list[:5]:
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req).read()
        soup = BeautifulSoup(webpage, 'html.parser')

        ## released_year
        year = soup.select_one('#pd_inf > div.cb.pt-1 > main > div.p_details_holder > h1 > span > span > a:nth-child(2) > span')
        if(year) :
            year_list.append(int(year.text))
        else :
            year_list.append("")
            print("!")

        ## gender
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

        ## accord  
        main_accords = []
        main = soup.select_one('div.mb-3.pointer')
        if(main) :
            spans = main.find_all('div', class_='text-xs grey')
            for span in spans:
                main_accords.append(span.text)
        else :
            main_accords.append('')

        ## notes
        top_notes = []
        base_notes = []
        heart_notes = []
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

        ## perfumers
        perfumer_name = []
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

        ## rating
        rating = soup.find('span', class_='ratingvalue r8')
        
        if rating :
            ratings_list.append(float(rating.text))
        else :
            ratings_list.append("")
        #pd_inf > div.cb.pt-1 > main > div.p_details_holder_second > div:nth-child(2) > span.text-xs > span
        
        rating_count = soup.find('span', class_='text-xs')
        if rating_count :
            ratings_count_list.append(float(rating_count.text.split()[0]))
        else :
            ratings_count_list.append("")

        ## scent
        scent_rating = soup.select_one('span.pr-1.text-lg.bold.blue')
        scent_count = soup.select_one("#bar > div > span.lightgrey.text-2xs.upper")
        
        if(scent_rating) :
            scent_rating_list.append(float(scent_rating.string))
            scent_rating_count_list.append(int(scent_count.string.split()[0]))
        else :
            scent_rating_list.append('')
            scent_rating_count_list.append('')

        ## longevity
        longevity_rating = soup.select_one('span.pr-1.text-lg.bold.pink')
        longevity_count = soup.select_one("#bar_dur > div > span.lightgrey.text-2xs.upper")
        
        if(longevity_rating) :
            longevity_rating_list.append(float(longevity_rating.string))
            longevity_rating_count_list.append(int(longevity_count.string.split()[0]))
        else :
            longevity_rating_list.append('')
            longevity_rating_count_list.append('')
            
        ## sillage
        sillage_rating = soup.select_one('span.pr-1.text-lg.bold.purple')
        sillage_count = soup.select_one("#bar_sillage > div > span.lightgrey.text-2xs.upper")
        
        if(sillage_rating) :
            sillage_rating_list.append(float(sillage_rating.string))
            sillage_rating_count_list.append(int(sillage_count.string.split()[0]))
        else :
            sillage_rating_list.append('')
            sillage_rating_count_list.append('')

        ## bottle
        bottle_rating = soup.select_one('span.pr-1.text-lg.bold.green')
        bottle_count = soup.select_one("#bar_bottle > div > span.lightgrey.text-2xs.upper")
        
        if(bottle_rating) :
            bottle_rating_list.append(float(bottle_rating.string))
            bottle_rating_count_list.append(int(bottle_count.string.split()[0]))
        else :
            bottle_rating_list.append('')
            bottle_rating_count_list.append('')

        ## value_for_money
        vfm_rating = soup.select_one('span.pr-1.text-lg.bold.grey')
        vfm_count = soup.select_one("#bar_pricing > div > span.lightgrey.text-2xs.upper")
        
        if(vfm_rating) :
            vfm_rating_list.append(float(vfm_rating.string))
            vfm_rating_count_list.append(int(vfm_count.string.split()[0]))
        else :
            vfm_rating_list.append('')
            vfm_rating_count_list.append('')

        ## description
        description_ = soup.find('div', class_='p_details_desc grey text-sm leading-6 mt-1 mb-2 pr-2')

        if(description_) :
            sstt = description_.text.replace('\n','')
            description_list.append(sstt)
        else : description_list.append("")
        
    df = df.iloc[:5,:]
    df['description'] = description_list

    df['gender'] = gender_list
    df['released_year'] = year_list
    df['perfumers'] = perfumers

    df['rating'] = ratings_list
    df['rating_count'] = ratings_count_list

    df['main_accords'] = total_main_accords

    df['top_notes'] = total_top_notes
    df['heart_notes'] = total_heart_notes
    df['base_notes'] = total_base_notes

    df['scent'] = scent_rating_list  
    df['longevity'] = longevity_rating_list
    df['sillage'] = sillage_rating_list
    df['bottle'] = bottle_rating_list
    df['vfm'] = vfm_rating_list

    df['scent_count'] = scent_rating_count_list
    df['longevity_count'] = longevity_rating_count_list
    df['sillage_count'] = sillage_rating_count_list
    df['bottle_count'] = bottle_rating_count_list
    df['vfm_count'] = vfm_rating_count_list


    
    src_dir_path = os.path.join(DOWNLOADS_DIR, f'perfume_detail/{NOW_DATE}')    
    os.makedirs(src_dir_path, exist_ok=True)
    src_path = os.path.join(src_dir_path, f'{NOW_DATE}_perfume_detail.csv') 
    df.to_csv(src_path, encoding='utf-8-sig',index=False)


#TO_DO : 실패했을 때 될 때가지 while문 써서 최대한 5번 정도는 실패하고 fail 처리하기
def crawling_perfume_chart_review(): 
    file_path = os.path.join(DOWNLOADS_DIR, f'perfume_detail/{NOW_DATE}')
    filename = glob.glob(os.path.join(file_path,'*.csv'))[0]
    df = pd.read_csv(filename)
    
    # URL 칼럼 가져오기
    url_list = df['url'].tolist()
    perfume_list = df['perfume'].tolist()
    """
    total_review = []
    total_title = []
    total_date = []
    """
    pattern = re.compile(r'\d+')
    total_review_json = {}
    
    driver = Driver(uc=True)
    driver.get(url_list[0])
    
    time.sleep(1)
    
    d = driver.find_element(By.ID, "sp_message_iframe_902160")
    driver.switch_to.frame(d)
    aa = driver.find_element(By.CSS_SELECTOR, ("button[aria-label='Accept']"))
    aa.click()
    driver.switch_to.default_content()
    
    time.sleep(1)
    
    logging.info("Access the website successfully!" )
    
    for url_count, url in enumerate(url_list):
        logging.info(url)
        driver.get(url)
        time.sleep(3)
        ########## 리뷰 ##########
        try : 
            review_number = int(driver.find_element(By.CLASS_NAME, "mt-3.mb-3").find_element(By.CLASS_NAME, "lightgrey.mb-1").text.split(' ')[0])
            for i in range(review_number//5):
                driver.execute_script('document.querySelector("#reviews_holder > div.w-100.text-center > span").click()')
                time.sleep(1.5)
            elements = driver.find_elements(By.CLASS_NAME, 
                                    'pointer.review-hover')
    
            for element in elements :
                #element = driver.find_element(By.CLASS_NAME, 
                #                        'pointer.review-hover')
                #print(element.text)
                driver.execute_script("arguments[0].click();", element)
                #print("1")
                # 찾은 모든 요소에 대해 반복하여 클릭
    
            body = driver.find_element(By.ID, 'reviews_holder_reviews')
            reviews = body.find_elements(By.CLASS_NAME, 'leading-7')
            logging.info(f"the number of review : {len(reviews)}")

            review_list = []
            for review in reviews :
                review_list.append(review.text.replace('\n',' '))

            titles = body.find_elements(By.CLASS_NAME, 'text-lg.bold')
            title_list = []
            for title in titles :
                title_list.append(title.text)

            dates = body.find_elements(By.CLASS_NAME, 'text-sm.lightgrey.grey.fl')
            date_list = []
            for date in dates :
                date_object = datetime.strptime(date.text, '%m/%d/%Y')
                formatted_date = date_object.strftime('%Y-%m-%d')
                date_list.append(formatted_date)
            
            #total_date.append(date_list)
            perfume_name = perfume_list[url_count]
            #print(perfume_name)
            
            review_json = common_util.generate_json(perfume_name, len(reviews), title_list, review_list, date_list)
            print(review_json)
            
            total_review_json.update(review_json)
            logging.info("Getting a review successfully!")
        except :
            pass
            logging.info("Fail to get review")
        
            
        ########## 차트 ##########
        try : 
            element = driver.find_element(By.ID, "edit_classification")
            # 해당 요소로 스크롤하기
            driver.execute_script("arguments[0].scrollIntoView();", element)
            time.sleep(3)
    
            element = driver.find_element(By.ID, "chartdiv1")
            # 해당 요소로 스크롤하기
            driver.execute_script("arguments[0].scrollIntoView();", element)
            ####################
            chart_counts = driver.find_elements(By.CLASS_NAME, 'col.mb-2')
    
            time.sleep(1)
    
            for count in chart_counts :   
                items = count.text.split("\n")
                for item in items :
                    feature, num = item.split(maxsplit=1)
                    percentage = pattern.   search(num).group()
                    if feature in df.columns:
                        df[feature] = df[feature].astype(object)  # 형 변환을 방지하기 위해 object로 설정
                        df.loc[url_count, feature] = percentage
                        # print("in~")
            logging.info(f"{feature} : {percentage}")
            logging.info("Getting a infomation successfully!")
            ####################         
        except :
            found = False
            attempts = 0
            while not found and attempts < 5:  # 최대 5번 시도
                # 페이지를 조금씩 아래로 스크롤
                driver.execute_script("window.scrollBy(0, 400);")  # 매 반복마다 400픽셀 아래로 스크롤
                time.sleep(1)  # 로딩 시간 대기
    
                # 특정 요소가 로드되었는지 확인
                try:
                    element = driver.find_element(By.ID, "edit_classification")  # ID를 통해 요소 찾기
                    driver.execute_script("arguments[0].scrollIntoView();", element)  # 요소가 보이면 스크롤
                    found = True
                    chart_counts = driver.find_elements(By.CLASS_NAME, 'col.mb-2')
    
                    time.sleep(1)
    
                    for count in chart_counts :   
                        items = count.text.split("\n")
                        for item in items :
                            feature, num = item.split(maxsplit=1)
                            percentage = pattern.search(num).group()
                            if feature in df.columns:
                                df[feature] = df[feature].astype(object)  # 형 변환을 방지하기 위해 object로 설정
                                df.loc[url_count, feature] = percentage
                    logging.info(f"{feature} : {percentage}")
                    logging.info("Getting a infomation successfully!")
                except :
                    attempts += 1
                    logging.info("Fail to get a infomation successfully")
    
    
    
    src_dir_path = os.path.join(DOWNLOADS_DIR, f'chart/{NOW_DATE}')    
    os.makedirs(src_dir_path, exist_ok=True)
    src_path = os.path.join(src_dir_path, f'{NOW_DATE}_chart.csv') 
    df.to_csv(src_path, encoding='utf-8-sig',index=False)
    
    
    review_dir_path = os.path.join(DOWNLOADS_DIR, f'review/{NOW_DATE}')    
    os.makedirs(review_dir_path, exist_ok=True)
    for key in total_review_json.keys():
        review_dic_json = {}
        review_dic_json[key] = total_review_json[key]
        file_path = os.path.join(review_dir_path, f'{key}.json')
        
        
        # 파일 쓰기
        with open(file_path, 'w', encoding='utf-8') as review_json_file:
            json.dump(review_dic_json, review_json_file, ensure_ascii=False, indent=4)


def upload_raw_files_to_s3(bucket_name: str) -> None:
    hook = S3Hook(aws_conn_id="aws_s3")
    logging.info("Connecting successfully!")
    
    perfume_product_src_path = os.path.join(DOWNLOADS_DIR, f'perfume_product/{NOW_DATE}')
    perfume_detail_src_path = os.path.join(DOWNLOADS_DIR, f'perfume_detail/{NOW_DATE}')
    chart_src_path = os.path.join(DOWNLOADS_DIR, f'chart/{NOW_DATE}')
    review_src_path = os.path.join(DOWNLOADS_DIR, f'review/{NOW_DATE}')

    src_paths = [perfume_product_src_path, perfume_detail_src_path, chart_src_path, review_src_path]
    for src_path in src_paths:
        try:
            src_files = glob.glob(os.path.join(src_path, '*.csv'))
            src_file = src_files[0]
            key = src_file.replace(DOWNLOADS_DIR, '')     
            key = os.path.join('downloads', key[1:])    #s3 경로에 맞게 경로명 수정'
            hook.load_file(filename=src_file, key=key, replace=True, bucket_name=bucket_name)
        except:
            src_files = glob.glob(os.path.join(src_path, '*.json'))
            for src_file in src_files:
                key = src_file.replace(DOWNLOADS_DIR, '')     
                key = os.path.join('downloads', key[1:])    #s3 경로에 맞게 경로명 수정'
                hook.load_file(filename=src_file, key=key, replace=True, bucket_name=bucket_name)



with DAG(dag_id="parfumo_to_s3_dag",
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

    call_trigger_task = TriggerDagRunOperator(
        task_id='call_trigger',
        trigger_dag_id='transform_parfumo_to_s3_dag',
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

    start_task >> crawling_perfume_product_task >> crawling_perfume_detail_task >> crawling_perfume_chart_review_task >> upload_raw_files_to_s3_task >> call_trigger_task >> end_task