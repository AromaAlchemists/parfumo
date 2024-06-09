import os
from sqlalchemy import create_engine
import pandas as pd


# ** 환경 변수에서 DB 접속 정보 읽기 ()
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
DB = os.getenv("DB")


# **
def accord_search(user_accord_list):
    engine_url = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DB}"

    print("==== pass accord_search -1")
    print(f"user_accord_list: {user_accord_list}")

    # SQLAlchemy 엔진 생성
    engine = create_engine(engine_url)

    # SQL 쿼리를 사용하여 데이터를 DataFrame으로 읽어옴
    query = f"""
            SELECT DISTINCT a.perfume_id, p.perfume
            FROM accord a
            JOIN perfume p ON a.perfume_id = p.perfume_id
            WHERE a.name IN {tuple(user_accord_list)} and p.rating is not null
            order by rating desc;
            """

    print(f"Executing query: {query}")

#    with engine.connect() as connection:
#        df = pd.read_sql_query(query, con=connection)

    df = pd.read_sql_query(query, con=engine)
    
    if df.empty:
        print("No records found")
        return []
    
    filtered_accord_perfume_id = list(df["perfume_id"])[:100]  # **
    print("==== pass accord_search -END")
    return filtered_accord_perfume_id


# **
def get_table_from_db(table):
    engine_url = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DB}"

    # SQLAlchemy 엔진 생성
    engine = create_engine(engine_url)

    # SQL 쿼리를 사용하여 데이터를 DataFrame으로 읽어옴
    query = f"""
            select * from {table}
            """
    df = pd.read_sql_query(query, con=engine).drop_duplicates()
    return df


# **
def get_recommend_perfume_info(recommend_perfume_list):
    engine_url = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DB}"

    # SQLAlchemy 엔진 생성
    engine = create_engine(engine_url)

    # SQL 쿼리를 사용하여 데이터를 DataFrame으로 읽어옴
    query = f"""
                select *
                from perfume p 
                where p.perfume_id in {tuple(recommend_perfume_list)};
            """
    df = pd.read_sql_query(query, con=engine)

    df = df[['perfume', 'released_year', 'brand', 'description', 'img_url', 'rating', 'url']]
    
    recommend_perfume_info = df.drop_duplicates()


    # 데이터프레임이 올바르게 생성되었는지 확인
    if recommend_perfume_info.empty:
        raise ValueError("No data found for the given perfume IDs.")

    # 데이터 타입 확인
    print(recommend_perfume_info.dtypes)

    pd.set_option('display.max_colwidth', None)

    print("[ before ]")
    print(recommend_perfume_info["img_url"])

    recommend_perfume_info["img_url"] = recommend_perfume_info["img_url"].str.replace(" ", "")
    # img_url 타입 object -> string으로 변환
    recommend_perfume_info = recommend_perfume_info.astype({"img_url": "str"})
    print("[ after ]")
    print(recommend_perfume_info["img_url"])

    return recommend_perfume_info
