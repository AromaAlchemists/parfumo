import os
from sqlalchemy import create_engine
import pandas as pd


# ** 환경 변수에서 DB 접속 정보 읽기 ()
# USER = os.getenv("USER")
# PASSWORD = os.getenv("PASSWORD")
# HOST = os.getenv("HOST")
# DB = os.getenv("DB")
USER = "admin"
PASSWORD = "Qkrwodnr12!"
HOST = "parfumo.cl2402usashg.ap-northeast-2.rds.amazonaws.com"
DB = "parfumo"


# **
def accord_search(user_accord_list):
    engine_url = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DB}"

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
    df = pd.read_sql_query(query, con=engine)
    filtered_accord_perfume_id = list(df["perfume_id"])
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
def get_recommand_perfume_info(recommand_perfume_list):
    engine_url = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DB}"

    # SQLAlchemy 엔진 생성
    engine = create_engine(engine_url)

    # SQL 쿼리를 사용하여 데이터를 DataFrame으로 읽어옴
    query = f"""
                select *
                from perfume p 
                where p.perfume in {tuple(recommand_perfume_list)};
            """
    recommand_perfume_info = pd.read_sql_query(query, con=engine)
    return recommand_perfume_info
