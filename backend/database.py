import os
from sqlalchemy import create_engine
import pandas as pd


USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
DB = os.getenv("DB")


def accord_search(user_accord_list):
    engine_url = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DB}"

    print("==== pass accord_search -1")
    print(f"user_accord_list: {user_accord_list}")

    # SQLAlchemy 엔진 생성
    engine = create_engine(engine_url)

    # accord 0개 선택시 처리
    if len(user_accord_list) == 0:
        user_accord_list = tuple(
            [
                "Sweet",
                "Spicy",
                "Oriental",
                "Powdery",
                "Woody",
                "Gourmand",
                "Fresh",
                "Synthetic",
                "Green",
                "Aquatic",
                "Citrus",
                "Creamy",
                "Fruity",
                "Floral",
                "Smoky",
                "Resinous",
                "Leathery",
                "Earthy",
                "Chypre",
                "Animal",
                "Fougère",
            ]
        )

    # accord 1개 선택시 처리
    elif len(user_accord_list) == 1:
        user_accord_list = f"('{user_accord_list[0]}')"

    else:
        user_accord_list = tuple(user_accord_list)

    # SQL 쿼리를 사용하여 데이터를 DataFrame으로 읽어옴
    query = f"""
            SELECT DISTINCT a.perfume_id, p.perfume
            FROM accord a
            JOIN perfume p ON a.perfume_id = p.perfume_id
            WHERE a.name IN {user_accord_list} and p.rating is not null
            order by rating desc;
            """

    print(f"Executing query: {query}")

    try:
        df = pd.read_sql_query(query, con=engine)
    except Exception as e:
        print(f"Error executing query: {e}")
        return []

    if df.empty:
        print("No records found")
        return []

    filtered_accord_perfume_id = list(df["perfume_id"])[:100]
    print("==== pass accord_search -END")

    return filtered_accord_perfume_id


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

    df = df[
        ["perfume", "released_year", "brand", "description", "img_url", "rating", "url"]
    ]

    recommend_perfume_info = df.drop_duplicates()

    # 데이터프레임이 올바르게 생성되었는지 확인
    if recommend_perfume_info.empty:
        raise ValueError("No data found for the given perfume IDs.")

    # 데이터 타입 확인
    print(recommend_perfume_info.dtypes)

    pd.set_option("display.max_colwidth", None)

    # img_url 타입 object -> string으로 변환
    recommend_perfume_info = recommend_perfume_info.astype({"img_url": "str"})
    print(recommend_perfume_info["img_url"])

    return recommend_perfume_info
