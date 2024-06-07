from models import Preferences, Chat
from database import accord_search, get_table_from_db
import os
import re
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm
import chromadb
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

# from langchain.embeddings.openai import OpenAIEmbeddings

from langchain_openai import OpenAIEmbeddings, ChatOpenAI

from langchain_community.vectorstores import Chroma

# from langchain.vectorstores import Chroma

# from langchain.chat_models import ChatOpenAI
from langchain.chains.question_answering import load_qa_chain
from langchain.chains import RetrievalQA
from langchain_text_splitters import CharacterTextSplitter

import logging

# DB_PATH = "../../"
DB_PATH = r""

# **user_input = [0.8, 0.2, 0, 0, 0, 0.3, 0.7, 0, 0, 0, 0, 0.6, 0.2, 0.2]


# **
def calculate_score2(vector, preference, count):
    preference = np.array(preference).reshape(1, -1)
    similarity = cosine_similarity(preference, vector)
    similarity_squared = similarity**2
    # 투표인원에 로그를 취하고 1을 더한 값
    count_score = 1 + np.log10(count + 1)
    # 스코어 계산
    # print(similarity_squared,count_score)
    score = similarity_squared * count_score

    return score


# **
def scoring_function(user_input, filtered_accord_perfume_id, k):
    scores = []
    result = {}
    user_input = [0.8, 0.2, 0, 0, 0, 0.3, 0.7, 0, 0, 0, 0, 0.6, 0.2, 0.2]
    chart_df = get_table_from_db("chart")
    chart_feature_df = get_table_from_db("chart_feature")

    for i in tqdm(filtered_accord_perfume_id):
        # chart_df
        audience_count = list(
            chart_df[(chart_df["perfume_id"] == i) & (chart_df["name"] == "Audience")][
                "vote"
            ]
        )[0]
        season_count = list(
            chart_df[(chart_df["perfume_id"] == i) & (chart_df["name"] == "Season")][
                "vote"
            ]
        )[0]
        occasion_count = list(
            chart_df[(chart_df["perfume_id"] == i) & (chart_df["name"] == "Occasion")][
                "vote"
            ]
        )[0]

        # chart_feature_df
        audience_vector = np.array(
            chart_feature_df[
                (chart_feature_df["perfume_id"] == i)
                & (chart_feature_df["parent"] == "Audience")
            ]["percentage"]
        ).reshape(1, -1)
        season_vector = np.array(
            chart_feature_df[
                (chart_feature_df["perfume_id"] == i)
                & (chart_feature_df["parent"] == "Season")
            ]["percentage"]
        ).reshape(1, -1)
        occasion_vector = np.array(
            chart_feature_df[
                (chart_feature_df["perfume_id"] == 1)
                & (chart_feature_df["parent"] == "Occasion")
            ]["percentage"][:6]
        ).reshape(1, -1)

        audience_score = calculate_score2(
            audience_vector, user_input[0:4], audience_count
        )
        season_score = calculate_score2(season_vector, user_input[4:8], season_count)
        occasion_score = calculate_score2(
            occasion_vector, user_input[8:], occasion_count
        )

        score = audience_score + season_score + occasion_score
        # print(score, type(score))
        result[str(i)] = score

    sorted_result = sorted(result.items(), key=lambda x: x[1], reverse=True)
    # 상위 k개의 아이템 추출
    top_k = sorted_result[:k]
    keys = [int(item[0]) for item in top_k]

    return keys


# **
def rag_with_filtered_list(filterd_perfume_list, query, DB_PATH):
    # api_key = os.getenv("OPENAI_API_KEY")
    api_key = "sk-"

    embeddings = OpenAIEmbeddings(openai_api_key=api_key)
    # 필터링 DB 생성
    embedding_function = OpenAIEmbeddingFunction(api_key=api_key)
    client = chromadb.PersistentClient(path=DB_PATH)

    collection_perfume_filtered = client.get_or_create_collection(
        name="perfume_review_with_description_filtered",
        embedding_function=embedding_function,
        metadata={"hnsw:space": "cosine"},
    )

    collection_perfume = client.get_collection(name="perfume_review_with_description")

    # 기존 DB에서 입력받은 list 내 향수들만 따로 저장
    for perfume_name in filterd_perfume_list:
        try:
            collection_perfume_filtered_ = collection_perfume.get(
                where={"perfume": "{}".format(perfume_name)}
            )

            if not collection_perfume_filtered_["documents"]:
                logging.warning(f"No documents found for perfume: {perfume_name}")
                continue

            collection_perfume_filtered.add(
                documents=collection_perfume_filtered_["documents"],
                metadatas=collection_perfume_filtered_["metadatas"],
                ids=collection_perfume_filtered_["ids"],
            )

        except Exception as e:
            logging.error(
                f"Error retrieving or adding perfume: {perfume_name}, {str(e)}"
            )
        # collection_perfume_filtered_ = collection_perfume.get(
        #    where={"perfume": "{}".format(perfume_name)}
        # )
        # collection_perfume_filtered.add(
        #    documents=collection_perfume_filtered_["documents"],
        #    metadatas=collection_perfume_filtered_["metadatas"],
        #    ids=collection_perfume_filtered_["ids"],
        # )

    # Chroma Vector Store 설정
    vector_store = Chroma(
        collection_name="perfume_review_with_description_filtered",
        client=client,
        embedding_function=embeddings,
    )
    # LangChain LLM 설정
    llm = ChatOpenAI(
        model="gpt-4",
        temperature=0,
        max_tokens=None,
        timeout=None,
        max_retries=2,
        api_key=api_key,
    )

    # QA 체인 구성
    qa_chain = load_qa_chain(llm=llm, chain_type="stuff")
    # RetrievalQA 체인 구성
    # retrieval_qa_chain = create_retrieval_chain(
    retrieval_qa_chain = RetrievalQA(
        retriever=vector_store.as_retriever(search_type="mmr", search_kwargs={"k": 3}),
        combine_documents_chain=qa_chain,
        return_source_documents=True,
    )

    prompt = """Your task is to answer the question using the provided document and to cite the passage(s) of the document used to answer the question.
              If there no any exact answer about given question, you can infer with given document and answer by using it.
              Provide 3 answers. If an answer to the question is provided, it must be annotated with a citation.
              Use the following format for to cite relevant passages.
              ({"perfume name": … ,"reason " : … , "citation" : …}). 
              You recognize that the recommanded "perfume name" should not be same but always be different
              Question : """
    question = prompt + query
    result = retrieval_qa_chain({"query": question})
    client.delete_collection(name="perfume_review_with_description_filtered")
    # print(f"Query: {question}")
    print(f"Answer: {result['result']}")
    # 필터링 DB 삭제
    result_list = []
    print(result["result"])
    find_perfume = re.findall(r'"perfume name": "([^"]+)"', result["result"])
    for name in find_perfume:
        # print(name)
        result_list.append(name)

    return result_list


# **filtering + scoring + RAG
def quick_recommendation(prefinput: Preferences):
    user_input = prefinput.audience + prefinput.season + prefinput.occasion

    # (1) 입력된 accord에 대해 filtering
    filtered_accord_perfume_id = accord_search(prefinput.accord)

    # (2) 입력된 audience, season, occasion에 대해 scoring
    filtered_scoring_function_id = scoring_function(
        user_input, filtered_accord_perfume_id, 30
    )

    # (3) filtering된 목록에 대해 RAG 수행
    # DB_PATH = "./"
    # query = "recommend the perfume with woody, cinnamon scent and also good to give a present to my girlfriend"
    recommendations = rag_with_filtered_list(
        filtered_scoring_function_id, prefinput.text, DB_PATH
    )
    print(recommendations)
    return {"recommendations": recommendations}


def chat_recommendation(chatinput: Chat):

    # === sample result ===
    recommendations = [
        "Gentleman Givenchy Réserve Privée",
        "Apex",
        "Terre d'Hermès Eau Givrée",
    ]

    return {"recommendations": recommendations}
