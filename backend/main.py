from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from contextlib import asynccontextmanager
from models import Preferences, Chat
from database import get_recommend_perfume_info
from recsys import FSRAG, chat_recommendation


# lifespan handler for graceful shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("...Starting up [main server]...")
    yield
    print("...Shutting down [main server]...")


app = FastAPI(lifespan=lifespan)


# scale input list to make the sum of its elements equals 1
def scale_to_one(val_list):
    total = sum(val_list)

    if total == 0:
        return val_list

    return [val / total for val in val_list]


# [POST] quick-recommendation
@app.post("/quick-recommendation")
async def quick_recommend(prefinput: Preferences):
    prefinput.audience = scale_to_one(prefinput.audience)
    prefinput.season = scale_to_one(prefinput.season)
    prefinput.occasion = scale_to_one(prefinput.occasion)

    try:
        # **유저 입력에 대해 RecSys-quick_recommendation 호출
        recommendations = FSRAG(prefinput)

        # error handling - no result from RecSys
        if not recommendations:
            raise HTTPException(status_code=404, detail="No perfumes found")

        print(recommendations)
        print(recommendations["recommendations"])
        recommendation_values = recommendations["recommendations"]

        # **결과 향수 목록에 대해 DB에서 정보 가져오기
        result_df = get_recommend_perfume_info(recommendation_values)

        # Pandas DataFrame을 JSON으로 변환
        result_json = result_df.to_dict(orient="records")
        return jsonable_encoder(result_json)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


# [POST] chat-recommendation
@app.post("/chat-recommendation")
async def chat_recommend(chatinput: Chat):
    try:
        recommendations = chat_recommendation(chatinput)

        # error handling - no result from RecSys
        if not recommendations:
            raise HTTPException(status_code=404, detail="No perfumes found")

        # **결과 향수 목록에 대해 DB에서 정보 가져오기
        return get_recommand_perfume_info(recommendations)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
