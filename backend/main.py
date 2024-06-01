from fastapi import FastAPI, HTTPException
import pandas as pd
from contextlib import asynccontextmanager
from models import Preferences, Chat
from database import get_data
from recsys import do_scoring_and_rag, do_rag


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
        recommendations = do_scoring_and_rag(prefinput)

        # error handling - no result from RecSys
        if not recommendations:
            raise HTTPException(status_code=404, detail="No perfumes found")

        sample_data = get_data(recommendations)

        return sample_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


# [POST] chat-recommendation
@app.post("/chat-recommendation")
async def chat_recommend(chatinput: Chat):
    try:
        recommendations = do_rag(chatinput)

        # error handling - no result from RecSys
        if not recommendations:
            raise HTTPException(status_code=404, detail="No perfumes found")

        sample_data = get_data(recommendations)

        return sample_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
