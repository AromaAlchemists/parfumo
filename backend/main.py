from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import pandas as pd
from database import get_data


app = FastAPI()


# temp
RECSYS_API_URL = "http://127.0.0.1:8001"


# define data model
class Preferences(BaseModel):
    audience: list
    season: list
    occasion: list
    text: str


class Chat(BaseModel):
    chat: str


# scale input list to make the sum of its elements equals 1
def scale_to_one(val_list):
    total = sum(val_list)

    if total == 0:
        return val_list

    return [val / total for val in val_list]


# [POST] quick-recommendation
@app.post("/quick-recommendation")
def quick_recommend(prefinput: Preferences):
    prefinput.audience = scale_to_one(prefinput.audience)
    prefinput.season = scale_to_one(prefinput.season)
    prefinput.occasion = scale_to_one(prefinput.occasion)

    try:
        response = requests.post(
            RECSYS_API_URL + "/quick-recommendation", json=prefinput.model_dump()
        )
        response.raise_for_status()
        response_data = response.json()
        recommendations = response_data.get("recommendations", [])

        # error handling - no result from RecSys
        if not recommendations:
            raise HTTPException(status_code=404, detail="No perfumes found")

        sample_data = get_data(recommendations)

        return sample_data

    except requests.RequestException as e:
        raise HTTPException(
            status_code=500, detail=f"RecSysAPI request failed: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


# [POST] chat-recommendation
@app.post("/chat-recommendation")
def chat_recommend(chatinput: Chat):
    try:
        response = requests.post(
            RECSYS_API_URL + "/chat-recommendation", json=chatinput.model_dump()
        )
        response.raise_for_status()
        response_data = response.json()
        recommendations = response_data.get("recommendations", [])

        # error handling - no result from RecSys
        if not recommendations:
            raise HTTPException(status_code=404, detail="No perfumes found")

        sample_data = get_data(recommendations)

        return sample_data

    except requests.RequestException as e:
        raise HTTPException(
            status_code=500, detail=f"RecSysAPI request failed: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
