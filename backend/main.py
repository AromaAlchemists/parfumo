from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import pandas as pd
from database import get_data


app = FastAPI()


# temp
RECSYS_API_URL = "http://127.0.0.1:8001/recommend"


# define data model
class Preferences(BaseModel):
    audience: list
    season: list
    occasion: list
    text: str


# handle post request from FE
@app.post("/recommend")
def recommend(request: Preferences):
    try:
        response = requests.post(RECSYS_API_URL, json=request.model_dump())
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
