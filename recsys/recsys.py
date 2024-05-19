from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI()


class Preferences(BaseModel):
    audience: list
    season: list
    occasion: list
    text: str


# sample code for test
@app.post("/recommend")
def recommend(request: Preferences):
    recommendations = [
        "Gentleman Givenchy Réserve Privée",
        "Apex",
        "Terre d'Hermès Eau Givrée",
    ]

    return {"recommendations": recommendations}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
