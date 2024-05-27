from fastapi import FastAPI
from pydantic import BaseModel
from contextlib import asynccontextmanager


# lifespan handler for graceful shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("...Starting up [Recsys API server]...")
    yield
    print("...Shutting down [Recsys API server]...")


app = FastAPI(lifespan=lifespan)


# sample code


class Preferences(BaseModel):
    audience: list
    season: list
    occasion: list
    text: str


class Chat(BaseModel):
    chat: str


def scoring():
    pass


def rag():
    pass


# [POST] quick-recommendation
@app.post("/quick-recommendation")
def do_scoring_and_rag(prefinput: Preferences):
    scoring()  # -> 1차 결과
    # DB - 1차 결과에 해당하는 데이터 가져오기...
    rag()

    # === sample result ===
    recommendations = [
        "Gentleman Givenchy Réserve Privée",
        "Apex",
        "Terre d'Hermès Eau Givrée",
    ]

    return {"recommendations": recommendations}


# [POST] chat-recommendation
@app.post("/chat-recommendation")
def do_rag(chatinput: Chat):
    rag()

    # === sample result ===
    recommendations = [
        "Gentleman Givenchy Réserve Privée",
        "Apex",
        "Terre d'Hermès Eau Givrée",
    ]

    return {"recommendations": recommendations}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
