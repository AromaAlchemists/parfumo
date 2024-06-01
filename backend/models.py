from pydantic import BaseModel


# define data model
class Preferences(BaseModel):
    audience: list
    season: list
    occasion: list
    text: str


class Chat(BaseModel):
    chat: str
