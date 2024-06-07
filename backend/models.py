from pydantic import BaseModel


# define data model
class Preferences(BaseModel):
    accord: list
    audience: list
    season: list
    occasion: list
    text: str


class Chat(BaseModel):
    text: str
