import sys
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

from configuration import config
from web.ChatService import ChatService


app=FastAPI()

app.mount("/static", StaticFiles(directory=config.WEB_STATIV_DIR), name="static")

service=ChatService()

class Question(BaseModel):
    message:str

class Answer(BaseModel):
    message:str


@app.get("/")
def read_root():
    return RedirectResponse("/static/index.html")

@app.post("/api/chat")
def read_item(question:Question):
    result=service.chat(question.message)
    return Answer(message=result)

if __name__=="__main__":
    # print(sys.path)
    uvicorn.run("app:app",host="0.0.0.0",port=5555)