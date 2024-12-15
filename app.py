from fastapi import FastAPI, HTTPException, BackgroundTasks, status, Request
from pydantic import BaseModel
from fastapi_pagination import Page, add_pagination, paginate
from fastapi_pagination.utils import disable_installed_extensions_check
disable_installed_extensions_check()
from typing import List, Optional
import mysql.connector
from discord import SyncWebhook
import time
import uuid
from fastapi.middleware.cors import CORSMiddleware
import requests
from urllib.parse import quote, urlencode

db_config = {
    'user': 'root',
    'password': 'dbuserdbuser',
    'host': '34.46.34.153',
    'database': 'w4153'
}

app = FastAPI()
add_pagination(app)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class BasicResponse(BaseModel):
    message: str
    links: dict

class DialogueResponse(BaseModel):
    id: int
    user_id: int
    conversation_id: str
    speaker: str
    content: str
    links: dict

# middleware to log all requests
@app.middleware("http")
async def sql_logging(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    
    with mysql.connector.connect(**db_config) as conn:
        with conn.cursor(dictionary=True) as cursor:
            query = "INSERT INTO logs (microservice, request, response, elapsed) VALUES (%s, %s, %s, %s)"
            values = ('dialogues', str(request.url.path), str(response.status_code), int(process_time))
            cursor.execute(query, values)
            conn.commit()
        
    return response

# basic hello world for the microservice
@app.get("/")
def get_microservice() -> BasicResponse:
    """
    Simple endpoint to test and return which microservice is being connected to.
    """
    return BasicResponse(message="hello world from dialogues microservice", links={})

# get dialogue by id
@app.get("/dialogues/{dialogue_id}")
def get_dialogue(dialogue_id: int) -> Optional[DialogueResponse]:
    """
    Get a Dialogue by its id from the database.
    """
    with mysql.connector.connect(**db_config) as conn:
        with conn.cursor(dictionary=True) as cursor:
            query = "SELECT id, user_id, conversation_id, speaker, content FROM dialogues WHERE id = %s"
            values = (dialogue_id,)
            cursor.execute(query, values)
            
            row = cursor.fetchone()
            if row:
                dialogue = DialogueResponse(id=row['id'], user_id=row['user_id'], conversation_id=row['conversation_id'], speaker=row['speaker'], content=row['content'], links={'get': f'/dialogues/{row["id"]}'})
                return dialogue
            else:
                return HTTPException(status_code=404, detail=f'dialogue id {dialogue_id} not found')

# get all dialogues
@app.get("/dialogues")
def get_dialogues() -> Page[DialogueResponse]:
    """
    Get all Dialogues in the database.
    """
    with mysql.connector.connect(**db_config) as conn:
        with conn.cursor(dictionary=True) as cursor:
            query = "SELECT id, user_id, conversation_id, speaker, content FROM dialogues"
            cursor.execute(query)
            
            rows = cursor.fetchall()
            if rows:
                dialogues = [DialogueResponse(id=row['id'], user_id=row['user_id'], conversation_id=row['conversation_id'], speaker=row['speaker'], content=row['content'], links={'get': f'/dialogues/{row["id"]}'})
                             for row in rows]
                return paginate(dialogues)
            else:
                return HTTPException(status_code=400, detail=f'bad request to dialogues table')

# post new dialogue
@app.post("/dialogues", status_code=201)
def post_dialogue(user_id: int, conversation_id: str, speaker: str, content: str) -> DialogueResponse:
    """
    Post a new Dialogue to the database.
    """
    time.sleep(5)
    with mysql.connector.connect(**db_config) as conn:
        with conn.cursor(dictionary=True) as cursor:
            query = "INSERT INTO dialogues (user_id, conversation_id, speaker, content) VALUES (%s, %s, %s, %s)"
            values = (user_id, conversation_id, speaker, content)
            cursor.execute(query, values)

            conn.commit()
            
            new_id = cursor.lastrowid

            dialogue = DialogueResponse(id=new_id, user_id=user_id, conversation_id=conversation_id, speaker=speaker, content=content, links={'get': f'/dialogues/{new_id}'})
            return dialogue

# get all dialogues from a specific user
@app.get("/dialogues/from_user/{user_id}")
def get_dialogues_from_user(user_id: int) -> Page[DialogueResponse]:
    """
    Get all Dialogues from a specific user id in the database.
    """
    with mysql.connector.connect(**db_config) as conn:
        with conn.cursor(dictionary=True) as cursor:
            query = "SELECT id, user_id, conversation_id, speaker, content FROM dialogues WHERE user_id = %s"
            values = (user_id,)
            cursor.execute(query, values)
            
            rows = cursor.fetchall()
            if rows:
                dialogues = [DialogueResponse(id=row['id'], user_id=row['user_id'], conversation_id=row['conversation_id'], speaker=row['speaker'], content=row['content'], links={'get': f'/dialogues/{row["id"]}'})
                             for row in rows]
                return paginate(dialogues)
            else:
                return HTTPException(status_code=400, detail=f'user id not found')

# get all dialogues from a specific conversation
@app.get("/dialogues/from_conversation/{conversation_id}")
def get_dialogues_from_conversation(conversation_id: int) -> List[DialogueResponse]:
    """
    Get all Dialogues from a specific conversation in the database.
    """
    with mysql.connector.connect(**db_config) as conn:
        with conn.cursor(dictionary=True) as cursor:
            query = "SELECT id, user_id, conversation_id, speaker, content FROM dialogues WHERE conversation_id = %s"
            values = (conversation_id,)
            cursor.execute(query, values)
            
            rows = cursor.fetchall()
            if rows:
                dialogues = [DialogueResponse(id=row['id'], user_id=row['user_id'], conversation_id=row['conversation_id'], speaker=row['speaker'], content=row['content'], links={'get': f'/dialogues/{row["id"]}'})
                             for row in rows]
                return dialogues
            else:
                return HTTPException(status_code=400, detail=f'conversation id not found')
            
task_status = dict()

# async post dialogues
@app.post("/dialogues/async", status_code=202)
async def async_post_dialogue(user_id: int, conversation_id: str, speaker: str, content: str, background_tasks: BackgroundTasks) -> BasicResponse:
    """
    Create a new dialogue with the given user_id, conversation_id, speaker, and content. Performs update asynchronously (and usually takes around 10s to take effect).
    """
    def wait_post_dialogue(user_id: int, conversation_id: str, speaker: str, content: str):
        time.sleep(10)
        post_dialogue(user_id, conversation_id, speaker, content)
        task_status[task_id] = 'done'

    task_id = str(uuid.uuid4())
    task_status[task_id] = 'working'
    background_tasks.add_task(wait_post_dialogue, user_id, conversation_id, speaker, content)

    return {'message': f'successfully accepted post for dialogue', 'links': {'status': f'/dialogues/async/{task_id}'}}

# async get task update
@app.get("/dialogues/async_check/{task_id}")
def get_async_status(task_id: str) -> BasicResponse:
    """
    Checks the async task status based on the task id.
    """
    if task_id not in task_status:
        return HTTPException(status_code=404, detail=f'task id {task_id} not found')
    elif task_status[task_id] != 'done':
        return BasicResponse(message=f'task {task_id} still in progress', links={'status': f"/dialogues/async_check/{task_id}"})
    else:
        return BasicResponse(message=f'task {task_id} has completed', links={'status': f"/dialogues/async_check/{task_id}"})
    
# main microservice run
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)