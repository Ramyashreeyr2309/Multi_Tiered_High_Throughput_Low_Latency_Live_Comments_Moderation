import asyncio
from pathlib import Path as LocalPath

from fastapi import FastAPI, status, Path, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, EmailStr
from datetime import datetime
from contextlib import asynccontextmanager
from Message_Queue_Util.producer import send_message
from Message_Queue_Util.consumer import start_consumer
import json
from threading import Thread
from pymongo import MongoClient, DESCENDING
from bson.objectid import ObjectId


# 1. Connect to MongoDB container
client = MongoClient("mongodb://localhost:27017/")

# 2. Select Database and Collection (automatically created if they don't exist)
db = client["maindb"]
comments_collection = db["Comments"]
live_events_collection = db["Live_events"]


class ConnectionManager:
    def __init__(self):
        self.connections = {}

    async def connect(self, event_id: str, websocket: WebSocket):
        await websocket.accept()
        self.connections.setdefault(event_id, set()).add(websocket)

    def disconnect(self, event_id: str, websocket: WebSocket):
        event_connections = self.connections.get(event_id)
        if event_connections is None:
            return
        event_connections.discard(websocket)
        if not event_connections:
            self.connections.pop(event_id, None)

    async def broadcast(self, event_id: str, comment: dict):
        for websocket in list(self.connections.get(event_id, set())):
            try:
                await websocket.send_json(comment)
            except Exception:
                self.disconnect(event_id, websocket)


connection_manager = ConnectionManager()
server_loop = None

def process_classified_comment(ch, method, properties, body):
    """Handle a comment received from the Tier 1 output queue."""
    comment = json.loads(body.decode('utf-8'))
    comment['timestamp']=datetime.fromisoformat(comment['timestamp'])
    print(" [<--] Received classified comment:", comment)
    prob = comment['prob']
    if (prob>=0.6):
        print("toxic comment")
        comment['Is_Toxic']=True
    elif (prob<0.35 and prob>=0.6):
        comment['Is_Toxic']=False
        # send_message(comment,"Tier_2_In_Queue")
    else:
        print(comment['comment_content'])
        comment['Is_Toxic']=False
    result = comments_collection.insert_one(comment)
    print(f"Inserted document ID: {result.inserted_id}")

    event_id = comment.get("event_id")
    if not comment['Is_Toxic'] and event_id and server_loop and not server_loop.is_closed():
        asyncio.run_coroutine_threadsafe(
            connection_manager.broadcast(
                str(event_id),
                jsonable_encoder(comment, custom_encoder={ObjectId: str}),
            ),
            server_loop,
        )
        
    ch.basic_ack(delivery_tag=method.delivery_tag)

@asynccontextmanager
async def lifespan(_app: FastAPI):
    global server_loop
    server_loop = asyncio.get_running_loop()
    comments_collection.create_index([("timestamp", DESCENDING)])
    Thread(
        target=start_consumer,
        args=("Tier_1_Out_Queue", process_classified_comment),
        daemon=True,
    ).start()
    yield


app = FastAPI(lifespan=lifespan)


class CommentCreate(BaseModel):
    comment_content:str
    timestamp:datetime

@app.get("/")
def read_root():
    return {"message": "Hello World"}


@app.get("/live_events_page", include_in_schema=False)
def events_page():
    page_path = LocalPath(__file__).with_name("events_page.html")
    return FileResponse(page_path, media_type="text/html")

@app.post("/live_events/{event_id}/comment",status_code=status.HTTP_201_CREATED)
def post_comment(comment: CommentCreate, event_id: str=Path(...)): 
    comment_data = comment.model_dump(mode="json")
    comment_data["event_id"] = event_id
    send_message(comment_data,"Tier_1_In_Queue")
    return {"message": "Comment created",
            "data":comment,
            "event_id":event_id}

@app.get("/live_events/{event_id}/comments")
def get_comments(page: int = 1, event_id: str=Path(...)):
    if(page < 1 ):
        page = 1
    limit = 20
    skip = (page - 1) * limit
    recent_comments = list(comments_collection.find({"event_id": event_id, "Is_Toxic": False}).sort("timestamp",DESCENDING).limit(limit).skip(skip))
    return jsonable_encoder(recent_comments, custom_encoder={ObjectId: str})


@app.websocket("/ws/live_events/{event_id}")
async def live_event_comments(websocket: WebSocket, event_id: str):
    await connection_manager.connect(event_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connection_manager.disconnect(event_id, websocket)


@app.get("/live_events/{event_id}/page", include_in_schema=False)
def live_event_page(event_id: str):
    page_path = LocalPath(__file__).with_name("live_event_page.html")
    return FileResponse(page_path, media_type="text/html")

@app.get("/live_events/")
def get_live_events(page: int = 1):
    if(page < 1 ):
        page = 1
    limit = 5
    skip = (page - 1) * limit
    live_events = list(live_events_collection.find({}).sort("start_time",DESCENDING).limit(limit).skip(skip))
    return jsonable_encoder(live_events, custom_encoder={ObjectId: str})

@app.get("/live_events/{event_id}/")
def get_live_event(event_id: str=Path(...)):
    live_event = live_events_collection.find_one({"_id": ObjectId(event_id)})
    return jsonable_encoder(live_event, custom_encoder={ObjectId: str})


app.mount(
    "/",
    StaticFiles(directory=LocalPath(__file__).with_name("assets")),
    name="event-assets",
)




