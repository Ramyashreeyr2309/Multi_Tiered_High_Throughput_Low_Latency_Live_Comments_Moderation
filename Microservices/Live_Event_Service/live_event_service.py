import asyncio
import os
from pathlib import Path as LocalPath

from fastapi import FastAPI, status, Path, Header, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, EmailStr
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from Message_Queue_Util.producer import send_message
from Message_Queue_Util.producer import send_fanout_message
from Message_Queue_Util.consumer import start_consumer, start_fanout_consumer
from Message_Queue_Util.constants import (
    LIVE_EVENT_NOTIFICATIONS_EXCHANGE,
    TIER_2_LABELS_EXCHANGE,
)
import json
from threading import Thread
from pymongo import MongoClient, DESCENDING, ReturnDocument
from bson.objectid import ObjectId


# 1. Connect to MongoDB container
client = MongoClient(
    os.getenv("MONGODB_URI", "mongodb://host.docker.internal:27017/")
)

# 2. Select Database and Collection (automatically created if they don't exist)
db = client["maindb"]
comments_collection = db["Comments"]
live_events_collection = db["Live_events"]


def comment_for_ui(comment: dict, user_name: str | None = None) -> dict:
    return {
        "_id": str(comment["_id"]),
        "user_id": str(comment["user_id"]),
        "user_name": user_name,
        "timestamp": comment["timestamp"],
        "comment_content": comment["comment_content"],
    }


class ConnectionManager:
    def __init__(self):
        self.connections = {}
        self.user_connections = {}

    async def connect(self, event_id: str, user_id: str, websocket: WebSocket):
        await websocket.accept()
        self.connections.setdefault(event_id, set()).add(websocket)
        event_users = self.user_connections.setdefault(event_id, {})
        event_users.setdefault(user_id, set()).add(websocket)

    def disconnect(self, event_id: str, user_id: str, websocket: WebSocket):
        event_connections = self.connections.get(event_id)
        if event_connections is None:
            return
        event_connections.discard(websocket)
        if not event_connections:
            self.connections.pop(event_id, None)

        event_users = self.user_connections.get(event_id, {})
        user_connections = event_users.get(user_id)
        if user_connections is not None:
            user_connections.discard(websocket)
            if not user_connections:
                event_users.pop(user_id, None)
            if not event_users:
                self.user_connections.pop(event_id, None)

    async def broadcast(self, event_id: str, comment: dict):
        for websocket in list(self.connections.get(event_id, set())):
            try:
                await websocket.send_json(comment)
            except Exception:
                user_id = next(
                    (
                        connected_user
                        for connected_user, user_connections in self.user_connections.get(event_id, {}).items()
                        if websocket in user_connections
                    ),
                    None,
                )
                if user_id is not None:
                    self.disconnect(event_id, user_id, websocket)

    async def send_to_user(self, event_id: str, user_id: str, comment: dict):
        user_connections = list(
            self.user_connections.get(event_id, {}).get(user_id, set())
        )
        for websocket in user_connections:
            try:
                await websocket.send_json(comment)
            except Exception:
                self.disconnect(event_id, user_id, websocket)


connection_manager = ConnectionManager()
server_loop = None


def publish_live_event_notification(
    delivery: str,
    event_id: str,
    payload: dict,
    user_id: str | None = None,
):
    notification = {
        "delivery": delivery,
        "event_id": str(event_id),
        "payload": payload,
    }
    if user_id is not None:
        notification["user_id"] = str(user_id)
    send_fanout_message(notification, LIVE_EVENT_NOTIFICATIONS_EXCHANGE)


def process_live_event_notification(ch, method, _properties, body):
    """Deliver a fanout notification to sockets connected to this instance."""
    notification = json.loads(body.decode("utf-8"))
    delivery = notification["delivery"]
    event_id = notification["event_id"]
    payload = notification["payload"]

    if delivery not in ("broadcast", "send_to_user"):
        raise ValueError(f"Unsupported live event delivery type: {delivery}")

    if server_loop and not server_loop.is_closed():
        if delivery == "broadcast":
            coroutine = connection_manager.broadcast(event_id, payload)
        else:
            coroutine = connection_manager.send_to_user(
                event_id,
                notification["user_id"],
                payload,
            )
        asyncio.run_coroutine_threadsafe(coroutine, server_loop)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def process_classified_comment(ch, method, properties, body):
    """Handle a comment received from the Tier 1 output queue."""
    comment = json.loads(body.decode('utf-8'))
    commentTimestamp = datetime.fromisoformat(comment['timestamp']).astimezone(timezone.utc)
    print(" [<--] Received classified comment:", comment)
    prob = comment['prob']
    if (prob>=0.6):
        print("toxic comment")
        comment['Is_Toxic']=True
    elif 0.35 <= prob < 0.6:
        comment['Is_Toxic']=None
    else:
        print(comment['comment_content'])
        comment['Is_Toxic']=False
    processingTime = datetime.now(timezone.utc).timestamp() - commentTimestamp.timestamp()
    comment_object = {
        'user_id': ObjectId(comment['user_id']), # Convert user_id to ObjectId before inserting into MongoDB
        'event_id': ObjectId(comment['event_id']),
        'timestamp': commentTimestamp,
        'comment_content': comment['comment_content'],  
        'Is_Toxic': comment['Is_Toxic'],
        'resolved_by': 'tier 1',
        'tier_1_toxic_probability': comment['prob'],
        'tier_1_processing_time_ms': int(processingTime * 1000)
    }
    result = comments_collection.insert_one(comment_object)
    print(f"Inserted document ID: {result.inserted_id}")
    ui_comment = comment_for_ui(
        {**comment_object, "_id": result.inserted_id},
        user_name=comment.get("user_name"),
    )
    if comment['Is_Toxic'] is None:
        message = {
                    'comment_id': str(result.inserted_id),
                    'comment_content': comment['comment_content'],
                    'event_id': comment['event_id'],
                    'user_name': comment.get('user_name')
                   }
        send_message(message,"Tier_2_In_Queue")

    event_id = comment.get("event_id")
    if comment['Is_Toxic'] is None and event_id:
        publish_live_event_notification(
            "send_to_user",
            event_id,
            jsonable_encoder(ui_comment, custom_encoder={ObjectId: str}),
            user_id=comment["user_id"],
        )
    elif comment['Is_Toxic'] is False and event_id:
        publish_live_event_notification(
            "broadcast",
            event_id,
            jsonable_encoder(ui_comment, custom_encoder={ObjectId: str}),
        )
        
    ch.basic_ack(delivery_tag=method.delivery_tag)

def process_tier_2_classified_comment(ch, method, properties, body):
    """Handle a comment received from the Tier 2 output queue."""
    comments = json.loads(body.decode('utf-8'))
    feedback_comments = []
    for comment in comments:
        comment_id = comment['comment_id']
        is_toxic = comment['Is_Toxic']
        if not isinstance(is_toxic, bool):
            raise ValueError(f"Invalid Tier 2 toxicity label for comment {comment_id}")
        updated_comment = comments_collection.find_one_and_update(
            {'_id': ObjectId(comment_id)},
            [{'$set': {
                'Is_Toxic': is_toxic,
                'resolved_by': 'tier 2',
                'tier_2_processing_time_ms': {'$subtract': ['$$NOW', '$timestamp']}
            }}],
            return_document=ReturnDocument.AFTER)

        if updated_comment is None:
            raise LookupError(f"Comment {comment_id} was not found for Tier 2 update")

        feedback_comments.append(
            {
                "comment_id": comment_id,
                "comment_content": updated_comment["comment_content"],
                "Is_Toxic": is_toxic,
            }
        )

        event_id = updated_comment.get("event_id")
        if updated_comment['Is_Toxic'] and event_id:
            removal = {
                "type": "remove_comment",
                "comment_id": str(updated_comment["_id"]),
            }
            publish_live_event_notification(
                "send_to_user",
                event_id,
                removal,
                user_id=updated_comment["user_id"],
            )
        elif not updated_comment['Is_Toxic'] and event_id:
            ui_comment = comment_for_ui(
                updated_comment,
                user_name=comment.get("user_name"),
            )
            publish_live_event_notification(
                "broadcast",
                event_id,
                jsonable_encoder(ui_comment, custom_encoder={ObjectId: str}),
            )

    if feedback_comments:
        send_fanout_message(feedback_comments, TIER_2_LABELS_EXCHANGE)

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
    Thread(
            target=start_consumer,
            args=("Tier_2_Out_Queue", process_tier_2_classified_comment),
            daemon=True,
        ).start()
    Thread(
        target=start_fanout_consumer,
        args=(
            LIVE_EVENT_NOTIFICATIONS_EXCHANGE,
            process_live_event_notification,
        ),
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
def post_comment(
    comment: CommentCreate,
    event_id: str = Path(...),
    user_id: str = Header(..., alias="x-user-id"),
    user_name: str = Header(..., alias="x-user-name")
):
    comment_data = comment.model_dump(mode="json")
    comment_data["event_id"] = event_id
    comment_data["user_id"] = user_id
    comment_data["user_name"] = user_name
    comment_data["timestamp"] = datetime.now(timezone.utc).isoformat()
    send_message(comment_data,"Tier_1_In_Queue")
    return {"message": "Comment created",
            "data":comment,
            "event_id":event_id}

@app.get("/live_events/{event_id}/comments")
def get_comments(event_id: str = Path(...), cursor: str | None = None):
    limit = 20
    match_filter = {"event_id": ObjectId(event_id), "Is_Toxic": False}
    if cursor is not None:
        if not ObjectId.is_valid(cursor):
            raise HTTPException(status_code=422, detail="Invalid comment cursor")
        match_filter["_id"] = {"$lt": ObjectId(cursor)}

    recent_comments = list(comments_collection.aggregate([
        {"$match": match_filter},
        {"$sort": {"_id": DESCENDING}},
        {"$limit": limit},
        {
            "$lookup": {
                "from": "Users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "_user",
            }
        },
        {
            "$addFields": {
                "user_name": {
                    "$arrayElemAt": ["$_user.user_name", 0]
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "user_id": 1,
                "user_name": 1,
                "timestamp": 1,
                "comment_content": 1,
            }
        },
    ]))

    return jsonable_encoder(recent_comments, custom_encoder={ObjectId: str})


@app.websocket("/ws/live_events/{event_id}")
async def live_event_comments(websocket: WebSocket, event_id: str):
    user_id = websocket.headers.get("x-user-id")
    if not user_id:
        await websocket.close(code=1008, reason="Authentication required")
        return

    await connection_manager.connect(event_id, user_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connection_manager.disconnect(event_id, user_id, websocket)


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
