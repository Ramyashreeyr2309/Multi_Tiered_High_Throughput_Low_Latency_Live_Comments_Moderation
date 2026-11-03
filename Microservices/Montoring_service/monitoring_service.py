from fastapi import FastAPI, status, Path, Header, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, EmailStr
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path as LocalPath
from fastapi.responses import FileResponse
from pymongo import MongoClient, DESCENDING, ReturnDocument
import asyncio
from threading import Thread
from bson import ObjectId


client = MongoClient("mongodb://host.docker.internal:27017/")

# 2. Select Database and Collection (automatically created if they don't exist)
db = client["maindb"]
comments_collection = db["Comments"]
live_events_collection = db["Live_events"]
user_collection = db["Users"]

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.mongodb_client = MongoClient("mongodb://host.docker.internal:27017/")
    app.db = app.mongodb_client["maindb"]

    yield
    app.mongodb_client.close()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"message": "Hello World"}

@app.get("/admin_dashboard_page", include_in_schema=False)
def admin_page():
    page_path = LocalPath(__file__).with_name("admin_dashboard_page.html")
    return FileResponse(page_path, media_type="text/html")


@app.get("/live_events/list")
def get_live_events_list():
    events = list(
        live_events_collection.find({}, {"name": 1, "start_time": 1}).sort(
            "start_time", DESCENDING
        )
    )
    return jsonable_encoder(events, custom_encoder={ObjectId: str})


def get_comment_total(event_id: str = None):
    query_filter = {"event_id": ObjectId(event_id)} if event_id else {}
    return comments_collection.count_documents(query_filter)


@app.get("/comments/count")
def get_total_comments_count(event_id: str = None):
    if event_id is None:
        query_filter = {}
    else:
        query_filter = {"event_id": ObjectId(event_id)}
    total_comments = comments_collection.count_documents(query_filter)
    return {
        "collection": comments_collection.name,
        "total_comments": total_comments
    }

@app.get("/users/count")
def get_total_users_count():
    total_users = user_collection.count_documents({})
    return {
        "collection": user_collection.name,
        "total_users": total_users
    }

@app.get("/comments/toxic/count")
def get_toxic_comments_count(event_id: str = None):
    if event_id is None:
        query_filter = {"Is_Toxic": True}
    else:
        # Pass filter dictionary to match toxic comments
        query_filter = {"Is_Toxic": True, "event_id": ObjectId(event_id)}
    
    toxic_count = comments_collection.count_documents(query_filter)
    total_count = get_comment_total(event_id)

    return {
        "toxic_comments_count": toxic_count,
        "total_comments_count": total_count,
        "percentage_toxic": round((toxic_count / total_count * 100), 2) if total_count > 0 else 0
    }

@app.get("/comments/nontoxic/count")
def get_non_toxic_comments_count(event_id: str = None):
    if event_id is None:
        query_filter = {"Is_Toxic": False}
    else:
        query_filter = {"Is_Toxic": False, "event_id": ObjectId(event_id)}

    non_toxic_count = comments_collection.count_documents(query_filter)
    total_count = get_comment_total(event_id)

    return {
        "non_toxic_comments_count": non_toxic_count,
        "total_comments_count": total_count,
        "percentage_non_toxic": round((non_toxic_count / total_count * 100), 2) if total_count > 0 else 0
    }

@app.get("/comments/toxic/count/tier_1")
def get_toxic_comments_count(event_id: str = None):
    if event_id is None:
        query_filter = {"Is_Toxic": True, "resolved_by": "tier 1"}
    else:
        # Pass filter dictionary to match toxic comments
        query_filter = {"Is_Toxic": True, "resolved_by": "tier 1", "event_id": ObjectId(event_id)}
    
    toxic_count = comments_collection.count_documents(query_filter)
    total_count = get_comment_total(event_id)

    return {
        "toxic_comments_count": toxic_count,
        "total_comments_count": total_count,
        "percentage_toxic": round((toxic_count / total_count * 100), 2) if total_count > 0 else 0
    }

@app.get("/comments/nontoxic/count/tier_1")
def get_non_toxic_comments_count(event_id: str = None):
    if event_id is None:
        query_filter = {"Is_Toxic": False, "resolved_by": "tier 1"}
    else:
        # Pass filter dictionary to match non-toxic comments
        query_filter = {"Is_Toxic": False, "resolved_by": "tier 1", "event_id": ObjectId(event_id)}

    non_toxic_count = comments_collection.count_documents(query_filter)
    total_count = get_comment_total(event_id)

    return {
        "non_toxic_comments_count": non_toxic_count,
        "total_comments_count": total_count,
        "percentage_non_toxic": round((non_toxic_count / total_count * 100), 2) if total_count > 0 else 0
    }

@app.get("/comments/count/tier_1")
def get_comments_count_tier1(event_id: str = None):
    if event_id is None:
        query_filter = {"resolved_by": "tier 1"}
    else:
        # Pass filter dictionary to match non-toxic comments
        query_filter = {"resolved_by": "tier 1", "event_id": ObjectId(event_id)}
    
    tier1_count = comments_collection.count_documents(query_filter)

    return {
        "tier1_comments_count": tier1_count
    }

@app.get("/comments/missed_count/tier_1")
def get_comments_count_tier1(event_id: str = None):
    if event_id is None:
        query_filter = {"Is_Toxic": None, "resolved_by": "tier 1"}
    else:
        # Pass filter dictionary to match non-toxic comments
        query_filter = {"Is_Toxic": None, "resolved_by": "tier 1", "event_id": ObjectId(event_id)}

    missed_count = comments_collection.count_documents(query_filter)
    total_count = get_comment_total(event_id)

    return {
        "missed_comments_count": missed_count,
        "total_comments_count": total_count,
        "percentage_missed": round((missed_count / total_count * 100), 2) if total_count > 0 else 0
    }

@app.get("/comments/toxic/count/tier_2")
def get_toxic_comments_count(event_id: str = None):
    if event_id is None:
        query_filter = {"Is_Toxic": True, "resolved_by": "tier 2"}
    else:
        # Pass filter dictionary to match toxic comments
        query_filter = {"Is_Toxic": True, "resolved_by": "tier 2", "event_id": ObjectId(event_id)}

    toxic_count = comments_collection.count_documents(query_filter)
    total_count = get_comment_total(event_id)

    return {
        "toxic_comments_count": toxic_count,
        "total_comments_count": total_count,
        "percentage_toxic": round((toxic_count / total_count * 100), 2) if total_count > 0 else 0
    }

@app.get("/comments/nontoxic/count/tier_2")
def get_non_toxic_comments_count(event_id: str = None):
    if event_id is None:
        query_filter = {"Is_Toxic": False, "resolved_by": "tier 2"}
    else:
        # Pass filter dictionary to match non-toxic comments
        query_filter = {"Is_Toxic": False, "resolved_by": "tier 2", "event_id": ObjectId(event_id)}

    non_toxic_count = comments_collection.count_documents(query_filter)
    total_count = get_comment_total(event_id)

    return {
        "non_toxic_comments_count": non_toxic_count,
        "total_comments_count": total_count,
        "percentage_non_toxic": round((non_toxic_count / total_count * 100), 2) if total_count > 0 else 0
    }

@app.get("/comments/count/tier_2")
def get_comments_count_tier2(event_id: str = None):
    if event_id is None:
        query_filter = {"resolved_by": "tier 2"}
    else:
        # Pass filter dictionary to match non-toxic comments
        query_filter = {"resolved_by": "tier 2", "event_id": ObjectId(event_id)}

    tier2_count = comments_collection.count_documents(query_filter)

    return {
        "tier2_comments_count": tier2_count
    }

@app.get("/live_events/count/active")
def get_live_events_count():
    # Pass filter dictionary to match non-toxic comments
    query_filter = {"end_time": {"$exists": False}}

    live_events_count = live_events_collection.count_documents(query_filter)

    return {
        "live_events_count": live_events_count
    }

@app.get("/comments/tier_1/average_processing_time")
def get_tier1_average_processing_time(event_id: str = None):
    if event_id is None:
        query_filter = {"resolved_by": "tier 1"}
    else:
        # Pass filter dictionary to match tier 1 comments
        query_filter = {"resolved_by": "tier 1", "event_id": ObjectId(event_id)}

    tier1_comments = list(comments_collection.find(query_filter))

    if not tier1_comments:
        return {"average_processing_time_ms": 0}

    total_processing_time = sum(comment.get("tier_1_processing_time_ms", 0) for comment in tier1_comments)
    average_processing_time = total_processing_time / len(tier1_comments)

    return {"average_processing_time_ms": average_processing_time}

@app.get("/comments/tier_2/average_processing_time")
def get_tier2_average_processing_time(event_id: str = None):
    if event_id is None:
        query_filter = {"resolved_by": "tier 2"}
    else:
        query_filter = {"resolved_by": "tier 2", "event_id": ObjectId(event_id)}

    tier2_comments = list(comments_collection.find(query_filter))

    if not tier2_comments:
        return {"average_processing_time_ms": 0}

    total_processing_time = sum(comment.get("tier_2_processing_time_ms", 0) for comment in tier2_comments)
    average_processing_time = total_processing_time / len(tier2_comments)

    return {"average_processing_time_ms": average_processing_time}
@app.get("/comments/tier_1/p95_processing_time")
def get_tier1_p95_processing_time(event_id: str = None):
    if event_id is None:
        query_filter = {"resolved_by": "tier 1"}
    else:
        query_filter = {"resolved_by": "tier 1", "event_id": ObjectId(event_id)}

    tier1_comments = list(comments_collection.find(query_filter))

    if not tier1_comments:
        return {"p95_processing_time_ms": 0}
    pipeline = [
        {
            "$match": query_filter
        },
        {
            "$group": {
                "_id": None,
                "p95_result": {
                    "$percentile": {
                        "input": "$tier_1_processing_time_ms",
                        "p": [0.95],
                        "method": "approximate"
                    }
                }
            }
        }
    ]
    p95_processing_time = list(comments_collection.aggregate(pipeline))
    if not p95_processing_time:
        return {"p95_processing_time_ms": 0}

    return {"p95_processing_time_ms": p95_processing_time[0]["p95_result"]}

@app.get("/comments/tier_2/p95_processing_time")
def get_tier2_p95_processing_time(event_id: str = None):
    if event_id is None:
        query_filter = {"resolved_by": "tier 2"}
    else:
        query_filter = {"resolved_by": "tier 2", "event_id": ObjectId(event_id)}

    tier2_comments = list(comments_collection.find(query_filter))

    if not tier2_comments:
        return {"p95_processing_time_ms": 0}
    pipeline = [
        {
            "$match": query_filter
        },
        {
            "$group": {
                "_id": None,
                "p95_result": {
                    "$percentile": {
                        "input": "$tier_2_processing_time_ms",
                        "p": [0.95],
                        "method": "approximate"
                    }
                }
            }
        }
    ]
    p95_processing_time = list(comments_collection.aggregate(pipeline))
    if not p95_processing_time:
        return {"p95_processing_time_ms": 0}

    return {"p95_processing_time_ms": p95_processing_time[0]["p95_result"]}

@app.get("/comments/tier_1/p99_processing_time")
def get_tier1_p99_processing_time(event_id: str = None):
    # Pass filter dictionary to match tier 1 comments
    if event_id is None:
        query_filter = {"resolved_by": "tier 1"}
    else:
        query_filter = {"resolved_by": "tier 1", "event_id": ObjectId(event_id)}

    tier1_comments = list(comments_collection.find(query_filter))

    if not tier1_comments:
        return {"p99_processing_time_ms": 0}
    pipeline = [
        {
            "$match": query_filter
        },
        {
            "$group": {
                "_id": None,
                "p99_result": {
                    "$percentile": {
                        "input": "$tier_1_processing_time_ms",
                        "p": [0.99],
                        "method": "approximate"
                    }
                }
            }
        }
    ]
    p99_processing_time = list(comments_collection.aggregate(pipeline))
    if not p99_processing_time:
        return {"p99_processing_time_ms": 0}

    return {"p99_processing_time_ms": p99_processing_time[0]["p99_result"]}

@app.get("/comments/tier_2/p99_processing_time")
def get_tier2_p99_processing_time(event_id: str = None):
    # Pass filter dictionary to match tier 2 comments
    if event_id is None:
        query_filter = {"resolved_by": "tier 2"}
    else:
        query_filter = {"resolved_by": "tier 2", "event_id": ObjectId(event_id)}

    tier2_comments = list(comments_collection.find(query_filter))

    if not tier2_comments:
        return {"p99_processing_time_ms": 0}
    pipeline = [
        {
            "$match": query_filter
        },
        {
            "$group": {
                "_id": None,
                "p99_result": {
                    "$percentile": {
                        "input": "$tier_2_processing_time_ms",
                        "p": [0.99],
                        "method": "approximate"
                    }
                }
            }
        }
    ]
    p99_processing_time = list(comments_collection.aggregate(pipeline))
    if not p99_processing_time:
        return {"p99_processing_time_ms": 0}

    return {"p99_processing_time_ms": p99_processing_time[0]["p99_result"]}