from river import compose
from river import preprocessing
from river import linear_model
from river import metrics
from river import stream
from sentence_transformers import SentenceTransformer
import pickle
import os
import time
from Message_Queue_Util.consumer import start_consumer
import json
from Message_Queue_Util.producer import send_message
from fastapi import FastAPI, status, Path, Header
from contextlib import asynccontextmanager
import asyncio
from threading import Thread


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global logistic_regression_model
    global minilm_model
    minilm_model = SentenceTransformer(
        "all-MiniLM-L6-v2", backend="onnx", model_kwargs={"file_name": "onnx/model.onnx"}
    )
    with open("Tier_1_Service/river_model.pkl", "rb") as f:
        logistic_regression_model = pickle.load(f)
    global server_loop
    server_loop = asyncio.get_running_loop()
    Thread(
        target=start_consumer,
        args=("Tier_1_In_Queue", process_comment),
        daemon=True,
    ).start()
    yield

app = FastAPI(lifespan=lifespan)

def classify_comment(comment_content):
    start:float = time.time() * 1000
    print('start:',start,' ms')
    test_embeddings = minilm_model.encode(comment_content)
    test_v = {i: val for i, val in enumerate(test_embeddings)}
    prob = logistic_regression_model.predict_proba_one(test_v).get(1, 0.0)
    stop:float = time.time() * 1000
    print('stop:',stop,' ms')
    total:float = stop - start
    print('Total Time : ',total,' ms')
    print(prob)
    print(f"\nLive Test Comment: '{comment_content}'")
    print(f"Predicted Toxicity Probability: {prob:.2%}")
    return prob

def process_comment(ch, method, properties, body):
    """Callback function triggered when a new message arrives."""
    comment = json.loads(body.decode('utf-8'))
    #print(f" [-->] Received: ID '{comment['comment_id']}' - Text: '{comment['text']}'")
    print(comment)
    # Simulate inference work (e.g., MiniLM embedding + River prediction)
    time.sleep(0.5) 
    
    #print(f" [v] Processed comment ID: {comment['comment_id']}")
    comment_content=comment['comment_content']
    print(comment_content)
    prob=classify_comment(comment_content)
    comment['prob']=prob
    send_message(comment,"Tier_1_Out_Queue")
     # Manual Acknowledgment (signals RabbitMQ to safely remove message from queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    


@app.get("/tier_1_health_check", include_in_schema=False)
def tier_1_healthcheck():
    return "Model is running"