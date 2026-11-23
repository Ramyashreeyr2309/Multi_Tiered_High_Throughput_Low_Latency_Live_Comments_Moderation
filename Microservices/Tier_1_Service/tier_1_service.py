import numpy as np
import pandas as pd
import logging
from transformers import AutoTokenizer
import onnxruntime as ort
import pickle
import os
import time
import redis
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from Message_Queue_Util.consumer import start_consumer, start_fanout_batch_consumer
import json
from Message_Queue_Util.producer import send_message
from Message_Queue_Util.constants import TIER_2_LABELS_EXCHANGE
from fastapi import FastAPI, status, Path, Header
from contextlib import asynccontextmanager
import asyncio
from threading import Lock, Thread

LOCAL_MODEL_DIR = "./Tier_1_Service/minilm_onnx_quantized"
MODEL_PATH = os.path.join("Tier_1_Service", "river_model.pkl")
LEARNING_BATCH_SIZE = 10
LEARNING_MAX_WAIT_TIME = 2
MODEL_CACHE_KEY = "tier_1:river_model"
MODEL_WRITER_LOCK_KEY = f"{MODEL_CACHE_KEY}:writer"
MODEL_WRITER_LEASE_SECONDS = 60
MODEL_S3_BUCKET = os.getenv("S3_BUCKET")
MODEL_S3_KEY = os.getenv("TIER_1_MODEL_S3_KEY", "tier_1/river_model.pkl")
logger = logging.getLogger(__name__)
redis_client = redis.Redis.from_url(
    os.getenv("REDIS_URL", "redis://host.docker.internal:6379/0"),
    decode_responses=False,
)
s3_client = None
model_writer_lock = None
model_lock = Lock()

@asynccontextmanager
async def lifespan(_app: FastAPI):
    global logistic_regression_model,embedder,server_loop
    embedder = FastLocalONNXEmbedder(LOCAL_MODEL_DIR, "model_quantized.onnx")

    logistic_regression_model = load_model()
    server_loop = asyncio.get_running_loop()
    Thread(
        target=start_consumer,
        args=("Tier_1_In_Queue", process_comment),
        daemon=True,
    ).start()
    Thread(
        target=start_fanout_batch_consumer,
        args=(
            TIER_2_LABELS_EXCHANGE,
            process_tier_2_feedback_batch,
            LEARNING_BATCH_SIZE,
            LEARNING_MAX_WAIT_TIME,
        ),
        daemon=True,
    ).start()
    yield

app = FastAPI(lifespan=lifespan)

def load_model():
    try:
        cached_model = redis_client.get(MODEL_CACHE_KEY)
    except redis.RedisError as error:
        logger.warning("Redis model lookup failed; trying S3: %s", error)
        cached_model = None

    if cached_model is not None:
        try:
            return deserialize_model(cached_model)
        except (pickle.PickleError, EOFError, AttributeError, ImportError, IndexError, TypeError, ValueError) as error:
            logger.warning("Redis model could not be loaded; trying S3: %s", error)

    s3_model_missing = False
    if MODEL_S3_BUCKET:
        try:
            response = get_s3_client().get_object(Bucket=MODEL_S3_BUCKET, Key=MODEL_S3_KEY)
            return deserialize_model(response["Body"].read())
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code")
            if error_code in ("NoSuchKey", "404", "NotFound"):
                logger.info("Tier 1 model is not present in S3; loading the local pickle")
                s3_model_missing = True
            else:
                logger.warning("S3 model lookup failed; using local pickle: %s", error)
        except (BotoCoreError, OSError) as error:
            logger.warning("S3 model lookup failed; using local pickle: %s", error)
        except (pickle.PickleError, EOFError, AttributeError, ImportError, IndexError, TypeError, ValueError) as error:
            logger.warning("S3 model could not be loaded; using local pickle: %s", error)

    with open(MODEL_PATH, "rb") as model_file:
        serialized_model = model_file.read()

    model = deserialize_model(serialized_model)
    if s3_model_missing:
        upload_model_to_s3(serialized_model)
    return model


def get_s3_client():
    global s3_client
    if s3_client is None:
        s3_client = boto3.client("s3")
    return s3_client


def deserialize_model(serialized_model):
    model = pickle.loads(serialized_model)
    #normalize_model_feature_keys(model)
    return model


def upload_model_to_s3(serialized_model):
    if not MODEL_S3_BUCKET:
        return

    try:
        get_s3_client().put_object(
            Bucket=MODEL_S3_BUCKET,
            Key=MODEL_S3_KEY,
            Body=serialized_model,
            ContentType="application/octet-stream",
        )
    except (BotoCoreError, ClientError, OSError) as error:
        logger.warning("Could not upload the Tier 1 model to S3: %s", error)


def normalize_model_feature_keys(model):
    try:
        scaler = model.steps["StandardScaler"]
        classifier = model.steps["LogisticRegression"]
        feature_maps = (
            scaler.counts,
            scaler.means,
            scaler.vars,
            classifier._weights,
        )
    except (AttributeError, KeyError) as error:
        raise TypeError("Tier 1 model must be a StandardScaler/LogisticRegression pipeline") from error

    for feature_map in feature_maps:
        for feature_key in list(feature_map.keys()):
            if type(feature_key) is int:
                feature_map[str(feature_key)] = feature_map.pop(feature_key)


def is_model_writer():
    global model_writer_lock
    if model_writer_lock is not None:
        try:
            if model_writer_lock.owned():
                model_writer_lock.extend(MODEL_WRITER_LEASE_SECONDS, replace_ttl=True)
                return True
        except redis.RedisError as error:
            logger.warning("Could not renew Redis model-writer lease: %s", error)
        model_writer_lock = None

    try:
        candidate_lock = redis_client.lock(
            MODEL_WRITER_LOCK_KEY,
            timeout=MODEL_WRITER_LEASE_SECONDS,
            blocking=False,
            thread_local=False,
        )
        if candidate_lock.acquire(blocking=False):
            model_writer_lock = candidate_lock
            return True
    except redis.RedisError as error:
        logger.warning("Could not acquire Redis model-writer lease: %s", error)

    return False


def cache_model_if_writer():
    if not is_model_writer():
        return

    with model_lock:
        serialized_model = pickle.dumps(logistic_regression_model)

    try:
        if not redis_client.set(MODEL_CACHE_KEY, serialized_model):
            logger.error("Redis did not confirm the Tier 1 model cache update")
    except redis.RedisError as error:
        logger.warning("Could not cache the updated Tier 1 model in Redis: %s", error)

    upload_model_to_s3(serialized_model)


def classify_comment(comment_content):
    start:float = time.time() * 1000
    test_embeddings = embedder.encode(comment_content)
    test_v = {str(i): float(val) for i, val in enumerate(test_embeddings)}
    with model_lock:
        prob = logistic_regression_model.predict_proba_one(test_v).get(1, 0.0)
    stop:float = time.time() * 1000
    total:float = stop - start
    logger.debug(
        "Classified comment in %.2f ms with toxic probability %.2f%%",
        total,
        prob * 100,
    )
    return prob

def process_comment(ch, method, properties, body):
    """Callback function triggered when a new message arrives."""
    comment = json.loads(body.decode('utf-8'))
    logger.info("Received comment %s", comment.get('comment_id'))
    comment_content=comment['comment_content']
    prob=classify_comment(comment_content)
    comment['prob']=prob
    send_message(comment,"Tier_1_Out_Queue")
     # Manual Acknowledgment (signals RabbitMQ to safely remove message from queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    

def process_tier_2_feedback_batch(ch, batch):
    comments = []
    for _delivery_tag, body in batch:
        feedback = json.loads(body.decode("utf-8"))
        if not isinstance(feedback, list):
            raise ValueError("Tier 2 fanout feedback must be a list of labeled comments")
        comments.extend(feedback)

    comment_texts = [comment["comment_content"] for comment in comments]
    labels = [comment["Is_Toxic"] for comment in comments]

    if any(not isinstance(text, str) for text in comment_texts):
        raise ValueError("Tier 2 feedback comments must contain comment_content text")
    if any(not isinstance(label, bool) for label in labels):
        raise ValueError("Tier 2 feedback labels must be boolean")

    embeddings = [embedder.encode(text) for text in comment_texts]
    feature_columns = [str(i) for i in range(len(embeddings[0]))]
    features = pd.DataFrame(embeddings, columns=feature_columns)
    targets = pd.Series(labels, dtype=bool)

    with model_lock:
        logistic_regression_model.learn_many(features, targets)
    cache_model_if_writer()

    for delivery_tag, _ in batch:
        ch.basic_ack(delivery_tag=delivery_tag)


@app.get("/tier_1_health_check", include_in_schema=False)
def tier_1_healthcheck():
    return "Model is running"

class FastLocalONNXEmbedder:
    def __init__(self, model_dir: str, model_filename: str = "model_quantized.onnx"):
        # 1. Load the fast Rust-backed tokenizer locally
        self.tokenizer = AutoTokenizer.from_pretrained(model_dir)
        
        # 2. Configure ONNX Runtime for maximum CPU performance
        opts = ort.SessionOptions()
        opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        opts.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
        opts.intra_op_num_threads = 2  # Optimize thread allocation for FastAPI workers
        
        # 3. Load the C++ ONNX Inference Session
        model_path = os.path.join(model_dir, model_filename)
        self.session = ort.InferenceSession(
            model_path, 
            sess_options=opts, 
            providers=["CPUExecutionProvider"]
        )

    def encode(self, text: str) -> np.ndarray:
        # Step A: Fast Tokenization
        inputs = self.tokenizer(
            text, 
            padding=True, 
            truncation=True, 
            max_length=256, 
            return_tensors="np"
        )
        
        # Prepare ONNX input dictionary
        onnx_inputs = {
            "input_ids": inputs["input_ids"].astype(np.int64),
            "attention_mask": inputs["attention_mask"].astype(np.int64)
        }
        # Include token_type_ids if required by MiniLM architecture
        if "token_type_ids" in [node.name for node in self.session.get_inputs()]:
            onnx_inputs["token_type_ids"] = inputs["token_type_ids"].astype(np.int64)

        # Step B: Direct ONNX Execution (C++ engine)
        outputs = self.session.run(None, onnx_inputs)
        last_hidden_state = outputs[0]  # Shape: [batch_size, sequence_length, 384]

        # Step C: Fast Mean Pooling (averaging token vectors weighted by attention mask)
        input_mask_expanded = np.expand_dims(inputs["attention_mask"], -1)
        sum_embeddings = np.sum(last_hidden_state * input_mask_expanded, axis=1)
        sum_mask = np.clip(input_mask_expanded.sum(axis=1), a_min=1e-9, a_max=None)
        
        sentence_embedding = (sum_embeddings / sum_mask).flatten()
        return sentence_embedding