import numpy as np
from transformers import AutoTokenizer
import onnxruntime as ort
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

LOCAL_MODEL_DIR = "./Tier_1_Service/minilm_onnx_quantized"

@asynccontextmanager
async def lifespan(_app: FastAPI):
    global logistic_regression_model,embedder,server_loop
    embedder = FastLocalONNXEmbedder(LOCAL_MODEL_DIR, "model_quantized.onnx")

    with open("Tier_1_Service/river_model.pkl", "rb") as f:
        logistic_regression_model = pickle.load(f)
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
    test_embeddings = embedder.encode(comment_content)
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
    test_embeddings=embedder.encode("Hello world")
    print(test_embeddings)
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