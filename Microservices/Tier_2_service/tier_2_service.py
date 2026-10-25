from Message_Queue_Util.consumer import start_consumer
import json
from Message_Queue_Util.producer import send_message
from fastapi import FastAPI, status, Path, Header
from contextlib import asynccontextmanager
from threading import Thread
import asyncio
from openai import OpenAI
import os

@asynccontextmanager
async def lifespan(_app: FastAPI):
    global client,server_loop
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    server_loop = asyncio.get_running_loop()
    Thread(
        target=start_consumer,
        args=("Tier_2_In_Queue", process_comment),
        daemon=True,
    ).start()
    yield

app = FastAPI(lifespan=lifespan)


def process_comment(ch, method, properties, body):
    comments=[
        {
            'id': '1',
            'comment_text': 'He is toxic'
        },
        {
            'id': '2',
            'comment_text': 'What a great performance'
        }
    ]
    response = client.responses.create(
        model="gpt-6-luna",
        input=str(comments),
        instructions='''
        check if the comments are toxic or not. give the response in json format - 
        [
            {
                id: '<id>'
                is_toxic: "true/false"
            },
            ...
        ]
        '''
    )

    print(response.output_text)