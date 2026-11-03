from Message_Queue_Util.consumer import start_batch_consumer
import json
from Message_Queue_Util.producer import send_message
from fastapi import FastAPI, status, Path, Header
from contextlib import asynccontextmanager
from threading import Thread
import asyncio
from openai import OpenAI
import os

batch_size = 10
max_wait_time = 5

@asynccontextmanager
async def lifespan(_app: FastAPI):
    global client,server_loop, batch_size
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    server_loop = asyncio.get_running_loop()
    Thread(
        target=start_batch_consumer,
        args=("Tier_2_In_Queue", process_batch_comments, batch_size, max_wait_time),
        daemon=True,
    ).start()
    yield

app = FastAPI(lifespan=lifespan)


def process_batch_comments(ch, batch):
    comments = []
    comment_id_map = {}
    for msg in batch:
        body = json.loads(msg[1].decode('utf-8'))
        comment_id=body.get('comment_id')
        comment={
            'id': comment_id,
            'comment_content': body.get('comment_content'),
            'event_id': body.get('event_id')
        }
        comments.append(comment)
        comment_id_map[comment_id] = body.get('user_name')

# { "user_id": "123456789",
# "comment_id": "123456,
# "comment_content": "What a ill manner behavior person shit",
# "event_id": "1"
# }

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

    output_json=json.loads(response.output_text)
    message = []
    for ele in output_json:
        comment_id=ele['id']
        if comment_id in comment_id_map.keys():
            user_name=comment_id_map.pop(comment_id)
            message.append({
                'comment_id': comment_id,
                'Is_Toxic': ele['is_toxic']=='true',
                'user_name': user_name
            })

    send_message(message, "Tier_2_Out_Queue")

    for msg in batch:
        if comment_id in comment_id_map.keys():
            ch.basic_nack(delivery_tag=msg[0])
        else:
            ch.basic_ack(delivery_tag=msg[0])

@app.get("/tier_2_health_check", include_in_schema=False)
def tier_2_healthcheck():
    return "Tier 2 is running"