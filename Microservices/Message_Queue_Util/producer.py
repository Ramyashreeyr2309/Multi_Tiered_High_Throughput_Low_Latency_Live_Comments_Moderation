import os

import pika
import json
import time


def send_message(message_content, queue_name):
    parameters = pika.URLParameters(
        os.getenv(
            "RABBITMQ_URL",
            "amqp://admin:secretpass@host.docker.internal:5672/%2F",
        )
    )
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Declare durable queue (persists across broker restarts)
    channel.queue_declare(queue=queue_name, durable=True)

    # Convert dictionary to JSON string
    message_body = json.dumps(message_content)

    # Publish message with persistent delivery mode
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message_body,
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent, # Save message to disk
            content_type='application/json'
        )
    )

    print(f" Added into message queue {queue_name}")
    connection.close()


def send_fanout_message(message_content, exchange_name):
    parameters = pika.URLParameters(
        os.getenv(
            "RABBITMQ_URL",
            "amqp://admin:secretpass@host.docker.internal:5672/%2F",
        )
    )
    connection = pika.BlockingConnection(parameters)
    try:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=exchange_name,
            exchange_type="fanout",
            durable=True,
        )
        channel.confirm_delivery()
        channel.basic_publish(
            exchange=exchange_name,
            routing_key="",
            body=json.dumps(message_content),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )
        print(f" Added into fanout exchange {exchange_name}")
    finally:
        connection.close()


if __name__ == '__main__':
    # Test Payload
    sample_comment = {
        "comment_id": "c_101",
        "user_id": "u_882",
        "text": "This is a great stream!",
        "timestamp": time.time()
    }
    
    send_message(sample_comment)