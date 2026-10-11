import pika
import json
import time

def process_comment(ch, method,  body):
    """Callback function triggered when a new message arrives."""
    comment = json.loads(body.decode('utf-8'))
    print(f" [-->] Received: ID '{comment['comment_id']}' - Text: '{comment['text']}'")

    # Simulate inference work (e.g., MiniLM embedding + River prediction)
    time.sleep(0.5) 
    
    print(f" [v] Processed comment ID: {comment['comment_id']}")

    # Manual Acknowledgment (signals RabbitMQ to safely remove message from queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_consumer(queue_name, process_message):
    credentials = pika.PlainCredentials('admin', 'secretpass')
    parameters = pika.ConnectionParameters(
        host='localhost', 
        port=5672, 
        credentials=credentials
    )
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=True)

    # Fair dispatch: do not give more than 1 unacknowledged message to a worker at a time
    channel.basic_qos(prefetch_count=1)

    # Bind queue to callback handler
    channel.basic_consume(queue=queue_name, on_message_callback=process_message)

    print(f" [*] {queue_name} Worker waiting for message.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n [*] Worker stopped.")
        channel.stop_consuming()
        connection.close()

if __name__ == '__main__':
    start_consumer()