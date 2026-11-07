import os

import pika
import json
import time


def _connection_parameters():
    return pika.URLParameters(
        os.getenv(
            "RABBITMQ_URL",
            "amqp://admin:secretpass@host.docker.internal:5672/%2F",
        )
    )


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
    connection = pika.BlockingConnection(_connection_parameters())
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




def start_batch_consumer(queue_name, process_message, batch_size, max_wait_time):
    connection = pika.BlockingConnection(_connection_parameters())
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    # Prefetch must be at least BATCH_SIZE so RabbitMQ sends 10 messages ahead
    channel.basic_qos(prefetch_count=batch_size)

    print(f"[*] Consumer started. Processing batches of {batch_size} instantly, or flushing partials every {max_wait_time}s.")

    # Use a very short inactivity timeout so the generator yields control quickly if empty
    message_generator = channel.consume(queue=queue_name, inactivity_timeout=0.05)

    try:
        batch = []
        last_flush_time = time.time()


        for method_frame, properties, body in message_generator:
            current_time = time.time()
            # 2. Time-check check: Evaluated on every message AND during idle timeout ticks
            time_elapsed = current_time - last_flush_time
            
            # 1. A new message arrived
            if method_frame is not None:
                batch.append((method_frame.delivery_tag, body))
                
                if len(batch) >= batch_size or time_elapsed >= max_wait_time:
                    print(f"Processing the batch of {len(batch)} now.")
                    
                    # --- PROCESS DATA ---
                    process_message(channel, batch)
                    #channel.basic_ack(delivery_tag=batch[-1][0], multiple=True)
                    batch.clear()
                    last_flush_time = current_time  # Reset clock

    except KeyboardInterrupt:
        print("\n[*] Stopping worker...")
        channel.cancel()

    connection.close()

    
if __name__ == '__main__':
    start_consumer()