import os

import pika
import json
import time
import logging

logger = logging.getLogger(__name__)

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
    logger.info("Received comment %s", comment['comment_id'])

    # Simulate inference work (e.g., MiniLM embedding + River prediction)
    time.sleep(0.5) 
    
    logger.info("Processed comment %s", comment['comment_id'])

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

    logger.info("%s worker waiting for messages", queue_name)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("%s worker stopped", queue_name)
        channel.stop_consuming()
        connection.close()


def start_fanout_consumer(exchange_name, process_message):
    connection = pika.BlockingConnection(_connection_parameters())
    try:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=exchange_name,
            exchange_type="fanout",
            durable=True,
        )
        queue_name = channel.queue_declare(
            queue="",
            exclusive=True,
            auto_delete=True,
        ).method.queue
        channel.queue_bind(exchange=exchange_name, queue=queue_name)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=process_message,
        )

        logger.info("%s fanout subscriber waiting for messages", exchange_name)
        channel.start_consuming()
    finally:
        if connection.is_open:
            connection.close()




def start_batch_consumer(queue_name, process_message, batch_size, max_wait_time):
    connection = pika.BlockingConnection(_connection_parameters())
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    # Prefetch must be at least BATCH_SIZE so RabbitMQ sends 10 messages ahead
    channel.basic_qos(prefetch_count=batch_size)

    logger.info(
        "Consumer started: batches of %d, flushing partial batches every %s seconds",
        batch_size,
        max_wait_time,
    )

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
                    logger.info("Processing a batch of %d comments", len(batch))
                    
                    # --- PROCESS DATA ---
                    process_message(channel, batch)
                    #channel.basic_ack(delivery_tag=batch[-1][0], multiple=True)
                    batch.clear()
                    last_flush_time = current_time  # Reset clock

    except KeyboardInterrupt:
        logger.info("Stopping %s worker", queue_name)
        channel.cancel()

    connection.close()


def start_fanout_batch_consumer(exchange_name, process_message, batch_size, max_wait_time):
    connection = pika.BlockingConnection(_connection_parameters())
    channel = connection.channel()
    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type="fanout",
        durable=True,
    )
    queue_name = channel.queue_declare(
        queue="",
        exclusive=True,
        auto_delete=True,
    ).method.queue
    channel.queue_bind(exchange=exchange_name, queue=queue_name)
    channel.basic_qos(prefetch_count=batch_size)

    logger.info(
        "%s fanout worker processing batches of %d, flushing every %s seconds",
        exchange_name,
        batch_size,
        max_wait_time,
    )
    message_generator = channel.consume(
        queue=queue_name,
        inactivity_timeout=0.1,
    )

    try:
        batch = []
        last_flush_time = time.monotonic()
        for method_frame, _properties, body in message_generator:
            current_time = time.monotonic()
            if method_frame is not None:
                batch.append((method_frame.delivery_tag, body))

            if batch and (
                len(batch) >= batch_size
                or current_time - last_flush_time >= max_wait_time
            ):
                process_message(channel, batch)
                batch.clear()
                last_flush_time = current_time
    except KeyboardInterrupt:
        logger.info("%s fanout worker stopped", exchange_name)
    finally:
        connection.close()

    
if __name__ == '__main__':
    start_consumer()