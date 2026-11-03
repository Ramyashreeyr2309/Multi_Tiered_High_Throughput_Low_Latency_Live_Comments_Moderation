import pika
import time

BATCH_SIZE = 3
MAX_WAIT_TIME = 60.0  # 1 second fallback
QUEUE_NAME = 'test_queue'


credentials = pika.PlainCredentials('admin', 'secretpass')
parameters = pika.ConnectionParameters(
    host='localhost', 
    port=5672, 
    credentials=credentials
)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# Prefetch must be at least BATCH_SIZE so RabbitMQ sends 10 messages ahead
channel.basic_qos(prefetch_count=BATCH_SIZE)

print(f"[*] Consumer started. Processing batches of {BATCH_SIZE} instantly, or flushing partials every {MAX_WAIT_TIME}s.")

# Use a very short inactivity timeout so the generator yields control quickly if empty
message_generator = channel.consume(queue=QUEUE_NAME, inactivity_timeout=0.05)

try:
    batch = []
    last_flush_time = time.time()


    for method_frame, properties, body in message_generator:
        current_time = time.time()
        
        # 1. A new message arrived
        if method_frame is not None:
            batch.append((method_frame.delivery_tag, body))
            
            # CONDITION A: 10 messages reached -> PROCESS IMMEDIATELY
            if len(batch) >= BATCH_SIZE:
                print(f"[!] Target met. Processing full batch of {len(batch)} IMMEDIATELY.")
                
                # --- PROCESS DATA ---
                print(batch)
                
                channel.basic_ack(delivery_tag=batch[-1][0], multiple=True)
                print(batch[-1][0])
                batch.clear()
                last_flush_time = current_time  # Reset clock

        # 2. Time-check check: Evaluated on every message AND during idle timeout ticks
        time_elapsed = current_time - last_flush_time
        
        # CONDITION B: Time limit reached -> FLUSH WHATEVER IS THERE
        if time_elapsed >= MAX_WAIT_TIME:
            if batch:
                print(batch)
                print(f"[#] 1 second elapsed. Flushing partial batch of {len(batch)} messages.")
                
                # --- PROCESS DATA ---
                
                channel.basic_ack(delivery_tag=batch[-1][0], multiple=True)
                print(batch[-1][0])
                batch.clear()
            
            last_flush_time = current_time  # Reset clock

except KeyboardInterrupt:
    print("\n[*] Stopping worker...")
    channel.cancel()

connection.close()
