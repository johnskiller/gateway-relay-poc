import zenoh
import time

session = zenoh.open()

def send_data(shard_id, original_key, value):
    print(f"Sending to {shard_id} (Original Key: {original_key})")
    # Put the original Key into the Attachment
    session.put(shard_id, value, attachment=original_key.encode())

try:
    while True:
        # Simulate sending to different shards
        for i in range(10):
            send_data(f"shard/p{i}", f"tenant/sensor/{i}", f"data-{i}")
            time.sleep(0.5)
except KeyboardInterrupt:
    session.close()