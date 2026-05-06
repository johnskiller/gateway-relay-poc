import zenoh
import time

session = zenoh.open()

def send_data(shard_id, original_key, value):
    print(f"Sending to {shard_id} (Original: {original_key})")
    # 将原始 Key 放入 Attachment
    session.put(shard_id, value, attachment=original_key.encode())

try:
    while True:
        # 模拟发送到不同的分片
        for i in range(10):
            send_data(f"shard/p{i}", f"tenant/sensor/{i}", f"data-{i}")
            time.sleep(0.5)
except KeyboardInterrupt:
    session.close()