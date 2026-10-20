
import os
import csv
import json
import time
import ssl
from pathlib import Path
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR.parent / "MQTT" / ".env"

BROKER = "k9f11cf1.ala.asia-southeast1.emqxsl.com"
PORT = 8883
TOPIC = "sister/temp"

CONSUMER_ID = os.getenv("CONSUMER_ID", "MQTT-CONSUMER-1")
CLIENT_ID = f"blokE-subscriber-{CONSUMER_ID}"
LOG_FILE = BASE_DIR / f"{CONSUMER_ID}_log.csv"

FIELDS = [
    "timestamp_iso",
    "consumer_id",
    "node_id",
    "sequence_no",
    "t_send_ms",
    "t_recv_ms",
    "latency_ms",
    "suhu",
    "topic",
    "qos",
    "payload_bytes",
    "error",
]


def load_env(path):
    if not path.exists():
        raise FileNotFoundError(f"File .env tidak ditemukan: {path}")

    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip().strip('"').strip("'")
    return result


def write_log(row):
    has_data = LOG_FILE.exists() and LOG_FILE.stat().st_size > 0

    with LOG_FILE.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not has_data:
            writer.writeheader()
        writer.writerow(row)
        f.flush()
        os.fsync(f.fileno())


config = load_env(ENV_FILE)
USERNAME = config.get("MQTT_USERNAME")
PASSWORD = config.get("MQTT_PASSWORD")

if not USERNAME or not PASSWORD:
    raise ValueError("MQTT_USERNAME atau MQTT_PASSWORD tidak ada di .env")

start_time = time.perf_counter()
received_count = 0


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"{CONSUMER_ID} gagal terhubung: {reason_code}")
        return

    print(f"{CONSUMER_ID} terhubung ke broker.")
    result, mid = client.subscribe(TOPIC, qos=1)
    if result != mqtt.MQTT_ERR_SUCCESS:
        print(f"Gagal subscribe ke topic {TOPIC}: kode={result}")
    else:
        print(f"Subscribe: {TOPIC} (QoS 1)")


def on_message(client, userdata, msg):
    global received_count

    t_recv_ms = int(time.time() * 1000)

    try:
        payload = json.loads(msg.payload.decode("utf-8"))

        required = ["node_id", "sequence_no", "t_send_ms", "suhu"]
        for key in required:
            if key not in payload:
                raise ValueError(f"Field wajib tidak ada: {key}")

        t_send_ms = int(payload["t_send_ms"])
        latency_ms = round(t_recv_ms - t_send_ms, 3)

        received_count += 1
        elapsed = time.perf_counter() - start_time
        throughput = received_count / elapsed if elapsed > 0 else 0

        row = {
            "timestamp_iso": datetime.now(timezone.utc).isoformat(),
            "consumer_id": CONSUMER_ID,
            "node_id": payload["node_id"],
            "sequence_no": payload["sequence_no"],
            "t_send_ms": t_send_ms,
            "t_recv_ms": t_recv_ms,
            "latency_ms": latency_ms,
            "suhu": payload["suhu"],
            "topic": msg.topic,
            "qos": msg.qos,
            "payload_bytes": len(msg.payload),
            "error": "",
        }

        write_log(row)

        print(
            f"{CONSUMER_ID} | node={row['node_id']} "
            f"| seq={row['sequence_no']} "
            f"| latency={latency_ms:.2f} ms "
            f"| throughput={throughput:.3f} pesan/detik "
            f"| CSV tersimpan"
        )

    except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
        print(f"Pesan tidak valid: {error}")

    except OSError as error:
        print(f"GAGAL MENULIS CSV {LOG_FILE}: {error}")


client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id=CLIENT_ID,
    protocol=mqtt.MQTTv311,
)
client.username_pw_set(USERNAME, PASSWORD)
client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
client.reconnect_delay_set(min_delay=1, max_delay=30)
client.on_connect = on_connect
client.on_message = on_message

print(f"Subscriber: {CONSUMER_ID}")
print(f"Broker: {BROKER}:{PORT}")
print(f"Topic: {TOPIC}")
print(f"CSV: {LOG_FILE}")

try:
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_forever()

except KeyboardInterrupt:
    print("\nSubscriber dihentikan.")

finally:
    client.disconnect()
