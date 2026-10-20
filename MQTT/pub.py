
import csv
import json
import os
import ssl
import time
import threading
from datetime import datetime

import paho.mqtt.client as mqtt

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(BASE_DIR, ".env")
LOG_FILE = os.path.join(BASE_DIR, "mqtt_log.csv")

BROKER = "k9f11cf1.ala.asia-southeast1.emqxsl.com"
PORT = 8883
TOPIC = "sister/temp"
NODE_ID = "mqtt-node-1"
INTERVAL = 1
NETWORK_CONDITION = os.getenv("NETWORK_CONDITION", "belum_diatur")

FIELDS = [
    "timestamp",
    "network_condition",
    "node_id",
    "protocol",
    "sequence_no",
    "t_send_ms",
    "suhu",
    "status",
    "success",
    "latency_ms",
    "payload_bytes",
    "error",
]


def write_log(row):
    file_exists = os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 0

    with open(LOG_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)
        f.flush()
        os.fsync(f.fileno())


# Baca username dan password dari .env
credentials = {}

with open(ENV_FILE, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            credentials[key.strip()] = value.strip().strip("\"'")

username = credentials.get("MQTT_USERNAME")
password = credentials.get("MQTT_PASSWORD")

if not username or not password:
    raise RuntimeError("Isi MQTT_USERNAME dan MQTT_PASSWORD di file .env")

connected = threading.Event()


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("MQTT publisher terhubung ke broker.")
        connected.set()
    else:
        print(f"Koneksi MQTT ditolak: {reason_code}")


client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="sister-publisher-1",
    protocol=mqtt.MQTTv311,
)

client.username_pw_set(username, password)
client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
client.on_connect = on_connect

sequence_no = 0

print(f"MQTT publisher berjalan. CSV: {LOG_FILE}")
print(f"Kondisi jaringan: {NETWORK_CONDITION}")

try:
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()

    if not connected.wait(timeout=15):
        raise RuntimeError("Koneksi MQTT gagal atau timeout.")

    while True:
        sequence_no += 1
        t_send_ms = int(time.time() * 1000)

        payload = {
            "node_id": NODE_ID,
            "protocol": "mqtt",
            "t_send_ms": t_send_ms,
            "suhu": round(25 + (sequence_no % 10) * 0.3, 2),
            "sequence_no": sequence_no,
        }

        payload_json = json.dumps(payload, separators=(",", ":"))
        payload_bytes = len(payload_json.encode("utf-8"))

        start = time.perf_counter()
        status = "ERROR"
        success = False
        latency_ms = ""
        error = ""

        try:
            info = client.publish(TOPIC, payload_json, qos=1)

            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(
                    f"Publish gagal dengan kode: {info.rc}"
                )

            info.wait_for_publish(timeout=10)
            latency_ms = round((time.perf_counter() - start) * 1000, 3)

            if info.is_published():
                status = "PUBACK_RECEIVED"
                success = True
            else:
                status = "ACK_TIMEOUT"
                error = "Belum menerima pengakuan publish dalam 10 detik"

            print(
                f"MQTT seq={sequence_no}, status={status}, "
                f"latency={latency_ms} ms"
            )

        except (ValueError, RuntimeError) as e:
            latency_ms = round((time.perf_counter() - start) * 1000, 3)
            error = str(e)
            print(f"MQTT gagal seq={sequence_no}: {error}")

        row = {
            "timestamp": datetime.now().astimezone().isoformat(timespec="milliseconds"),
            "network_condition": NETWORK_CONDITION,
            "node_id": NODE_ID,
            "protocol": "mqtt",
            "sequence_no": sequence_no,
            "t_send_ms": t_send_ms,
            "suhu": payload["suhu"],
            "status": status,
            "success": success,
            "latency_ms": latency_ms,
            "payload_bytes": payload_bytes,
            "error": error,
        }

        try:
            write_log(row)
        except OSError as e:
            print(f"GAGAL MENULIS CSV {LOG_FILE}: {e}")

        time.sleep(INTERVAL)

except KeyboardInterrupt:
    print("\nMQTT publisher dihentikan.")

finally:
    if client.is_connected():
        client.disconnect()

    client.loop_stop()
    print("MQTT publisher selesai.")
