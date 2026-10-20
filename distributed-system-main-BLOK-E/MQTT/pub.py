
import os
import csv
import json
import time
import random
import ssl
from pathlib import Path
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR.parent / "MQTT" / ".env"
LOG_FILE = BASE_DIR / "publisher_log.csv"

BROKER = "k9f11cf1.ala.asia-southeast1.emqxsl.com"
PORT = 8883
TOPIC = "sister/temp"
INTERVAL = 1.0

NODE_ID = os.getenv("NODE_ID", "MQTT-NODE-1")
CLIENT_ID = f"blokE-publisher-{NODE_ID}"

FIELDS = [
    "timestamp_iso",
    "node_id",
    "protocol",
    "sequence_no",
    "t_send_ms",
    "suhu",
    "topic",
    "qos",
    "status",
    "published",
    "latency_ms",
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

client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id=CLIENT_ID,
    protocol=mqtt.MQTTv311,
)
client.username_pw_set(USERNAME, PASSWORD)
client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
client.reconnect_delay_set(min_delay=1, max_delay=30)

connected = False


def on_connect(client, userdata, flags, reason_code, properties):
    global connected
    connected = not reason_code.is_failure
    if connected:
        print("Publisher berhasil terhubung ke broker.")
    else:
        print(f"Koneksi ditolak broker: {reason_code}")


client.on_connect = on_connect

sequence_no = 0

print(f"Publisher aktif: {NODE_ID}")
print(f"Broker: {BROKER}:{PORT}")
print(f"Topic: {TOPIC}")
print(f"CSV: {LOG_FILE}")

try:
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()

    # Tunggu koneksi awal agar pesan pertama tidak langsung gagal.
    deadline = time.monotonic() + 15
    while not connected and time.monotonic() < deadline:
        time.sleep(0.1)

    if not connected:
        raise RuntimeError("Tidak berhasil terhubung ke broker dalam 15 detik.")

    while True:
        sequence_no += 1
        t_send_ms = int(time.time() * 1000)
        suhu = round(random.uniform(25, 35), 2)

        payload = {
            "node_id": NODE_ID,
            "protocol": "MQTT",
            "t_send_ms": t_send_ms,
            "suhu": suhu,
            "sequence_no": sequence_no,
        }

        payload_json = json.dumps(payload, separators=(",", ":"))
        payload_bytes = len(payload_json.encode("utf-8"))
        start = time.perf_counter()

        status = "ERROR"
        published = False
        latency_ms = ""
        error_message = ""

        try:
            info = client.publish(TOPIC, payload_json, qos=1)

            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"Publish gagal, kode={info.rc}")

            info.wait_for_publish(timeout=10)
            latency_ms = round((time.perf_counter() - start) * 1000, 3)
            published = info.is_published()

            if published:
                status = "PUBACK_RECEIVED"
            else:
                status = "ACK_TIMEOUT"
                error_message = "Belum ada konfirmasi publikasi setelah 10 detik"

        except (ValueError, RuntimeError, mqtt.MQTTException) as error:
            latency_ms = round((time.perf_counter() - start) * 1000, 3)
            error_message = str(error)

        row = {
            "timestamp_iso": datetime.now(timezone.utc).isoformat(),
            "node_id": NODE_ID,
            "protocol": "MQTT",
            "sequence_no": sequence_no,
            "t_send_ms": t_send_ms,
            "suhu": suhu,
            "topic": TOPIC,
            "qos": 1,
            "status": status,
            "published": published,
            "latency_ms": latency_ms,
            "payload_bytes": payload_bytes,
            "error": error_message,
        }

        try:
            write_log(row)
            print(
                f"{NODE_ID} | seq={sequence_no} | status={status} "
                f"| CSV tersimpan"
            )
        except OSError as error:
            print(f"GAGAL MENULIS CSV: {error}")

        time.sleep(INTERVAL)

except KeyboardInterrupt:
    print("\nPublisher dihentikan.")

finally:
    client.disconnect()
    client.loop_stop()
