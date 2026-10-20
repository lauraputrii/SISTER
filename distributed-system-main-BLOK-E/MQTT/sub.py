#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 17:49:00 2024

@author: widhi
"""


import csv
import json
import os
import ssl
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(BASE_DIR, ".env")
LOG_FILE = os.path.join(BASE_DIR, "mqtt_log.csv")

credentials = {}
with open(ENV_FILE, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            credentials[key.strip()] = value.strip()

username = credentials.get("MQTT_USERNAME")
password = credentials.get("MQTT_PASSWORD")

if not username or not password:
    raise RuntimeError("Isi MQTT_USERNAME dan MQTT_PASSWORD di MQTT/.env")

BROKER = "k9f11cf1.ala.asia-southeast1.emqxsl.com"
PORT = 8883
TOPIC = "sister/temp"

FIELDS = [
    "timestamp_iso", "node_id", "protocol", "t_send_ms",
    "t_recv_ms", "payload_bytes", "suhu", "sequence_no"
]

def on_connect(client, userdata, flags, reason_code, properties):
    print("Subscriber terhubung:", reason_code)
    if reason_code == 0:
        client.subscribe(TOPIC, qos=1)
        print("Berlangganan:", TOPIC)

def on_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode("utf-8"))
        t_recv_ms = int(time.time() * 1000)

        row = {
            "timestamp_iso": datetime.now(timezone.utc).isoformat(),
            "node_id": payload["node_id"],
            "protocol": "mqtt",
            "t_send_ms": int(payload["t_send_ms"]),
            "t_recv_ms": t_recv_ms,
            "payload_bytes": len(message.payload),
            "suhu": float(payload["suhu"]),
            "sequence_no": int(payload["sequence_no"])
        }

        new_file = not os.path.exists(LOG_FILE)
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            if new_file:
                writer.writeheader()
            writer.writerow(row)

        print("MQTT diterima:", row)

    except (ValueError, KeyError, TypeError) as e:
        print("Pesan MQTT tidak valid:", e)

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="sister-subscriber-1"
)
client.username_pw_set(username, password)
client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(BROKER, PORT, keepalive=60)
    print("Menunggu pesan MQTT...")
    client.loop_forever()
except KeyboardInterrupt:
    print("\nMQTT subscriber dihentikan.")
finally:
    client.disconnect()
