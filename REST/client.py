
import csv
import json
import os
import time
from datetime import datetime

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "rest_log.csv")

URL = "https://curling-concept-roundup.ngrok-free.dev/sensor"
NODE_ID = "rest-node-1"
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


sequence_no = 0
print(f"REST publisher berjalan. CSV: {LOG_FILE}")
print(f"Kondisi jaringan: {NETWORK_CONDITION}")

try:
    while True:
        sequence_no += 1
        t_send_ms = int(time.time() * 1000)

        payload = {
            "node_id": NODE_ID,
            "protocol": "rest",
            "t_send_ms": t_send_ms,
            "suhu": round(25 + (sequence_no % 10) * 0.3, 2),
            "sequence_no": sequence_no,
        }

        payload_bytes = len(
            json.dumps(payload, separators=(",", ":")).encode("utf-8")
        )

        start = time.perf_counter()
        status = "ERROR"
        success = False
        latency_ms = ""
        error = ""

        try:
            response = requests.post(URL, json=payload, timeout=10)
            latency_ms = round((time.perf_counter() - start) * 1000, 3)
            status = response.status_code
            success = 200 <= response.status_code < 300

            print(
                f"REST seq={sequence_no}, status={status}, "
                f"latency={latency_ms} ms, success={success}"
            )

            if not success:
                error = response.text[:500]

        except requests.RequestException as e:
            latency_ms = round((time.perf_counter() - start) * 1000, 3)
            error = str(e)
            print(f"REST gagal seq={sequence_no}: {error}")

        row = {
            "timestamp": datetime.now().astimezone().isoformat(timespec="milliseconds"),
            "network_condition": NETWORK_CONDITION,
            "node_id": NODE_ID,
            "protocol": "rest",
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
    print("\nREST publisher dihentikan.")
