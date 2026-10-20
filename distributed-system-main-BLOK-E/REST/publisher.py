
import os
import time
import random
import csv
import json
import requests
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SERVER_URL = os.getenv(
    "REST_SENSOR_URL",
    "http://127.0.0.1:5151/sensor"
)

NODE_ID = os.getenv("NODE_ID", "REST-NODE-1")
NETWORK_CONDITION = os.getenv(
    "NETWORK_CONDITION",
    "belum_diatur"
)

INTERVAL = 1.0
LOG_FILE = os.path.join(BASE_DIR, "rest_log.csv")

FIELDS = [
    "timestamp",
    "network_condition",
    "node_id",
    "protocol",
    "sequence_no",
    "t_send_ms",
    "suhu",
    "http_status",
    "success",
    "latency_ms",
    "payload_bytes",
    "error",
]


def write_log(data):
    file_exists = (
        os.path.exists(LOG_FILE)
        and os.path.getsize(LOG_FILE) > 0
    )

    with open(
        LOG_FILE,
        "a",
        newline="",
        encoding="utf-8-sig"
    ) as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)

        if not file_exists:
            writer.writeheader()

        writer.writerow(data)
        file.flush()
        os.fsync(file.fileno())


sequence_no = 0

print(f"REST publisher aktif: {NODE_ID}")
print(f"Tujuan: {SERVER_URL}")
print(f"Kondisi jaringan: {NETWORK_CONDITION}")
print(f"File log: {LOG_FILE}")

try:
    while True:
        sequence_no += 1

        t_send_ms = int(time.time() * 1000)
        suhu = round(random.uniform(25, 35), 2)

        payload = {
            "node_id": NODE_ID,
            "protocol": "REST",
            "t_send_ms": t_send_ms,
            "suhu": suhu,
            "sequence_no": sequence_no,
        }

        payload_bytes = len(
            json.dumps(
                payload,
                separators=(",", ":")
            ).encode("utf-8")
        )

        start = time.perf_counter()

        http_status = ""
        success = False
        latency_ms = ""
        error_message = ""

        try:
            response = requests.post(
                SERVER_URL,
                json=payload,
                timeout=5
            )

            latency_ms = round(
                (time.perf_counter() - start) * 1000,
                3
            )

            http_status = response.status_code
            success = 200 <= response.status_code < 300

            if not success:
                error_message = response.text[:500]

            print(
                f"{NODE_ID} | seq={sequence_no} | "
                f"HTTP={http_status} | "
                f"latency={latency_ms} ms | "
                f"success={success}"
            )

        except requests.RequestException as error:
            latency_ms = round(
                (time.perf_counter() - start) * 1000,
                3
            )

            error_message = str(error)

            print(
                f"{NODE_ID} | seq={sequence_no} | "
                f"ERROR={error_message}"
            )

        log_data = {
            "timestamp": datetime.now().astimezone().isoformat(
                timespec="milliseconds"
            ),
            "network_condition": NETWORK_CONDITION,
            "node_id": NODE_ID,
            "protocol": "REST",
            "sequence_no": sequence_no,
            "t_send_ms": t_send_ms,
            "suhu": suhu,
            "http_status": http_status,
            "success": success,
            "latency_ms": latency_ms,
            "payload_bytes": payload_bytes,
            "error": error_message,
        }

        try:
            write_log(log_data)
            print(f"Log tersimpan: {LOG_FILE}")
        except OSError as error:
            print(f"GAGAL MENULIS CSV: {error}")

        time.sleep(INTERVAL)

except KeyboardInterrupt:
    print("\nREST publisher dihentikan.")
