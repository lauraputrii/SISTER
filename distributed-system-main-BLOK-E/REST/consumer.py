import csv
import os
import time
import requests
from datetime import datetime, timezone

SERVER_URL = os.getenv(
    "REST_DATA_URL",
    "https://curling-concept-roundup.ngrok-free.dev/data"
)

CONSUMER_ID = os.getenv("CONSUMER_ID", "REST-CONSUMER-1")
POLL_INTERVAL = 1.0

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, f"{CONSUMER_ID}_log.csv")

FIELDS = [
    "timestamp_iso",
    "consumer_id",
    "message_id",
    "node_id",
    "sequence_no",
    "suhu",
    "poll_rtt_ms",
]

with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
    csv.DictWriter(f, fieldnames=FIELDS).writeheader()

last_id = 0
start_time = time.perf_counter()
unique_messages = set()

print(f"{CONSUMER_ID} aktif")
print(f"Endpoint: {SERVER_URL}")
print(f"Log: {LOG_FILE}")

while True:
    request_start = time.perf_counter()

    try:
        response = requests.get(
            SERVER_URL,
            params={"after": last_id},
            timeout=5
        )
        response.raise_for_status()

        poll_rtt_ms = (
            time.perf_counter() - request_start
        ) * 1000

        messages = response.json()

        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)

            for message in messages:
                message_id = int(message["message_id"])

                if message_id <= last_id:
                    continue

                last_id = max(last_id, message_id)
                unique_messages.add(message_id)

                writer.writerow({
                    "timestamp_iso": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "consumer_id": CONSUMER_ID,
                    "message_id": message_id,
                    "node_id": message["node_id"],
                    "sequence_no": message["sequence_no"],
                    "suhu": message["suhu"],
                    "poll_rtt_ms": round(poll_rtt_ms, 3),
                })

        elapsed = time.perf_counter() - start_time
        throughput = (
            len(unique_messages) / elapsed if elapsed > 0 else 0
        )

        print(
            f"{CONSUMER_ID} | data_baru={len(messages)} | "
            f"total_unik={len(unique_messages)} | "
            f"poll_RTT={poll_rtt_ms:.2f} ms | "
            f"throughput={throughput:.3f} pesan/detik"
        )

    except (requests.RequestException, ValueError, KeyError) as error:
        print(f"{CONSUMER_ID} | ERROR: {error}")

    time.sleep(POLL_INTERVAL)