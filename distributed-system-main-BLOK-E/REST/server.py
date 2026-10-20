
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import time
import threading
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "rest_log.csv")

FIELDS = [
    "timestamp_iso",
    "node_id",
    "protocol",
    "t_send_ms",
    "t_recv_ms",
    "payload_bytes",
    "suhu",
    "sequence_no",
]

# Penyimpanan pesan untuk endpoint GET /data
messages = []
next_message_id = 1
data_lock = threading.Lock()


def write_log(data):
    new_file = (
        not os.path.exists(LOG_FILE)
        or os.path.getsize(LOG_FILE) == 0
    )

    with open(LOG_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)

        if new_file:
            writer.writeheader()

        writer.writerow(data)
        f.flush()
        os.fsync(f.fileno())


@app.route("/sensor", methods=["POST"])
def receive_sensor():
    global next_message_id

    payload = request.get_json(silent=True)

    if not isinstance(payload, dict):
        return jsonify({"error": "JSON tidak valid"}), 400

    try:
        t_recv_ms = int(time.time() * 1000)
        raw = request.get_data()

        row = {
            "timestamp_iso": datetime.now(timezone.utc).isoformat(),
            "node_id": str(payload["node_id"]),
            "protocol": str(payload.get("protocol", "REST")).lower(),
            "t_send_ms": int(payload["t_send_ms"]),
            "t_recv_ms": t_recv_ms,
            "payload_bytes": len(raw),
            "suhu": float(payload["suhu"]),
            "sequence_no": int(payload["sequence_no"]),
        }

        # Simpan log CSV dan tambahkan pesan ke penyimpanan
        with data_lock:
            write_log(row)

            message = {
                "message_id": next_message_id,
                "node_id": row["node_id"],
                "sequence_no": row["sequence_no"],
                "suhu": row["suhu"],
            }

            messages.append(message)
            next_message_id += 1

        print("REST diterima:", row)

        return jsonify({
            "status": "ok",
            "message_id": message["message_id"],
            "sequence_no": row["sequence_no"],
        }), 200

    except (KeyError, ValueError, TypeError, OSError) as e:
        app.logger.exception("Gagal memproses data REST")
        return jsonify({"error": str(e)}), 400


@app.route("/data", methods=["GET"])
def get_data():
    try:
        after = int(request.args.get("after", 0))

        if after < 0:
            return jsonify({"error": "Parameter after tidak boleh negatif"}), 400

    except ValueError:
        return jsonify({"error": "Parameter after harus berupa angka"}), 400

    with data_lock:
        result = [
            message.copy()
            for message in messages
            if message["message_id"] > after
        ]

    return jsonify(result), 200


if __name__ == "__main__":
    print(f"REST server aktif di port 5151")
    print(f"Log CSV: {LOG_FILE}")
    app.run(host="0.0.0.0", port=5151, debug=False)
