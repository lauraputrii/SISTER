#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 17:02:45 2024

@author: widhi
"""


import csv
import os
import time
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

LOG_FILE = os.path.join(os.path.dirname(__file__), "rest_log.csv")

FIELDS = [
    "timestamp_iso", "node_id", "protocol", "t_send_ms",
    "t_recv_ms", "payload_bytes", "suhu", "sequence_no"
]

def write_log(data):
    new_file = not os.path.exists(LOG_FILE)

    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow(data)

@app.route("/sensor", methods=["POST"])
def receive_sensor():
    payload = request.get_json(silent=True)

    if not payload:
        return jsonify({"error": "JSON tidak ditemukan"}), 400

    try:
        t_recv_ms = int(time.time() * 1000)
        raw = request.get_data()
        row = {
            "timestamp_iso": datetime.now(timezone.utc).isoformat(),
            "node_id": payload["node_id"],
            "protocol": "rest",
            "t_send_ms": int(payload["t_send_ms"]),
            "t_recv_ms": t_recv_ms,
            "payload_bytes": len(raw),
            "suhu": float(payload["suhu"]),
            "sequence_no": int(payload["sequence_no"])
        }

        write_log(row)
        print("REST diterima:", row)

        return jsonify({
            "status": "ok",
            "sequence_no": row["sequence_no"]
        }), 200

    except (KeyError, ValueError, TypeError) as e:
        return jsonify({"error": str(e)}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5151, debug=False)
