
import base64
import json
import os
import requests
from flask import Flask, request, jsonify
from google.auth import default
from google.auth.transport.requests import Request as GoogleAuthRequest

app = Flask(__name__)

COMPOSER_AIRFLOW_URI = os.environ["COMPOSER_AIRFLOW_URI"].rstrip("/")
AIRFLOW_DAG_ID = "customer_lakehouse_event_pipeline"
ALLOWED_BUCKET = os.environ["GCS_BUCKET_NAME"]


@app.route("/", methods=["POST"])
def handle_pubsub():
    envelope = request.get_json(silent=True)

    if not envelope or "message" not in envelope:
        return jsonify({"error": "Invalid Pub/Sub message"}), 400

    try:
        message = envelope["message"]
        payload = json.loads(
            base64.b64decode(message["data"]).decode("utf-8")
        )

        bucket = payload.get("bucket")
        object_name = payload.get("name", "")

        if bucket != ALLOWED_BUCKET:
            return jsonify({"status": "ignored", "reason": "wrong bucket"}), 200

        if not object_name.lower().endswith(".csv"):
            return jsonify({"status": "ignored", "reason": "not a CSV"}), 200

        if not object_name.startswith("incoming/") or ".." in object_name:
            return jsonify({"status": "ignored", "reason": "invalid object path"}), 200

        credentials, _ = default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        credentials.refresh(GoogleAuthRequest())

        url = (
            f"{COMPOSER_AIRFLOW_URI}/api/v1/dags/"
            f"{AIRFLOW_DAG_ID}/dagRuns"
        )

        response = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {credentials.token}",
                "Content-Type": "application/json",
            },
            json={
                "dag_run_id": f"gcs_upload_{message.get('messageId', object_name.replace('/', '_'))}",
                "conf": {"object_name": object_name},
            },
            timeout=30,
        )

        if response.status_code not in (200, 201):
            app.logger.error(
                "Airflow trigger failed: %s %s",
                response.status_code,
                response.text,
            )
            return jsonify({"error": "Airflow trigger failed"}), 500

        return jsonify({
            "status": "triggered",
            "dag_id": AIRFLOW_DAG_ID,
            "object_name": object_name,
        }), 200

    except Exception:
        app.logger.exception("Failed to process Pub/Sub event")
        return jsonify({"error": "Event processing failed"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
