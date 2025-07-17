import base64
import json
import os
from flask import Flask, make_response, request, jsonify
import functions_framework
from google.cloud import storage
from google.cloud import firestore
import pandas as pd

app = Flask(__name__)
db = firestore.Client()

@functions_framework.http
def hello_http(request):
    envelope = request.get_json()
    if envelope and "message" in envelope:
        pubsub_message = envelope["message"]
        data = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
        file_info = json.loads(data)

        # CASE 2: CloudEvents format
    elif envelope and "data" in envelope:
        data = envelope["data"]
        file_info = data if isinstance(data, dict) else json.loads(data)

    else:
        return make_response(jsonify({"error": "Invalid message format, missing required fields"}), 400)

    # GCS info

    #file_info = json.loads(data)
    bucket = file_info["bucket"]
    name = file_info["name"]

    # Only process CSVs
    if not name.lower().endswith(".csv"):
        return jsonify({"error": "Only .csv files are allowed"}), 400

    doc_ref = db.collection("processed_files").document(name)
    if doc_ref.get().exists:
        return jsonify({"message": f"{name} already processed. Skipping."}), 200

    # ——— Process CSV File (Placeholder) ———
    print(f"Processing file: gs://{bucket}/{name}")
    # Here you would typically download the file from GCS, process it, 
    # transform it, and insert it into BigQuery or another service.
    # For now, we just log the action.     
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket)
        blob = bucket.blob(name)

        # Download CSV content into memory
        csv_bytes = blob.download_as_bytes()

        # Read CSV into DataFrame
        df = pd.read_csv(pd.io.common.BytesIO(csv_bytes))

        
    # ——— Idempotency: Save that we’ve processed this file ———
        doc_ref.set({
            "bucket": bucket,
            "filename": name,
            "processed_at": firestore.SERVER_TIMESTAMP
        })

        return make_response(jsonify({
            "message": "CSV read successful",
            "rows": len(df),
            "columns": df.columns.tolist()
        }), 200)

    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)
    

    