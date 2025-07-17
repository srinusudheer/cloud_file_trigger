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
    try:
        envelope = request.get_json(silent=True) # Use silent=True to avoid JSON parsing errors
        
        # Log the raw incoming request for debugging
        print(f"Received request headers: {request.headers}")
        print(f"Received request JSON envelope: {json.dumps(envelope, indent=2)}")

        file_info = {} # Initialize file_info

        if not envelope:
            print("Error: No JSON envelope found in request.")
            return make_response(jsonify({"error": "Invalid JSON payload"}), 400)

        # Standard Pub/Sub message format (Cloud Storage events typically arrive this way)
        if "message" in envelope:
            pubsub_message = envelope["message"]
            
            if "data" not in pubsub_message:
                print("Error: Pub/Sub message missing 'data' field.")
                return make_response(jsonify({"error": "Pub/Sub message missing data"}), 400)
                
            try:
                # The 'data' field in Pub/Sub is base64 encoded
                decoded_data_str = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
                print(f"Decoded Pub/Sub message data (CloudEvent JSON string): {decoded_data_str}")
                file_info = json.loads(decoded_data_str) # This should be the CloudEvent JSON
            except (base64.binascii.Error, json.JSONDecodeError) as e:
                print(f"Error decoding or parsing Pub/Sub message data: {e}")
                return make_response(jsonify({"error": f"Failed to decode or parse Pub/Sub data: {e}"}), 400)

        # Direct CloudEvent format (less common for Pub/Sub triggers unless explicitly configured)
        # This branch might not be needed if all events come via Pub/Sub from GCS
        elif "data" in envelope: # This would be if the *outer* envelope itself was a CloudEvent
            file_info = envelope["data"] if isinstance(envelope["data"], dict) else json.loads(envelope["data"])
            print(f"Received direct CloudEvent data: {json.dumps(file_info, indent=2)}")

        else:
            print("Error: Request envelope does not contain 'message' or 'data' field.")
            return make_response(jsonify({"error": "Invalid message format, missing 'message' or 'data' field"}), 400)

        # Ensure file_info contains bucket and name as expected from a Cloud Storage CloudEvent
        # Cloud Storage event "data" payload has 'bucket' and 'name' at the top level
        bucket_name = file_info.get("bucket")
        object_name = file_info.get("name") # 'name' in CloudEvent is the object path/name

        if not bucket_name or not object_name:
            print(f"Error: Missing bucket or name in file_info. file_info: {file_info}")
            return make_response(jsonify({"error": "Missing 'bucket' or 'name' in Cloud Storage event data"}), 400)

        # Only process CSVs
        if not object_name.lower().endswith(".csv"):
            print(f"Skipping file {object_name}: Not a CSV.")
            return jsonify({"message": f"Skipping {object_name}: Only .csv files are allowed"}), 200 # Return 200 as it's not an error

        # Idempotency check with Firestore
        doc_ref = db.collection("processed_files").document(object_name)
        if doc_ref.get().exists:
            print(f"Skipping file {object_name}: Already processed.")
            return jsonify({"message": f"{object_name} already processed. Skipping."}), 200

        # ——— Process CSV File ———
        print(f"Attempting to process file: gs://{bucket_name}/{object_name}")
        
        storage_client = storage.Client()
        gcs_bucket = storage_client.bucket(bucket_name) # Use gcs_bucket to avoid name clash
        blob = gcs_bucket.blob(object_name)

        # Check if blob exists before trying to download
        if not blob.exists():
            print(f"Error: Blob gs://{bucket_name}/{object_name} does not exist.")
            return make_response(jsonify({"error": f"File gs://{bucket_name}/{object_name} not found"}), 404)

        # Download CSV content into memory
        csv_bytes = blob.download_as_bytes()
        
        # Read CSV into DataFrame
        df = pd.read_csv(pd.io.common.BytesIO(csv_bytes))

        # ——— Idempotency: Save that we’ve processed this file ———
        doc_ref.set({
            "bucket": bucket_name,
            "filename": object_name,
            "processed_at": firestore.SERVER_TIMESTAMP,
            "rows_processed": len(df),
            "columns_found": df.columns.tolist()
        })

        print(f"Successfully processed {object_name}. Rows: {len(df)}, Columns: {df.columns.tolist()}")
        return make_response(jsonify({
            "message": "CSV read successful",
            "rows": len(df),
            "columns": df.columns.tolist()
        }), 200)

    except Exception as e:
        # Catch any unexpected errors and log them comprehensively
        import traceback
        print(f"An unhandled error occurred: {e}")
        print(traceback.format_exc()) # Print full traceback for debugging
        return make_response(jsonify({"error": f"Internal Server Error: {str(e)}"}), 500)