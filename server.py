from fastapi import FastAPI, UploadFile, File
from starlette.responses import PlainTextResponse, JSONResponse
import boto3
import os
import asyncio
import threading

# ---------- Configuration ----------
ASU_ID = "1232089042"
S3_BUCKET_NAME = f"{ASU_ID}-in-bucket"
SIMPLEDB_DOMAIN = f"{ASU_ID}-simpleDB"
PORT = 8000
server_running = True
REQ_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/390844739554/1232089042-req-queue'
RESP_QUEUE_URL  = 'https://sqs.us-east-1.amazonaws.com/390844739554/1232089042-resp-queue'

# Store results and waiting events
RESULTS = {}  # Example: {"cat.jpg": "cat.jpg:dog"}
WAIT_EVENTS = {}  # Tracks files waiting for processing

# ========== AWS Clients ==========
aws_session = boto3.Session(region_name="us-east-1")
s3 = aws_session.client('s3')  # For file storage
sqs = aws_session.client('sqs')  # For task queues

# ========== FastAPI App ==========
app = FastAPI()

# ========== Helper Functions ==========
def upload_to_s3(file_content: bytes, filename: str):
    """Uploads a file to S3 bucket"""
    try:
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=filename, Body=file_content)
        print(f"[DEBUG] Uploaded {filename} to S3 bucket {S3_BUCKET_NAME}")
    except Exception as e:
        print(f"[ERROR] Failed to upload to S3: {e}")
        return False
    return True

def send_to_request_queue(filename: str):
    """Sends a filename to the SQS request queue"""
    try:
        sqs.send_message(QueueUrl=REQ_QUEUE_URL, MessageBody=filename)
        print(f"[DEBUG] Sent {filename} to request queue")
    except Exception as e:
        print(f"[ERROR] Failed to send to SQS: {e}")
        return False
    return True

def fetch_results_from_queue():
    """Continuously checks the response queue for results (runs in background)"""
    while True:
        try:
            # Check for new messages
            response = sqs.receive_message(
                QueueUrl=RESP_QUEUE_URL,
                MaxNumberOfMessages=10,
                VisibilityTimeout=5,
            )
            messages = response.get('Messages', [])

            if not messages:
                print("[DEBUG] No messages in response queue")
                continue

            # Process each message
            for msg in messages:
                receipt_handle = msg['ReceiptHandle']
                result = msg['Body']  # Format: "filename:classification"

                # Example: "cat.jpg:dog" → store in RESULTS
                filename, classification = result.split(':')
                RESULTS[filename] = f"{filename}:{classification}"
                print(f"[DEBUG] Received result: {filename} → {classification}")

                # Notify waiting requests
                if filename in WAIT_EVENTS:
                    WAIT_EVENTS[filename].set()

                # Delete processed message
                sqs.delete_message(QueueUrl=RESP_QUEUE_URL, ReceiptHandle=receipt_handle)

        except Exception as e:
            print(f"[ERROR] Failed to process SQS messages: {e}")

# ========== API Endpoint ==========
@app.post("/", response_class=PlainTextResponse)
async def predict_image(inputFile: UploadFile = File(...)):
    """
    Uploads an image, sends it for processing, and waits for the result.
    Returns format: "filename:classification"
    """
    try:
        # 1. Read uploaded file
        file_content = await inputFile.read()
        filename = inputFile.filename
        print(f"[DEBUG] Received file: {filename}")

        # 2. Upload to S3
        if not upload_to_s3(file_content, filename):
            return JSONResponse(status_code=500, content={"detail": "S3 upload failed"})

        # 3. Send filename to request queue
        if not send_to_request_queue(filename):
            return JSONResponse(status_code=500, content={"detail": "SQS send failed"})

        # 4. Wait for result (max 30 seconds)
        wait_event = asyncio.Event()
        WAIT_EVENTS[filename] = wait_event

    
        await wait_event.wait()
        result = RESULTS.pop(filename)
        file_name, prediction = result.split(':')
        return f"{file_name}:{prediction}"
    

    except Exception as e:
        print(f"[ERROR] Prediction failed: {e}")
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})

    finally:
        # Cleanup
        WAIT_EVENTS.pop(filename, None)
        RESULTS.pop(filename, None)

# ========== Start Background Thread ==========
# Runs fetch_results_from_queue() in the background
threading.Thread(target=fetch_results_from_queue, daemon=True).start()

# ========== Start Server ==========
if __name__ == "__main__":
    import uvicorn
    print(f"[INFO] Starting server on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
