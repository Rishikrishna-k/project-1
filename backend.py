from face_recognition import face_match  # Your face recognition function
import boto3
import os
import time

# ========== Configuration ==========
ASU_ID = "1232089042"
S3_INPUT_BUCKET = f"{ASU_ID}-in-bucket"   # Where images are uploaded
S3_OUTPUT_BUCKET = f"{ASU_ID}-out-bucket" # Where results are stored
REQUEST_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/390844739554/1232089042-req-queue'
RESPONSE_QUEUE_URL  = 'https://sqs.us-east-1.amazonaws.com/390844739554/1232089042-resp-queue'

# ========== AWS Clients ==========
aws_session = boto3.Session(region_name="us-east-1")
s3 = aws_session.client('s3')  # For file storage/download
sqs = aws_session.client('sqs')  # For task queues

# ========== Helper Functions ==========
def download_image_from_s3(filename: str) -> str:
    """Downloads an image from S3 to local storage."""
    try:
        print(f"[DEBUG] Downloading {filename} from S3...")
        local_path = f"/tmp/{filename}"  # Save in /tmp (recommended for AWS Lambda)
        s3.download_file(S3_INPUT_BUCKET, filename, local_path)
        return local_path
    except Exception as e:
        print(f"[ERROR] Failed to download {filename}: {e}")
        raise

def upload_result_to_s3(filename: str, result: str):
    """Uploads the recognition result to S3."""
    try:
        print(f"[DEBUG] Uploading result for {filename} to S3...")
        s3.put_object(Bucket=S3_OUTPUT_BUCKET, Key=filename, Body=result)
    except Exception as e:
        print(f"[ERROR] Failed to upload result for {filename}: {e}")
        raise

def send_result_to_queue(result: str):
    """Sends the recognition result to the response queue."""
    try:
        print(f"[DEBUG] Sending result to response queue: {result}")
        sqs.send_message(QueueUrl=RESPONSE_QUEUE_URL, MessageBody=result)
    except Exception as e:
        print(f"[ERROR] Failed to send result to queue: {e}")
        raise

def recognize_face(filename: str, image_path: str) -> str:
    """Runs face recognition and returns formatted result."""
    try:
        print(f"[DEBUG] Processing {filename}...")
        classification = face_match(image_path)[0]  # Your face recognition logic
        return f"{filename}:{classification}"
    except Exception as e:
        print(f"[ERROR] Face recognition failed for {filename}: {e}")
        return f"{filename}:error"

def fetch_next_task():
    """Fetches the next task from the request queue."""
    try:
        response = sqs.receive_message(
            QueueUrl=REQUEST_QUEUE_URL,
            MaxNumberOfMessages=1,
            VisibilityTimeout=15,
        )
        messages = response.get('Messages', [])
        if not messages:
            print("[DEBUG] No tasks in queue. Waiting...")
            return None
        
        message = messages[0]
        return {
            'filename': message['Body'],
            'receipt_handle': message['ReceiptHandle']
        }
    except Exception as e:
        print(f"[ERROR] Failed to fetch task from queue: {e}")
        return None

# ========== Main Worker Loop ==========
def process_tasks_forever():
    """Continuously processes tasks from the queue."""
    while True:
        try:
            # 1. Get next task
            task = fetch_next_task()
            if not task:
                time.sleep(5)  # Wait before checking again
                continue

            filename = task['filename']
            receipt_handle = task['receipt_handle']

            # 2. Download image
            image_path = download_image_from_s3(filename)

            # 3. Process image
            result = recognize_face(filename, image_path)

            # 4. Upload result
            upload_result_to_s3(filename, result)

            # 5. Send result to response queue
            send_result_to_queue(result)

            # 6. Cleanup
            os.remove(image_path)
            sqs.delete_message(
                QueueUrl=REQUEST_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
            print(f"[SUCCESS] Processed {filename}")

        except Exception as e:
            print(f"[CRITICAL] Task failed: {e}. Retrying...")
            time.sleep(5)

if __name__ == '__main__':
    print("[INFO] Starting face recognition worker...")
    process_tasks_forever()