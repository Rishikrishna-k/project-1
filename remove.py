import boto3
import logging

# AWS Configuration
AWS_REGION = "us-east-1"
ASU_ID = "1232089042"  # Replace with your ASU ID
S3_IN_BUCKET_NAME = f"{ASU_ID}-in-bucket"
S3_OUT_BUCKET_NAME = f"{ASU_ID}-out-bucket"
SQS_REQUEST_QUEUE = f"{ASU_ID}-req-queue"
SQS_RESPONSE_QUEUE = f"{ASU_ID}-resp-queue"

# AWS Clients
s3 = boto3.client("s3", region_name=AWS_REGION)
sqs = boto3.client("sqs", region_name=AWS_REGION)

# Get SQS Queue URLs
sqs_req_url = sqs.get_queue_url(QueueName=SQS_REQUEST_QUEUE)["QueueUrl"]
sqs_resp_url = sqs.get_queue_url(QueueName=SQS_RESPONSE_QUEUE)["QueueUrl"]

# Debugging - Logging Configuration
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

def delete_all_s3_objects(bucket_name):
    try:
        logging.info(f"Deleting all objects from S3 bucket: {bucket_name}")

        # List all objects in the bucket
        response = s3.list_objects_v2(Bucket=bucket_name)
        
        if "Contents" in response:
            for obj in response["Contents"]:
                s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
                logging.info(f"Deleted object: {obj['Key']}")
        else:
            logging.info(f"No objects found in the bucket {bucket_name}.")
    except Exception as e:
        logging.error(f"Error deleting S3 objects: {e}")

def delete_all_sqs_messages(queue_url):
    try:
        logging.info(f"Deleting all messages from SQS queue: {queue_url}")
        
        while True:
            # Receive a message from the queue
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,  # Max 10 messages per call
                WaitTimeSeconds=0
            )

            if "Messages" in response:
                for message in response["Messages"]:
                    # Delete each message
                    receipt_handle = message["ReceiptHandle"]
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                    logging.info(f"Deleted message: {message['MessageId']}")
            else:
                logging.info(f"No more messages in the queue {queue_url}.")
                break
    except Exception as e:
        logging.error(f"Error deleting SQS messages: {e}")

# Remove all objects from the specified S3 buckets
delete_all_s3_objects(S3_IN_BUCKET_NAME)
delete_all_s3_objects(S3_OUT_BUCKET_NAME)

# Remove all messages from the specified SQS queues
delete_all_sqs_messages(sqs_req_url)
delete_all_sqs_messages(sqs_resp_url)

logging.info("Cleanup completed.")
