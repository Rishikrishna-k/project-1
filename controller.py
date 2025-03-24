import boto3
import time
from time import sleep

# ========== Configuration ==========
ASU_ID = '1232089042'
MAX_INSTANCES = 15  # Maximum number of worker instances
AMI_ID = 'ami-0f738cfef11d583e8'  # Amazon Machine Image ID
KEY_NAME = 'web-instance'  # Your EC2 key pair name
# SQS Queues
REQUEST_QUEUE = 'https://sqs.us-east-1.amazonaws.com/390844739554/1232089042-req-queue'
RESPONSE_QUEUE = 'https://sqs.us-east-1.amazonaws.com/390844739554/1232089042-resp-queue'

# ========== AWS Clients ==========
session = boto3.Session(region_name="us-east-1")
sqs = session.client('sqs')
ec2 = session.resource('ec2')

# ========== Helper Functions ==========
def get_queue_length():
    """Returns the approximate number of messages in the request queue"""
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=REQUEST_QUEUE,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        count = int(response['Attributes']['ApproximateNumberOfMessages'])
        print(f"[DEBUG] Queue has {count} messages")
        return count
    except Exception as e:
        print(f"[ERROR] Failed to get queue length: {e}")
        return 0  # Default to 0 if there's an error

def get_active_instances():
    """Returns all running/pending instances with the AppTier tag"""
    try:
        instances = ec2.instances.filter(
            Filters=[
                {'Name': 'tag:AppTier', 'Values': ['true']},
                {'Name': 'instance-state-name', 'Values': ['running', 'pending']}
            ]
        )
        instance_list = list(instances)
        print(f"[DEBUG] Found {len(instance_list)} active instances")
        return instance_list
    except Exception as e:
        print(f"[ERROR] Failed to fetch instances: {e}")
        return []

def launch_instance(instance_number):
    """Launches a new EC2 worker instance"""
    try:
        print(f"[INFO] Launching worker instance #{instance_number}")
        
        # User data script that runs when instance starts
        user_data_script = """#!/bin/bash
            sudo -u ec2-user nohup /usr/bin/python3 /home/ec2-user/project-1/backend.py > /home/ec2-user/project-1/backend.log 2>&1 &
            """
        
        instance = ec2.create_instances(
            ImageId=AMI_ID,
            MinCount=1,
            MaxCount=1,
            InstanceType='t2.micro',
            KeyName=KEY_NAME,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [
                    {'Key': 'Name', 'Value': f'app-tier-instance-{instance_number}'},
                    {'Key': 'AppTier', 'Value': 'true'}
                ]
            }],
            UserData=user_data_script
        )
        print(f"[SUCCESS] Launched instance {instance[0].id}")
        return instance
    except Exception as e:
        print(f"[ERROR] Failed to launch instance: {e}")
        return None

def terminate_instances(instance_ids):
    """Terminates specified EC2 instances"""
    if not instance_ids:
        return
        
    print(f"[INFO] Terminating {len(instance_ids)} instances")
    try:
        ec2.instances.filter(InstanceIds=instance_ids).terminate()
        print(f"[SUCCESS] Termination command sent for instances")
    except Exception as e:
        print(f"[ERROR] Failed to terminate instances: {e}")

# ========== Scaling Logic ==========
def manage_scaling():
    """
    Main scaling logic:
    - Scales up when queue has messages
    - Scales down when queue is empty
    """
    try:
        # 1. Check current queue length
        message_count = get_queue_length()
        
        # 2. Determine required workers
        workers_needed = min(MAX_INSTANCES, message_count)
        if message_count == 0:
            workers_needed = 0  # Scale down completely if queue is empty
            
        print(f"[INFO] Messages: {message_count} | Workers needed: {workers_needed}")
        
        # 3. Get current running instances
        current_instances = get_active_instances()
        current_count = len(current_instances)
        
        # 4. Scale up if needed
        if current_count < workers_needed:
            new_workers = workers_needed - current_count
            print(f"[SCALE UP] Adding {new_workers} workers")
            for i in range(new_workers):
                launch_instance(current_count + i + 1)
        
        # 5. Scale down if needed
        elif current_count > workers_needed:
            excess = current_count - workers_needed
            print(f"[SCALE DOWN] Removing {excess} workers")
            instances_to_terminate = [inst.id for inst in current_instances[workers_needed:]]
            terminate_instances(instances_to_terminate)
            
    except Exception as e:
        print(f"[CRITICAL] Scaling error: {e}")

# ========== Main Loop ==========
if __name__ == "__main__":
    print("[INFO] Starting auto-scaler...")
    print(f"[CONFIG] Max instances: {MAX_INSTANCES}")
    print(f"[CONFIG] AMI: {AMI_ID}")
    print(f"[CONFIG] Queue: {REQUEST_QUEUE}")
    
    while True:
        manage_scaling()
        sleep(1)  