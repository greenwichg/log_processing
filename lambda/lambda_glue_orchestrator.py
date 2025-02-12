# lambda_glue_orchestrator.py

import boto3
import os
import json

# Initialize AWS clients
glue = boto3.client('glue')
dynamodb = boto3.resource('dynamodb')

# Environment variables
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME')
CHECKPOINT_TABLE = os.environ.get('CHECKPOINT_TABLE')  # e.g., "GlueJobCheckpoints"
DEFAULT_INPUT_PATH = os.environ.get('INPUT_PATH')       # e.g., "s3://your-bucket/raw-landing-zone/"

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))
    
    # Determine input path: use S3 event details if available
    s3_input_path = None
    if 'Records' in event:
        for record in event['Records']:
            if record.get('eventSource') == "aws:s3":
                bucket = record.get('s3', {}).get('bucket', {}).get('name')
                key = record.get('s3', {}).get('object', {}).get('key')
                s3_input_path = f"s3://{bucket}/{key}"
                print(f"S3 event triggered: bucket={bucket}, key={key}")
                break

    glue_input_path = s3_input_path if s3_input_path else DEFAULT_INPUT_PATH

    # Optionally, retrieve the last checkpoint from DynamoDB (if using incremental processing)
    checkpoint_table = dynamodb.Table(CHECKPOINT_TABLE)
    checkpoint_resp = checkpoint_table.get_item(Key={'job_name': GLUE_JOB_NAME})
    last_checkpoint = checkpoint_resp.get('Item', {}).get('last_successful_run', "")

    try:
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--input_path': glue_input_path,
                '--last_checkpoint': last_checkpoint
            }
        )
        job_run_id = response['JobRunId']
        print(f"Started Glue job {GLUE_JOB_NAME} with run ID: {job_run_id}")
        return {
            'statusCode': 200,
            'body': json.dumps(f"Started Glue job {GLUE_JOB_NAME} with run ID: {job_run_id}")
        }
    except Exception as e:
        print(f"Failed to start Glue job: {e}")
        raise
