# lambda_glue_orchestrator.py

import boto3
import time
import os
import json

# Initialize boto3 clients
glue = boto3.client('glue')
dynamodb = boto3.resource('dynamodb')

# Environment variables set in the Lambda configuration
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME')
CHECKPOINT_TABLE = os.environ.get('CHECKPOINT_TABLE')  # e.g., "GlueJobCheckpoints"
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', 3))
INPUT_PATH = os.environ.get('INPUT_PATH')       # e.g., "s3://your-bucket/raw-parquet-logs/"
OUTPUT_PATH = os.environ.get('OUTPUT_PATH')     # e.g., "s3://your-bucket/processed-parquet-data/"

def lambda_handler(event, context):
    # Retrieve the last checkpoint (if applicable)
    checkpoint_table = dynamodb.Table(CHECKPOINT_TABLE)
    checkpoint_resp = checkpoint_table.get_item(Key={'job_name': GLUE_JOB_NAME})
    last_checkpoint = checkpoint_resp.get('Item', {}).get('last_successful_run', None)
    print(f"Last checkpoint: {last_checkpoint}")
    
    # Start the Glue job run with appropriate arguments
    try:
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--input_path': INPUT_PATH,
                '--output_path': OUTPUT_PATH,
                # Optionally pass the last_checkpoint for incremental processing
                '--last_checkpoint': last_checkpoint if last_checkpoint else ""
            }
        )
        job_run_id = response['JobRunId']
        print(f"Started Glue job {GLUE_JOB_NAME} with run ID: {job_run_id}")
    except Exception as e:
        print(f"Failed to start Glue job: {e}")
        raise

    # Poll for job status with a retry mechanism
    retries = 0
    while retries < MAX_RETRIES:
        job_status = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)['JobRun']['JobRunState']
        print(f"Current job status: {job_status}")
        if job_status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break
        time.sleep(15)  # Wait before polling again
        retries += 1

    if job_status == 'SUCCEEDED':
        print("Glue job succeeded.")
        # Update the checkpoint with the current run's timestamp (or use job metadata)
        checkpoint_table.put_item(Item={
            'job_name': GLUE_JOB_NAME,
            'last_successful_run': int(time.time())
        })
        return {
            'statusCode': 200,
            'body': json.dumps(f"Glue job {GLUE_JOB_NAME} run {job_run_id} succeeded.")
        }
    else:
        # Handle failure (e.g., send notifications, trigger retries)
        error_message = f"Glue job {GLUE_JOB_NAME} run {job_run_id} failed with state: {job_status}"
        print(error_message)
        # Optionally, publish to SNS or log the error to CloudWatch
        raise Exception(error_message)
