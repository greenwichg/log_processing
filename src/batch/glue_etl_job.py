# glue_etl_job.py

import sys
import json
import logging
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import avg, count, to_date, col, current_timestamp
import sys
import os

# Import pre-processing module
from src.common import preprocessing

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("glue_etl_job")

def notify_error(error_message):
    """
    Invoke a Lambda function for error notifications.
    """
    try:
        lambda_client = boto3.client('lambda', region_name='us-east-1')
        payload = {"error_message": error_message}
        lambda_client.invoke(
            FunctionName="your-lambda-function-name",  # Replace with your actual Lambda function name
            InvocationType="Event",
            Payload=json.dumps(payload)
        )
        logger.info("Error notification sent.")
    except Exception as ex:
        logger.error("Failed to send error notification: %s", str(ex))

try:
    # Expected parameters: JOB_NAME, input_path, redshift_url, redshift_dbtable, redshift_temp_dir
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 
        'input_path', 
        'redshift_url', 
        'redshift_dbtable', 
        'redshift_temp_dir'
    ])

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    logger.info("Starting Glue ETL job with input path: %s", args['input_path'])

    # Read raw data from S3 (landing zone)
    df_raw = spark.read.parquet(args['input_path'])
    logger.info("Raw data read from S3.")

    # Apply pre-processing using the common module
    df_preprocessed = preprocessing.preprocess_data(df_raw)

    # Main processing: Aggregation per page_url
    df_agg = df_preprocessed.groupBy("page_url").agg(
        avg("TTI_seconds").alias("avg_tti"),
        avg("TTAR_seconds").alias("avg_ttar"),
        count("*").alias("event_count")
    )
    logger.info("Aggregated metrics computed.")

    # Write the aggregated results directly to Redshift
    df_agg.write \
          .format("com.databricks.spark.redshift") \
          .option("url", args['redshift_url']) \
          .option("dbtable", args['redshift_dbtable']) \
          .option("tempdir", args['redshift_temp_dir']) \
          .mode("overwrite") \
          .save()
    logger.info("Aggregated data written to Redshift.")

    job.commit()
    logger.info("Glue ETL job completed successfully.")

except Exception as e:
    logger.error("Glue ETL job failed: %s", str(e))
    notify_error(str(e))
    raise
