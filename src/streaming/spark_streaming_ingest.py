# spark_streaming_ingest.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import sys
import os

# Import shared pre-processing module
from src.common import preprocessing

# Create the Spark session
spark = SparkSession.builder \
    .appName("KafkaToS3_HighThroughput_Ingestion") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "kafka-broker1:9092,kafka-broker2:9092"  
kafka_topic = "log_topic"

# Define the schema for incoming log data
log_schema = StructType([
    StructField("event_timestamp", TimestampType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("TTI", DoubleType(), True),
    StructField("TTAR", DoubleType(), True)
])

# Read data from Kafka using Spark Structured Streaming
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

# Parse the Kafka 'value' as JSON using the defined schema
df_logs = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), log_schema).alias("data")).select("data.*")

# Apply pre-processing using the common module
df_preprocessed = preprocessing.preprocess_data(df_logs)

# Write the preprocessed data to S3 as Parquet files
query = (
    df_preprocessed.writeStream
    .format("parquet")
    .option("checkpointLocation", "s3://your-bucket/checkpoints/")  
    .option("path", "s3://your-bucket/processed-data/")               
    .partitionBy("event_date")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)


query.awaitTermination()
