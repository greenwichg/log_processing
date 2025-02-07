# spark_streaming_ingest.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create the Spark session
spark = SparkSession.builder \
    .appName("KafkaToS3Parquet") \
    .getOrCreate()

# Define the schema for log events (customize to your log format)
log_schema = StructType([
    StructField("event_timestamp", TimestampType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("TTI", DoubleType(), True),
    StructField("TTAR", DoubleType(), True)
])

# Read from Kafka topic
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker1:9092,kafka-broker2:9092") \
    .option("subscribe", "log_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Convert the binary value column to string and parse JSON
df_logs = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), log_schema).alias("data")).select("data.*")

# Add a derived column for partitioning (for example, by event date)
df_logs = df_logs.withColumn("event_date", to_date("event_timestamp"))

# Write stream as Parquet files partitioned by event_date in S3
query = df_logs.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3://your-bucket/checkpoints/") \
    .option("path", "s3://your-bucket/raw-parquet-logs/") \
    .partitionBy("event_date") \
    .outputMode("append") \
    .start()

query.awaitTermination()
