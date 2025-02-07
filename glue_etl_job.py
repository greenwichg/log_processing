# glue_etl_job.py (to be uploaded as an AWS Glue job script)

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import avg, count

# Parse job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read the raw Parquet data from S3
df_raw = spark.read.parquet(args['input_path'])

# Filter out any records with null metrics (for example)
df_filtered = df_raw.filter("TTI IS NOT NULL AND TTAR IS NOT NULL")

# Create a derived column, if needed (e.g., converting milliseconds to seconds)
df_transformed = df_filtered.withColumn("TTI_seconds", df_filtered["TTI"] / 1000)

# Aggregate example: compute average TTI per page_url and count events
df_agg = df_transformed.groupBy("page_url") \
    .agg(avg("TTI_seconds").alias("avg_tti"), count("*").alias("event_count"))

# Write the aggregated results to the output S3 location in Parquet format
df_agg.write.mode("overwrite").parquet(args['output_path'])
