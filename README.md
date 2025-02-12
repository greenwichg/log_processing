# Scalable Data Pipeline for High-Volume Log Processing

This repository contains a scalable, serverless data pipeline built to process high-volume log data and real-time event streams. The pipeline leverages a combination of Apache Spark, Kafka, AWS Glue, and AWS Lambda to achieve low-latency processing, efficient ETL transformations, and robust orchestration with error handling and checkpointing.

## Table of Contents
- [Overview](#overview)
- [Architecture & Data Flow](#architecture--data-flow)
- [Project Structure](#project-structure)
- [File Descriptions](#file-descriptions)
- [Setup & Prerequisites](#setup--prerequisites)
- [Usage](#usage)
- [Learnings & Insights](#learnings--insights)
- [License](#license)

## Overview

This project demonstrates a full-fledged data pipeline that:

- **Ingests and processes log data in real time** using Apache Spark and Kafka.
- **Transforms and parses log data** into a structured format using PySpark.
- **Persists transformed data** in Parquet format to Amazon S3 for efficient storage and querying.
- **Executes further ETL transformations** and aggregations with AWS Glue.
- **Orchestrates and monitors** the ETL process via AWS Lambda, including error handling, automated retries, and checkpointing.

The end-to-end flow allows for handling millions of log events per day, ensuring data quality and low-latency processing for downstream analytics.

## Architecture & Data Flow

Below is an overview diagram of the data pipeline flow:

               +----------------+
               |  Kafka Topic   |
               | (Log Streams)  |
               +-------+--------+
                       │
                       ▼
          +---------------------------+
          | Apache Spark (PySpark)    |
          | Structured Streaming Job  |
          | - Reads from Kafka        |
          | - Parses & transforms logs|
          | - Writes Parquet to S3    |
          +--------------+------------+
                         │
                         ▼
          +---------------------------+
          |          Amazon S3        |
          | (Raw Parquet Data Storage)|
          +--------------+------------+
                         │
                         ▼
          +---------------------------+
          |        AWS Glue Job       |
          |    (ETL Processing and    |
          |     Data Aggregation)     |
          +--------------+------------+
                         │
                         ▼
          +---------------------------+
          |         Amazon S3         |
          |    (Processed Data for    |
          |    Analytics/BI Tools)    |
          +--------------+------------+
                         │
                         ▼
          +------------------------------+
          | AWS Lambda Orchestrator      |
          | - Triggers & monitors        |
          |   Glue ETL Job               |
          | - Implements error handling, |
          |   retries & checkpointing    |
          +------------------------------+


### Flow Summary

1. **Real-Time Ingestion**  
   - Spark Structured Streaming reads log events from Kafka.
   - Events are parsed from JSON and enriched with additional metadata (e.g., partitioning by date).
   - Transformed data is written as Parquet files to an S3 bucket.

2. **Batch ETL Processing**  
   - AWS Glue reads raw Parquet data from S3.
   - Data cleaning, transformation, and aggregations (e.g., average TTI per page) are performed.
   - Enriched data is stored back in S3 for further downstream consumption.

3. **Orchestration & Reliability**  
   - AWS Lambda triggers the Glue job and monitors its status.
   - On job success, the Lambda updates a checkpoint (e.g., in DynamoDB).
   - On failure, automated retries and error notifications are handled via Lambda.

## Project Structure

```plaintext
my-data-pipeline/
├── spark_streaming_ingest.py       # Apache Spark Structured Streaming job for real-time ingestion
├── glue_etl_job.py                 # AWS Glue ETL job script for further transformation & aggregation
├── lambda_glue_orchestrator.py     # AWS Lambda function for orchestrating Glue jobs, error handling, and checkpointing
├── README.md                       # Project documentation (this file)
└── requirements.txt                # (Optional) Python dependencies for local testing