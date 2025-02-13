# Scalable Data Pipeline for High-Volume Log Processing

This repository contains a modern, modular data pipeline that processes high-volume log data and real-time event streams. The pipeline leverages Apache Spark, Kafka, AWS Glue, and AWS Lambda to achieve low-latency processing, efficient ETL transformations, robust error handling, and checkpointing. It is designed for scalability and operational efficiency, ensuring data quality and consistency across both streaming and batch layers.

## Table of Contents
- [Overview](#overview)
- [Architecture & Data Flow](#architecture--data-flow)
- [Project Structure](#project-structure)
- [File Descriptions](#file-descriptions)
- [Setup & Prerequisites](#setup--prerequisites)
- [Usage](#usage)
- [Interview Explanation](#interview-explanation)
- [Learnings & Insights](#learnings--insights)
- [License](#license)

## Overview

This project demonstrates an end-to-end data pipeline that:

- **Ingests and processes log data in real time** using Spark Structured Streaming reading from Kafka.
- **Transforms and parses log data** into a structured format by applying a shared pre-processing module.
- **Persists the processed data** in Parquet format on Amazon S3, partitioned by event date for optimized storage and querying.
- **Executes batch ETL transformations and aggregations** with AWS Glue to calculate key metrics (e.g., average TTI and TTAR per page URL).
- **Orchestrates the ETL process** via an AWS Lambda function, which triggers the Glue job based on S3 events, handles error notifications, and manages checkpoints using DynamoDB.

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
      | - Parses JSON logs with   |
      |   a defined schema        |
      | - Applies shared          |
      |   pre-processing          |
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
      | (ETL Processing &         |
      |  Aggregation)             |
      | - Reads raw data from S3  |
      | - Applies pre-processing  |
      | - Aggregates key metrics  |
      | - Loads data into         |
      |   Amazon Redshift         |
      +--------------+------------+
                     │
                     ▼
      +------------------------------+
      | AWS Lambda Orchestrator      |
      | - Triggers Glue ETL Job      |
      | - Manages error handling,    |
      |   retries & checkpointing    |
      |   (using DynamoDB)           |
      +------------------------------+


### Flow Summary

1. **Real-Time Ingestion**  
   Spark Structured Streaming ingests log events from Kafka, converts the incoming JSON data into a structured format using a predefined schema, and applies a shared pre-processing module for data cleaning and enrichment. The processed data is written as partitioned Parquet files to Amazon S3, ensuring optimized storage and fast query performance.

2. **Batch ETL Processing**  
   An AWS Glue job picks up the raw data from S3, re-applies the pre-processing logic to maintain consistency, and performs aggregations such as calculating average TTI (Time To Interactive) and TTAR (Time To Articulate Response) per page URL. The aggregated results are then loaded into Amazon Redshift for business intelligence and analytics.

3. **Orchestration & Reliability**  
   An AWS Lambda function orchestrates the overall process by dynamically determining the input path from S3 events and triggering the Glue job. It also manages incremental processing through checkpoints stored in DynamoDB and implements error notifications, ensuring a robust and fault-tolerant pipeline.

## Project Structure

my-data-pipeline/
├── spark_streaming_ingest.py       # Spark Structured Streaming job for real-time ingestion from Kafka and writing to S3
├── glue_etl_job.py                 # AWS Glue ETL job for data transformation, aggregation, and loading into Redshift
├── lambda_glue_orchestrator.py     # AWS Lambda function for orchestrating Glue ETL jobs with error handling and checkpoint management
├── src/common/preprocessing.py     # Shared pre-processing module for data cleaning and enrichment
├── README.md                       # Project documentation (this file)
└── requirements.txt                # (Optional) Python dependencies for local testing

## File Descriptions
1. **spark_streaming_ingest.py**
   Uses Spark Structured Streaming to read data from a Kafka topic. The JSON log events are parsed using a predefined schema and processed with a shared pre-processing module. The resulting data is stored in Amazon S3 in Parquet format with partitioning by event date. Checkpointing is used to ensure fault tolerance and exactly-once processing.

2. **glue_etl_job.py**
   An AWS Glue ETL script that reads the raw Parquet data from S3, applies the same pre-processing logic, and aggregates key metrics such as average TTI and TTAR per page URL. The aggregated data is then loaded into Amazon Redshift for downstream analytics. The job includes error handling mechanisms that notify the operations team via an auxiliary Lambda function.

3. **lambda_glue_orchestrator.py**
   An AWS Lambda function that orchestrates the execution of the AWS Glue ETL job. It listens to S3 events to dynamically determine the input data path, retrieves the latest processing checkpoint from DynamoDB, and triggers the Glue job accordingly. This function ensures robust orchestration with error handling and automated retries.

4. **src/common/preprocessing.py**
   Contains the common logic for data cleaning and enrichment, which is used by both the streaming ingestion job and the batch ETL job to ensure consistency in data transformation across the pipeline.


## Setup & Prerequisites
1. Apache Spark configured with PySpark.
2. A Kafka cluster with a topic for log data.
3. An Amazon S3 bucket for data storage.
4. AWS Glue with the necessary IAM roles and permissions.
5. AWS Lambda configured with environment variables (e.g., for Glue job name, checkpoint table, and default input path).
6. A DynamoDB table for managing checkpoints (if using incremental processing).
7. Amazon Redshift for the data warehouse.


## Usage
1. Deploy Spark Streaming Job:
   Configure your Kafka parameters and S3 bucket details in spark_streaming_ingest.py and run the job to start ingesting real-time log data.

2. Deploy AWS Glue Job:
   Upload glue_etl_job.py to AWS Glue, set the job parameters (input path, Redshift URL, etc.), and schedule or trigger the job as needed.

3. Configure AWS Lambda Orchestrator:
   Deploy lambda_glue_orchestrator.py to AWS Lambda, set the required environment variables, and configure it to trigger based on S3 events for automated orchestration.


## Interview Explanation
During interviews, you can describe the pipeline as follows:

In this data pipeline, we integrate real-time ingestion with batch processing to efficiently manage high-volume log data. We use Spark Structured Streaming to read data from Kafka, where a strict schema ensures data consistency. The incoming JSON logs are parsed and enriched using a shared pre-processing module, which is used across both the streaming and batch layers. Processed data is stored as partitioned Parquet files in Amazon S3 for optimized storage. Later, an AWS Glue job reads this raw data, re-applies the pre-processing logic for consistency, and aggregates key performance metrics like average TTI and TTAR per page URL. The aggregated results are loaded into Amazon Redshift for analytics. An AWS Lambda function orchestrates the entire process by dynamically determining the input path from S3 events, retrieving checkpoints from DynamoDB for incremental processing, and triggering the Glue job. This design demonstrates a robust, scalable, and fault-tolerant approach to data processing and highlights best practices in modern data engineering.


## Learnings & Insights
This project underscores several important data engineering principles:

1. Modular Design: Using a shared pre-processing module ensures consistency and simplifies maintenance across different parts of the pipeline.
2. Fault Tolerance: Implementing checkpointing in Spark and using Lambda for error notifications help build a resilient system.
3. Scalability: Leveraging managed services such as AWS Glue, Lambda, and Redshift allows the pipeline to scale effortlessly with increasing data volumes.
4. Operational Efficiency: Dynamic orchestration via Lambda and incremental processing using DynamoDB checkpoints streamline the overall process and reduce manual intervention.
