# AWS EMR Sales Analytics Pipeline

This project implements an end-to-end data engineering pipeline using AWS and Apache Spark.

## Architecture
S3 (Raw) → EMR (PySpark) → S3 (Silver & Gold Parquet) → Athena → QuickSight

## Features
- Incremental data processing using high-water-mark bookmark
- Silver and Gold data lake layers
- Daily revenue aggregation
- Querying via Athena
- Dashboard in QuickSight

## Technologies
- AWS S3
- AWS EMR
- PySpark
- Amazon Athena
- Amazon QuickSight

