# NYC Taxi Pipeline — Databricks Data Engineering Project

End-to-end data engineering pipeline built on Databricks Community Edition,
processing NYC Yellow Taxi trip data using the Medallion Architecture (Bronze → Silver → Gold).

## Architecture
Raw Files (Parquet)
↓
Bronze Layer  →  Raw data ingested as Delta Tables
↓
Silver Layer  →  Cleaned and transformed data
↓
Gold Layer    →  Aggregated data ready for analysis

## Tech Stack
- Apache Spark / PySpark
- Delta Lake
- Databricks Community Edition
- Python 3

## Dataset
NYC Yellow Taxi Trip Records — January & February 2024  
Source: [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Project Structure
```
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_aggregations.py
├── data/
│   ├── raw/          # Local Parquet files (not tracked by Git)
│   └── sample/       # Small data samples for reference
├── docs/
│   └── architecture.png
└── README.md
```

## Status
🚧 In progress