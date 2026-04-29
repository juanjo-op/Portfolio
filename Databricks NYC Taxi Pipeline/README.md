## 🔷 NYC Taxi Pipeline — Databricks Data Engineering

**Tools:** PySpark · Delta Lake · Databricks · Unity Catalog · Python

End-to-end data engineering pipeline processing ~6M NYC Yellow Taxi trip records 
using the Medallion Architecture (Bronze → Silver → Gold) on Databricks Community Edition.

**Key skills demonstrated:**
- Medallion Architecture (Bronze / Silver / Gold layers)
- Delta Lake: ACID transactions, Time Travel, VACUUM
- PySpark transformations at scale (~6M rows)
- Automated pipeline orchestration with Databricks Jobs
- SQL analytics and dashboard visualization

## Architecture
Raw Files (Parquet)
↓
Bronze Layer  →  Raw ingestion as Delta Table (5.97M rows)
↓
Silver Layer  →  Cleaned and transformed data (5.44M rows)
↓
Gold Layer    →  Aggregated data ready for analysis

## Dashboard Preview

![Dashboard Preview](docs/Dashboard_preview.png)

## Automated Pipeline

The full pipeline runs as an automated Databricks Job with three sequential tasks:

![Job Pipeline](docs/job_pipeline.png)

- **Trigger:** Weekly scheduled run
- **Duration:** ~2 minutes for full pipeline execution
- **Orchestration:** Task dependencies ensure Bronze → Silver → Gold order

## Key Findings

- **Peak hour**: 6 PM with 398K trips — classic Manhattan rush hour
- **Busiest day**: Thursday with 930K trips and highest revenue
- **Early morning anomaly**: Hour 5 has the longest avg distance (10.3 miles) — likely airport runs
- **January vs February**: Nearly identical volume (~2.72M trips each month)


## Tech Stack
- Apache Spark / PySpark
- Delta Lake
- Databricks Community Edition
- Python 3
- Databricks Jobs (workflow orchestration)
- Delta Lake Time Travel & VACUUM

## Dataset
NYC Yellow Taxi Trip Records — January & February 2024  
Source: [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Pipeline Details

| Layer  | Table | Rows | Description |
|--------|-------|------|-------------|
| Bronze | `bronze_yellow_taxi` | 5,972,150 | Raw Parquet ingestion |
| Silver | `silver_yellow_taxi` | 5,443,409 | Cleaned + derived columns |
| Gold | `gold_hourly_performance` | 24 | Aggregated by hour |
| Gold | `gold_daily_performance` | 7 | Aggregated by day of week |
| Gold | `gold_monthly_comparison` | 2 | January vs February |

## Project Structure
```
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_aggregations.py
│   └── 04_delta_time_travel.py
├── data/
│   ├── raw/        # Local Parquet files (not tracked by Git)
│   └── sample/
├── docs/
    ├── Dashboard_preview.png        # Local Parquet files (not tracked by Git)
│   └── job_pipeline.png
└── README.md
```

## Status
✅ Complete