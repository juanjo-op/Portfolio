# Databricks notebook source
# ============================================================
# NOTEBOOK: 01_bronze_ingestion
# PURPOSE: Ingest raw Parquet files into Bronze Delta Tables
# ============================================================

# Source path where raw data is stored
RAW_PATH = "/Volumes/main/default/raw_data/"

# Destination path where bronze delta tables will be stored
BRONZE_PATH = "main.default"

print('Configuration loaded successfully')
print(f'Source: {RAW_PATH}')
print(f'Destination: {BRONZE_PATH}')

# COMMAND ----------

# Read both parquet into a single Spark dataframe
df_raw = spark.read.parquet(RAW_PATH)

# Show total number of rows loaded
print(f'Total rows loaded: {df_raw.count():,}')

# Display the first 5 rows
df_raw.show(5)

# COMMAND ----------

# print the schema to understand data types of each column
print('Schema of raw data:')
df_raw.printSchema()

# COMMAND ----------

# Write raw data as a delta table in the Bronze layer
# This preserves the original data without any transformations

(
    df_raw
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(f'{BRONZE_PATH}.bronze_yellow_taxi')
)

print('Bronze table created successfully: main.default.bronze_yellow_taxi')
print(f'Total rows written: {df_raw.count():,}')

# COMMAND ----------

