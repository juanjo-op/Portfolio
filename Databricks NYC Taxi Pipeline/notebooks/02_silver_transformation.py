# Databricks notebook source
# ============================================================
# NOTEBOOK: 02_silver_transformation
# PURPOSE: Clean and transform Bronze data into Silver layer
# ============================================================

# Source table from Bronze layer
BRONZE_TABLE = "main.default.bronze_yellow_taxi"

# Destination path for silver Delta Table
SILVER_PATH = 'main.default'

print('Configuration loaded successfully')
print(f'Source: {BRONZE_TABLE}')
print(f'Destination: {SILVER_PATH}')



# COMMAND ----------

# Read data from Bronze Delta Table
df_bronze = spark.read.table(BRONZE_TABLE)

# Count total rows before cleaning
total_before = df_bronze.count()

# Check for nulls in critical columns
from pyspark.sql.functions import col, sum as spark_sum
null_counts = df_bronze.select([
    # List comprehension calculation
    spark_sum(col(c).isNull().cast('int')).alias(c) for c in ["passenger_count", "trip_distance", "fare_amount", "tpep_pickup_datetime"]
]).collect()[0].asDict()

print(f'Total rows before cleaning: {total_before:,}')
print('\nNull counts per critical column:')
for column, count in null_counts.items():
    print(f'  {column}: {count:,}')

# COMMAND ----------

from pyspark.sql.functions import (
    col, hour, dayofweek, month, round as spark_round,
    when, to_date, unix_timestamp
)

df_silver = (df_bronze
             # Remove rows with null values in critical columns
             .filter(col("passenger_count").isNotNull())
             .filter(col("trip_distance").isNotNull())
             .filter(col('fare_amount').isNotNull())

             # Remove trips with invalid values
             .filter(col('passenger_count') > 0)
             .filter(col('trip_distance') > 0)
             .filter(col('fare_amount') > 0)
             .filter(col('total_amount') > 0)

             # Remove trips with negative or zero duration
             .filter(col('tpep_dropoff_datetime') > col('tpep_pickup_datetime'))

             # Add derived columns useful for analysis
             .withColumn('pickup_date', to_date(col('tpep_pickup_datetime')))
             .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
             .withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
             .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
             .withColumn('trip_duration_minutes',
                         spark_round(
                             (unix_timestamp(col('tpep_dropoff_datetime')) -
                              unix_timestamp(col('tpep_pickup_datetime'))) / 60, 2)
                         )
             .withColumn('speed_mph',
                         spark_round(col('trip_distance') / (col('trip_duration_minutes') / 60), 2)
             )
             .withColumn("tip_percentage",
                         spark_round((col("tip_amount") / col("fare_amount")) * 100, 2)
             )
)

total_after = df_silver.count()
rows_removed = total_before - total_after

print(f"Rows before cleaning : {total_before:,}")
print(f"Rows after cleaning  : {total_after:,}")
print(f"Rows removed         : {rows_removed:,} ({rows_removed/total_before*100:.1f}%)")

# COMMAND ----------

# Write cleaned data as a Delta Table in the Silver layer
(
    df_silver
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(f'{SILVER_PATH}.silver_yellow_taxi')
)

print('Silver table created successfully: main.default.silver_yellow_taxi')
print(f'Total rows written: {df_silver.count():,}')