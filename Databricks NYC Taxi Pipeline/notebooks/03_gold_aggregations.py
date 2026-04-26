# Databricks notebook source
# ============================================================
# NOTEBOOK: 03_gold_aggregations
# PURPOSE: Create aggregated Gold tables ready for analysis
# ============================================================

# Source table from Silver layer
SILVER_TABLE = 'main.default.silver_yellow_taxi'

# Destination for gold Delta Tables
GOLD_PATH  ='main.default'

print("Configuration loaded successfully")
print(f"Source: {SILVER_TABLE}")
print(f"Destination: {GOLD_PATH}")

# COMMAND ----------

from pyspark.sql.functions import col, avg, count, round as spark_round, sum as spark_sum

# Read Silver table
df_silver = spark.read.table(SILVER_TABLE)

# Gold Table 1: Hourly Performance aggregation
df_gold_hourly = (
    df_silver
    .groupBy('pickup_hour')
    .agg(
        count('*').alias('total_trips'),
        spark_round(avg('trip_distance'), 2).alias('avg_distance_miles'),
        spark_round(avg('fare_amount'), 2).alias('avg_fare_usd'),
        spark_round(avg('tip_amount'), 2).alias('avg_tip_usd'),
        spark_round(avg('trip_duration_minutes'), 2).alias('avg_duration_minutes'),
        spark_round(spark_sum('total_amount'), 2).alias('total_revenue_usd')
    )
    .orderBy('pickup_hour')
)

df_gold_hourly.show(24)

# COMMAND ----------

from pyspark.sql.functions import when

# Gold table 2: Daily performance aggregation
df_gold_daily = (df_silver
                 .withColumn('day_name',
                             when(col('pickup_day_of_week') == 1, 'Sunday')
                 .when(col('pickup_day_of_week') == 2, 'Monday')
                 .when(col('pickup_day_of_week') == 3, 'Tuesday')
                 .when(col('pickup_day_of_week') == 4, 'Wednesday')
                 .when(col('pickup_day_of_week') == 5, 'Thursday')
                 .when(col('pickup_day_of_week') == 6, 'Friday')
                 .when(col('pickup_day_of_week') == 7, 'Saturday')  
                 )
                 .groupBy('pickup_day_of_week', 'day_name')
                 .agg(
                     count('*').alias('total_trips'),
                     spark_round(avg('trip_distance'), 2).alias('avg_distance_miles'),
                     spark_round(avg('fare_amount'), 2).alias('avg_fare_usd'),
                     spark_round(avg('tip_amount'), 2).alias('avg_tip_usd'),
                     spark_round(spark_sum('total_amount'), 2).alias('total_revenue_usd')
                 )
                 .orderBy('pickup_day_of_week')
)

df_gold_daily.show()

# COMMAND ----------

# Gold table 3: Monthly Comparison - January vs February
df_gold_monthly = (df_silver
                   # Keep only January and February records -  clean corrupt data
                   .filter(col("pickup_month").isin([1,2]))
                   .groupBy('pickup_month')
                   .agg(
                       count('*').alias('total_trips'),
                        spark_round(avg("trip_distance"), 2).alias("avg_distance_miles"),
                        spark_round(avg("fare_amount"), 2).alias("avg_fare_usd"),
                        spark_round(avg("tip_amount"), 2).alias("avg_tip_usd"),
                        spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
                        spark_round(spark_sum("total_amount"), 2).alias("total_revenue_usd")
                   )
                   .withColumn('month_name',
                               when(col('pickup_month') == 1, 'January')
                               .when(col("pickup_month") == 2, 'February')
                               )
                   .orderBy('pickup_month')
)

df_gold_monthly.show()

# COMMAND ----------

# Write gold table 1: Hourly aggregation
(df_gold_hourly
 .write
 .format('delta')
 .mode('overwrite')
 .saveAsTable(f'{GOLD_PATH}.gold_hourly_performance')
 )

print('gold table created: main.default.gold_hourly_performance')

# Write gold table 2: Daily aggregation
(df_gold_daily
 .write
 .format('delta')
 .mode('overwrite')
 .saveAsTable(f'{GOLD_PATH}.gold_daily_performance')
 )

print('gold table created: main.default.gold_daily_performance')

# Write gold table 3: Monthly comparison
(df_gold_monthly
 .write
 .format('delta')
 .mode('overwrite')
 .saveAsTable(f'{GOLD_PATH}.gold_monthly_comparison')
 )

print('gold table created: main.default.gold_monthly_comparison')

print("\nAll Gold tables written successfully.")
