# Databricks notebook source
# ============================================================
# NOTEBOOK: 04_delta_time_travel
# PURPOSE: Demonstrate Delta Lake Time Travel capabilities
# ============================================================

# Time Travel Table
TABLE_NAME = 'main.default.bronze_yellow_taxi'

print("Delta Lake Time Travel Demo")
print(f"Table: {TABLE_NAME}")

# COMMAND ----------

# Show full version history of the bronze table
history_df = spark.sql(f"DESCRIBE HISTORY {TABLE_NAME}")
history_df.select("version", "timestamp", "operation", "operationParameters").show(10, truncate=False)

# COMMAND ----------

# Read the table at version 0 (original ingestion)
df_version_0 = spark.read.format('delta').option('versionAsOf', 2).table(TABLE_NAME)

# Read the current version
df_current = spark.read.table(TABLE_NAME)

print(f"Version 0 row count : {df_version_0.count():,}")
print(f"Current version rows: {df_current.count():,}")

# COMMAND ----------

# Read the table as it was at a specific timestamp
df_timestamp = (spark.read
    .format("delta")
    .option("timestampAsOf", "2026-04-27")
    .table(TABLE_NAME)
)

print(f"Rows as of 2026-04-27: {df_timestamp.count():,}")

# COMMAND ----------

spark.sql(f"""
           ALTER TABLE {TABLE_NAME}
           SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days')
           """)

# Run VACUUM in dry-run mode (shows what would be deleted without actually deleting)
print("Files that would be removed by VACUUM:")
spark.sql(f'VACUUM {TABLE_NAME} DRY RUN').show(truncate=False)

# COMMAND ----------

print("Delta Lake Time Travel - Summary")
print("=" * 45)
print(f"Table          : {TABLE_NAME}")
print(f"Total versions : 5 (0 through 4)")
print("")
print("Capabilities:")
print("  - DESCRIBE HISTORY : full audit log of all operations")
print("  - versionAsOf      : query table at a specific version")
print("  - timestampAsOf    : query table at a specific timestamp")
print("  - VACUUM DRY RUN   : preview files eligible for cleanup")
print("  - VACUUM           : remove files outside retention window")
print("")
print("Retention policy: 7 days (168 hours)")
print("Use case: data recovery, auditing, reproducible pipelines")