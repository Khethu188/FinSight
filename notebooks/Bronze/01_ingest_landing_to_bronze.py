
# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer: Landing → Bronze Ingestion
# MAGIC
# MAGIC **Purpose:** Ingest raw CSV files from the S3 landing zone into Bronze Delta Lake tables
# MAGIC using Auto Loader (Structured Streaming) for incremental, exactly-once processing.
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Auto Loader with schema enforcement and evolution
# MAGIC - Source file tracking for lineage
# MAGIC - Audit metadata columns
# MAGIC - Idempotent — safe to re-run
# MAGIC
# MAGIC **Input:** `s3://...-landing/landing/transactions/year=*/month=*/day=*/*.csv`
# MAGIC **Output:** `s3://...-bronze/transactions/` (Delta Lake, partitioned by date)

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# ─── Configuration ───────────────────────────────────────────────────────────

dbutils.widgets.text("landing_path", LANDING_TRANSACTIONS_PATH, "Landing Path")
dbutils.widgets.text("bronze_path", BRONZE_TRANSACTIONS, "Bronze Path")
dbutils.widgets.text("checkpoint_path", LANDING_CHECKPOINT_PATH, "Checkpoint Path")
dbutils.widgets.dropdown("processing_mode", "batch", ["batch", "streaming"], "Processing Mode")

landing_path = dbutils.widgets.get("landing_path")
bronze_path = dbutils.widgets.get("bronze_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
processing_mode = dbutils.widgets.get("processing_mode")

print(f"Landing Path   : {landing_path}")
print(f"Bronze Path    : {bronze_path}")
print(f"Checkpoint Path: {checkpoint_path}")
print(f"Mode           : {processing_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Define Bronze Schema
# MAGIC We enforce schema on read to catch structural issues immediately.

# COMMAND ----------

bronze_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("exchange_rate_to_zar", DoubleType(), True),
    StructField("amount_in_zar", DoubleType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("status", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("reference_number", StringType(), True),
    StructField("description", StringType(), True),
    StructField("is_weekend", BooleanType(), True),
    StructField("is_month_end", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("_defect_injected", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read from Landing Zone with Auto Loader

# COMMAND ----------

def read_landing_autoloader(spark, path, schema, checkpoint):
    """
    Read raw CSV files from landing zone using Auto Loader.
    Auto Loader tracks which files have been processed via the checkpoint.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
        .option("cloudFiles.inferColumnTypes", "false")  # Use enforced schema
        .option("header", "true")
        .option("multiLine", "false")
        .option("rescue", "_rescued_data")  # Capture malformed rows
        .schema(schema)
        .load(path)
    )


def read_landing_batch(spark, path, schema):
    """Batch read for backfill or testing."""
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("multiLine", "false")
        .schema(schema)
        .load(path)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply Bronze Transformations
# MAGIC Bronze = raw data + metadata. No business logic, no filtering.

# COMMAND ----------

def transform_to_bronze(df):
    """
    Apply Bronze-layer transformations:
    - Add ingestion metadata (timestamp, source file, pipeline run)
    - Add partition columns for efficient querying
    - Preserve ALL records including bad ones (quarantine happens in Silver)
    """
    return (
        df
        # Audit columns
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("_bronze_ingestion_date", F.current_date())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_pipeline_run_id", F.lit(
            f"bronze_{F.current_timestamp()}"
        ))
        # Partition columns (extracted from transaction_date string)
        .withColumn("_ingestion_year", F.year(F.current_date()))
        .withColumn("_ingestion_month", F.month(F.current_date()))
        .withColumn("_ingestion_day", F.dayofmonth(F.current_date()))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to Bronze Delta Lake

# COMMAND ----------

if processing_mode == "streaming":
    # ─── Streaming Mode (Production) ────────────────────────────────────
    print("Starting Auto Loader streaming ingestion...")

    raw_stream = read_landing_autoloader(spark, landing_path, bronze_schema, checkpoint_path)
    bronze_stream = transform_to_bronze(raw_stream)

    query = (
        bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_year", "_ingestion_month", "_ingestion_day")
        .trigger(availableNow=True)  # Process all available files, then stop
        .start(bronze_path)
    )

    query.awaitTermination()
    print(f"✓ Streaming ingestion complete")

else:
    # ─── Batch Mode (Backfill / Testing) ────────────────────────────────
    print("Starting batch ingestion...")

    raw_batch = read_landing_batch(spark, landing_path, bronze_schema)
    bronze_batch = transform_to_bronze(raw_batch)

    (
        bronze_batch.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_year", "_ingestion_month", "_ingestion_day")
        .save(bronze_path)
    )

    print(f"✓ Batch ingestion complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate Bronze Output

# COMMAND ----------

bronze_df = spark.read.format("delta").load(bronze_path)

record_count = bronze_df.count()
null_ids = bronze_df.filter(F.col("transaction_id").isNull()).count()
rescued = bronze_df.filter(F.col("_rescued_data").isNotNull()).count() if "_rescued_data" in bronze_df.columns else 0

print("=" * 60)
print("  Bronze Layer Validation")
print("=" * 60)
print(f"  Total records      : {record_count:,}")
print(f"  Null transaction IDs: {null_ids:,}")
print(f"  Rescued rows       : {rescued:,}")
print(f"  Partitions         : {bronze_df.select('_ingestion_year', '_ingestion_month').distinct().count()}")
print("=" * 60)

# Fail the notebook if critical issues detected
assert record_count > 0, "CRITICAL: Bronze table is empty after ingestion!"
print("✓ Bronze validation passed")

