
# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Layer: Bronze → Silver Cleansing
# MAGIC
# MAGIC **Purpose:** Cleanse, validate, deduplicate, and type-cast Bronze data into
# MAGIC a reliable Silver layer. Bad records are routed to Quarantine.
# MAGIC
# MAGIC **Transformations:**
# MAGIC 1. Parse and validate transaction dates
# MAGIC 2. Validate currencies, statuses, channels against allowed values
# MAGIC 3. Validate and fix amounts (nulls, negatives, overflows)
# MAGIC 4. Deduplicate on transaction_id (keep first occurrence)
# MAGIC 5. Standardize all amounts to ZAR
# MAGIC 6. Add date dimension columns
# MAGIC 7. Add amount bucketing
# MAGIC 8. Route invalid records to quarantine
# MAGIC
# MAGIC **Input:** Bronze Delta table
# MAGIC **Output:** Silver Delta table + Quarantine Delta table

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce

# ─── Configuration ───────────────────────────────────────────────────────────

dbutils.widgets.text("bronze_path", BRONZE_TRANSACTIONS, "Bronze Path")
dbutils.widgets.text("silver_path", SILVER_TRANSACTIONS, "Silver Path")
dbutils.widgets.text("quarantine_path", QUARANTINE_PATH, "Quarantine Path")
dbutils.widgets.text("process_date", "", "Process Date (YYYY-MM-DD, blank=all)")

bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")
quarantine_path = dbutils.widgets.get("quarantine_path")
process_date = dbutils.widgets.get("process_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze Data

# COMMAND ----------

bronze_df = spark.read.format("delta").load(bronze_path)

# Optional: filter to specific date for incremental processing
if process_date:
    bronze_df = bronze_df.filter(F.col("_bronze_ingestion_date") == process_date)
    print(f"Filtered to process date: {process_date}")

total_bronze = bronze_df.count()
print(f"Bronze records to process: {total_bronze:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Data Quality Validation Rules
# MAGIC Each rule produces a boolean column. Records failing ANY critical rule go to quarantine.

# COMMAND ----------

def apply_validation_rules(df):
    """
    Apply all validation rules. Each rule adds a boolean flag column.
    Records failing critical rules are routed to quarantine.
    """

    validated = df

    # ─── Rule 1: Transaction ID must exist and not be null ───────────
    validated = validated.withColumn(
        "_valid_transaction_id",
        F.col("transaction_id").isNotNull() & (F.length(F.col("transaction_id")) > 0)
    )

    # ─── Rule 2: Transaction date must be parseable and not in the future ─
    validated = validated.withColumn(
        "_parsed_date",
        F.to_timestamp(F.col("transaction_date"), "yyyy-MM-dd HH:mm:ss")
    )
    validated = validated.withColumn(
        "_valid_date",
        F.col("_parsed_date").isNotNull() &
        (F.col("_parsed_date") <= F.date_add(F.current_timestamp(), QUALITY_THRESHOLDS["future_date_tolerance_hours"]))
    )

    # ─── Rule 3: Amount must be positive and within realistic range ──
    validated = validated.withColumn(
        "_valid_amount",
        F.col("transaction_amount").isNotNull() &
        (F.col("transaction_amount") > QUALITY_THRESHOLDS["amount_min"]) &
        (F.col("transaction_amount") < QUALITY_THRESHOLDS["amount_max"])
    )

    # ─── Rule 4: Currency must be in valid set ──────────────────────
    validated = validated.withColumn(
        "_valid_currency",
        F.col("currency").isin(VALID_CURRENCIES)
    )

    # ─── Rule 5: Customer ID must exist ─────────────────────────────
    validated = validated.withColumn(
        "_valid_customer",
        F.col("customer_id").isNotNull() & (F.length(F.col("customer_id")) > 0)
    )

    # ─── Rule 6: Status must be in valid set ────────────────────────
    validated = validated.withColumn(
        "_valid_status",
        F.col("status").isin(VALID_STATUSES)
    )

    # ─── Rule 7: Channel must be in valid set ───────────────────────
    validated = validated.withColumn(
        "_valid_channel",
        F.col("channel").isin(VALID_CHANNELS)
    )

    # ─── Rule 8: Region must be in valid set ────────────────────────
    validated = validated.withColumn(
        "_valid_region",
        F.col("region_code").isin(VALID_REGIONS)
    )

    # ─── Composite: Record passes ALL critical rules ────────────────
    critical_rules = [
        "_valid_transaction_id",
        "_valid_date",
        "_valid_amount",
        "_valid_currency",
        "_valid_customer",
    ]

    validated = validated.withColumn(
        "_passes_all_critical",
        reduce(lambda a, b: a & b, [F.col(rule) for rule in critical_rules])
    )

    # ─── Failure reason (for quarantine debugging) ──────────────────
    validated = validated.withColumn(
        "_failure_reasons",
        F.concat_ws(", ",
            F.when(~F.col("_valid_transaction_id"), F.lit("invalid_transaction_id")),
            F.when(~F.col("_valid_date"), F.lit("invalid_date")),
            F.when(~F.col("_valid_amount"), F.lit("invalid_amount")),
            F.when(~F.col("_valid_currency"), F.lit("invalid_currency")),
            F.when(~F.col("_valid_customer"), F.lit("missing_customer")),
            F.when(~F.col("_valid_status"), F.lit("invalid_status")),
            F.when(~F.col("_valid_channel"), F.lit("invalid_channel")),
            F.when(~F.col("_valid_region"), F.lit("invalid_region")),
        )
    )

    return validated

# COMMAND ----------

validated_df = apply_validation_rules(bronze_df)

# Split into good and bad records
good_records = validated_df.filter(F.col("_passes_all_critical") == True)
bad_records = validated_df.filter(F.col("_passes_all_critical") == False)

good_count = good_records.count()
bad_count = bad_records.count()
pass_rate = round(good_count / total_bronze * 100, 2) if total_bronze > 0 else 0

print(f"Validation Results:")
print(f"  ✓ Passed : {good_count:,} ({pass_rate}%)")
print(f"  ✗ Failed : {bad_count:,} ({100 - pass_rate}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Route Bad Records to Quarantine

# COMMAND ----------

if bad_count > 0:
    quarantine_df = (
        bad_records
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_quarantine_reason", F.col("_failure_reasons"))
    )

    (
        quarantine_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_year", "_ingestion_month")
        .save(quarantine_path)
    )

    print(f"✓ {bad_count:,} records routed to quarantine")

    # Show failure distribution
    print("\nFailure Reason Distribution:")
    bad_records.groupBy("_failure_reasons").count().orderBy(F.desc("count")).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Deduplicate Good Records

# COMMAND ----------

# Flag duplicates — keep earliest by transaction_date
dedup_window = Window.partitionBy("transaction_id").orderBy(F.col("_parsed_date").asc())

deduped_df = (
    good_records
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

dupes_removed = good_count - deduped_df.count()
print(f"Duplicates removed: {dupes_removed:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Apply Silver Transformations

# COMMAND ----------

def transform_to_silver(df):
    """
    Apply Silver-layer transformations:
    - Cast transaction_date to proper timestamp
    - Standardize amounts to ZAR
    - Add date dimension columns
    - Add amount bucketing
    - Add business hours flag
    - Clean up validation columns
    """

    # Build exchange rate mapping
    exchange_mapping = F.create_map([
        F.lit(x) for pair in EXCHANGE_RATES_TO_ZAR.items() for x in pair
    ])

    silver = (
        df
        # Use the already-parsed date
        .withColumn("transaction_date", F.col("_parsed_date"))

        # Recalculate ZAR amount using validated exchange rates
        .withColumn("exchange_rate_to_zar",
            F.coalesce(exchange_mapping[F.col("currency")], F.lit(1.0))
        )
        .withColumn("amount_in_zar",
            F.round(F.col("transaction_amount") * F.col("exchange_rate_to_zar"), 2)
        )

        # Date dimensions
        .withColumn("transaction_date_key", F.to_date(F.col("transaction_date")))
        .withColumn("transaction_year", F.year(F.col("transaction_date")))
        .withColumn("transaction_month", F.month(F.col("transaction_date")))
        .withColumn("transaction_day", F.dayofmonth(F.col("transaction_date")))
        .withColumn("transaction_hour", F.hour(F.col("transaction_date")))
        .withColumn("day_of_week", F.date_format(F.col("transaction_date"), "EEEE"))

        # Business hours flag (8am-5pm, Mon-Fri)
        .withColumn("is_business_hours",
            F.when(
                (F.hour(F.col("transaction_date")).between(8, 16)) &
                (F.dayofweek(F.col("transaction_date")).between(2, 6)),
                True
            ).otherwise(False)
        )

        # Amount bucketing
        .withColumn("amount_bucket",
            F.when(F.col("amount_in_zar") < 100, "micro (<R100)")
             .when(F.col("amount_in_zar") < 1_000, "small (R100-R1K)")
             .when(F.col("amount_in_zar") < 10_000, "medium (R1K-R10K)")
             .when(F.col("amount_in_zar") < 100_000, "large (R10K-R100K)")
             .when(F.col("amount_in_zar") < 1_000_000, "major (R100K-R1M)")
             .otherwise("enterprise (R1M+)")
        )

        # Ensure boolean types
        .withColumn("is_weekend", F.col("is_weekend").cast("boolean"))
        .withColumn("is_month_end", F.col("is_month_end").cast("boolean"))

        # Audit
        .withColumn("_silver_loaded_at", F.current_timestamp())

        # Drop validation columns (not needed downstream)
        .drop(
            "_parsed_date", "_valid_transaction_id", "_valid_date",
            "_valid_amount", "_valid_currency", "_valid_customer",
            "_valid_status", "_valid_channel", "_valid_region",
            "_passes_all_critical", "_failure_reasons",
            "_defect_injected", "created_at",
            "_bronze_ingested_at", "_bronze_ingestion_date",
            "_ingestion_year", "_ingestion_month", "_ingestion_day",
            "_pipeline_run_id",
        )
    )

    return silver

# COMMAND ----------

silver_df = transform_to_silver(deduped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Write to Silver Delta Lake

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("transaction_year", "transaction_month")
    .save(silver_path)
)

silver_count = silver_df.count()
print(f"✓ {silver_count:,} records written to Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Silver Validation Summary

# COMMAND ----------

silver_final = spark.read.format("delta").load(silver_path)

print("=" * 65)
print("  Silver Layer Summary")
print("=" * 65)
print(f"  Total Silver records  : {silver_final.count():,}")
print(f"  Date range            : {silver_final.agg(F.min('transaction_date')).collect()[0][0]} → "
      f"{silver_final.agg(F.max('transaction_date')).collect()[0][0]}")
print(f"  Unique customers      : {silver_final.select('customer_id').distinct().count():,}")
print(f"  Unique transactions   : {silver_final.select('transaction_id').distinct().count():,}")
print(f"  Total amount (ZAR)    : R{silver_final.agg(F.sum('amount_in_zar')).collect()[0][0]:,.2f}")
print("=" * 65)

# Pipeline metrics
print(f"\n  Pipeline Metrics:")
print(f"  Bronze input          : {total_bronze:,}")
print(f"  Quarantined           : {bad_count:,} ({100 - pass_rate:.1f}%)")
print(f"  Duplicates removed    : {dupes_removed:,}")
print(f"  Silver output         : {silver_count:,}")
print(f"  Recovery rate         : {round(silver_count / total_bronze * 100, 1)}%")

