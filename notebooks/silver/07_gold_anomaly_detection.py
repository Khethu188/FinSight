
# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Gold Layer: Anomaly Detection
# MAGIC
# MAGIC **Purpose:** Detect suspicious transactions and unusual spending patterns.
# MAGIC Answers: *"Are there suspicious transactions or unusual spending patterns?"*
# MAGIC
# MAGIC **Methods:**
# MAGIC 1. **Statistical outliers** — Z-score and IQR on transaction amounts
# MAGIC 2. **Velocity checks** — Unusual transaction frequency per customer
# MAGIC 3. **Pattern anomalies** — Off-hours high-value transactions, unusual regions
# MAGIC 4. **Behavioral deviation** — Customer spending vs. their historical average

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_df = spark.read.format("delta").load(SILVER_TRANSACTIONS)
customer_df = spark.read.format("delta").load(SILVER_CUSTOMERS)

ZSCORE_THRESHOLD = QUALITY_THRESHOLDS["anomaly_zscore_threshold"]
IQR_MULTIPLIER = QUALITY_THRESHOLDS["anomaly_iqr_multiplier"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 1: Statistical Outliers (Z-Score per Product Category)

# COMMAND ----------

# Calculate stats per product category
product_stats = (
    silver_df
    .filter(F.col("status") == "completed")
    .groupBy("product_category")
    .agg(
        F.mean("amount_in_zar").alias("_mean_amount"),
        F.stddev("amount_in_zar").alias("_stddev_amount"),
        F.expr("percentile_approx(amount_in_zar, 0.25)").alias("_q1"),
        F.expr("percentile_approx(amount_in_zar, 0.75)").alias("_q3"),
    )
    .withColumn("_iqr", F.col("_q3") - F.col("_q1"))
    .withColumn("_lower_fence", F.col("_q1") - IQR_MULTIPLIER * F.col("_iqr"))
    .withColumn("_upper_fence", F.col("_q3") + IQR_MULTIPLIER * F.col("_iqr"))
)

# Join stats back and calculate Z-scores
with_stats = silver_df.join(product_stats, on="product_category", how="left")

anomaly_scored = (
    with_stats
    .withColumn("zscore",
        F.when(F.col("_stddev_amount") > 0,
            F.abs((F.col("amount_in_zar") - F.col("_mean_amount")) / F.col("_stddev_amount"))
        ).otherwise(0)
    )
    .withColumn("is_zscore_outlier", F.col("zscore") > ZSCORE_THRESHOLD)
    .withColumn("is_iqr_outlier",
        (F.col("amount_in_zar") < F.col("_lower_fence")) |
        (F.col("amount_in_zar") > F.col("_upper_fence"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2: Velocity Checks (Transaction Frequency)

# COMMAND ----------

# Count transactions per customer per day
daily_velocity = (
    silver_df
    .groupBy("customer_id", "transaction_date_key")
    .agg(F.count("transaction_id").alias("daily_tx_count"))
)

velocity_stats = daily_velocity.agg(
    F.mean("daily_tx_count").alias("_mean_velocity"),
    F.stddev("daily_tx_count").alias("_stddev_velocity"),
).collect()[0]

mean_vel = velocity_stats["_mean_velocity"] or 0
std_vel = velocity_stats["_stddev_velocity"] or 1

velocity_anomalies = (
    daily_velocity
    .withColumn("velocity_zscore",
        F.abs((F.col("daily_tx_count") - F.lit(mean_vel)) / F.lit(std_vel))
    )
    .withColumn("is_velocity_anomaly", F.col("velocity_zscore") > ZSCORE_THRESHOLD)
    .filter(F.col("is_velocity_anomaly") == True)
    .select("customer_id", "transaction_date_key", "daily_tx_count", "velocity_zscore")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 3: Pattern Anomalies

# COMMAND ----------

pattern_flags = (
    anomaly_scored
    # Flag 1: High-value transactions outside business hours
    .withColumn("is_offhours_high_value",
        (~F.col("is_business_hours")) &
        (F.col("amount_in_zar") > 100_000)
    )
    # Flag 2: Enterprise-level transactions on weekends
    .withColumn("is_weekend_enterprise",
        (F.col("is_weekend") == True) &
        (F.col("amount_bucket") == "enterprise (R1M+)")
    )
    #
