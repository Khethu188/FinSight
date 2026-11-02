
# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Quality Metrics Store
# MAGIC
# MAGIC **Purpose:** Aggregate historical quality metrics for trend analysis.
# MAGIC Powers the Data Quality Dashboard showing quality over time.
# MAGIC
# MAGIC **Output:** `gold/quality_trends` — Daily quality scores by layer and check type

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

QUALITY_METRICS_PATH = f"{S3_BUCKET_GOLD}/quality_metrics"
QUALITY_TRENDS_PATH = f"{S3_BUCKET_GOLD}/quality_trends"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Historical Quality Metrics

# COMMAND ----------

metrics_df = spark.read.format("delta").load(QUALITY_METRICS_PATH)

print(f"Total quality check records: {metrics_df.count():,}")
print(f"Date range: {metrics_df.agg(F.min('check_timestamp')).collect()[0][0]} → "
      f"{metrics_df.agg(F.max('check_timestamp')).collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Quality Trends

# COMMAND ----------

daily_trends = (
    metrics_df
    .withColumn("check_date", F.to_date(F.col("check_timestamp")))
    .groupBy("check_date", "layer", "severity")
    .agg(
        F.count("rule_id").alias("total_checks"),
        F.sum(F.when(F.col("passed") == True, 1).otherwise(0)).alias("passed_checks"),
        F.sum(F.when(F.col("passed") == False, 1).otherwise(0)).alias("failed_checks"),
        F.avg("actual_value").alias("avg_quality_score"),
        F.min("actual_value").alias("min_quality_score"),
    )
    .withColumn("pass_rate",
        F.round(F.col("passed_checks") / F.col("total_checks") * 100, 2)
    )
    # 7-day rolling average
    .withColumn("rolling_7d_pass_rate",
        F.round(F.avg("pass_rate").over(
            Window.partitionBy("layer")
            .orderBy("check_date")
            .rowsBetween(-6, 0)
        ), 2)
    )
    .withColumn("_generated_at", F.current_timestamp())
)

# COMMAND ----------

(
    daily_trends.write
    .format("delta")
    .mode("overwrite")
    .save(QUALITY_TRENDS_PATH)
)

print(f"✓ Quality trends written ({daily_trends.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer-Level Health Summary

# COMMAND ----------

layer_health = (
    metrics_df
    .withColumn("check_date", F.to_date(F.col("check_timestamp")))
    .filter(F.col("check_date") == F.current_date())
    .groupBy("layer")
    .agg(
        F.count("rule_id").alias("total_checks"),
        F.sum(F.when(F.col("passed"), 1).otherwise(0)).alias("passed"),
        F.sum(F.when(~F.col("passed") & (F.col("severity") == "critical"), 1).otherwise(0)).alias("critical_fails"),
        F.avg("actual_value").alias("avg_score"),
    )
    .withColumn("health_status",
        F.when(F.col("critical_fails") == 0, "🟢 HEALTHY")
         .when(F.col("critical_fails") <= 2, "🟡 DEGRADED")
         .otherwise("🔴 CRITICAL")
    )
)

print("Today's Layer Health:")
layer_health.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Worst-Performing Rules (Last 7 Days)

# COMMAND ----------

worst_rules = (
    metrics_df
    .withColumn("check_date", F.to_date(F.col("check_timestamp")))
    .filter(F.col("check_date") >= F.date_sub(F.current_date(), 7))
    .groupBy("rule_id", "rule_name", "layer", "severity")
    .agg(
        F.count("*").alias("times_checked"),
        F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("times_failed"),
        F.avg("actual_value").alias("avg_score"),
    )
    .withColumn("failure_rate",
        F.round(F.col("times_failed") / F.col("times_checked") * 100, 1)
    )
    .filter(F.col("times_failed") > 0)
    .orderBy(F.desc("failure_rate"))
)

print("Worst-Performing Rules (Last 7 Days):")
worst_rules.show(20, truncate=False)

