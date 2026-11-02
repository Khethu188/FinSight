
# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Gold Layer: Product Performance
# MAGIC
# MAGIC **Purpose:** Analyze product-level performance metrics including
# MAGIC adoption, revenue contribution, failure rates, and channel preferences.

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_df = spark.read.format("delta").load(SILVER_TRANSACTIONS)

# COMMAND ----------

product_perf = (
    silver_df
    .groupBy(
        "transaction_year",
        "transaction_month",
        "product_category",
        "product_name",
    )
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("amount_in_zar").alias("total_volume_zar"),
        F.avg("amount_in_zar").alias("avg_transaction_zar"),
        F.percentile_approx("amount_in_zar", 0.5).alias("median_transaction_zar"),

        # Status breakdown
        F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_count"),
        F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_count"),
        F.sum(F.when(F.col("status") == "flagged", 1).otherwise(0)).alias("flagged_count"),
        F.sum(F.when(F.col("status") == "pending", 1).otherwise(0)).alias("pending_count"),

        # Channel distribution
        F.sum(F.when(F.col("channel") == "mobile_app", 1).otherwise(0)).alias("mobile_transactions"),
        F.sum(F.when(F.col("channel") == "online", 1).otherwise(0)).alias("online_transactions"),
        F.sum(F.when(F.col("channel") == "branch", 1).otherwise(0)).alias("branch_transactions"),
        F.sum(F.when(F.col("channel") == "atm", 1).otherwise(0)).alias("atm_transactions"),

        # Amount distribution
        F.sum(F.when(F.col("amount_bucket") == "micro (<R100)", 1).otherwise(0)).alias("micro_count"),
        F.sum(F.when(F.col("amount_bucket") == "enterprise (R1M+)", 1).otherwise(0)).alias("enterprise_count"),
    )

    # Derived rates
    .withColumn("completion_rate",
        F.round(F.col("completed_count") / F.col("total_transactions") * 100, 2)
    )
    .withColumn("failure_rate",
        F.round(F.col("failed_count") / F.col("total_transactions") * 100, 2)
    )
    .withColumn("flag_rate",
        F.round(F.col("flagged_count") / F.col("total_transactions") * 100, 2)
    )
    .withColumn("digital_adoption_rate",
        F.round(
            (F.col("mobile_transactions") + F.col("online_transactions")) /
            F.col("total_transactions") * 100, 2
        )
    )

    # Revenue contribution (% of total)
    .withColumn("_total_volume_all",
        F.sum("total_volume_zar").over(Window.partitionBy("transaction_year", "transaction_month"))
    )
    .withColumn("revenue_contribution_pct",
        F.round(F.col("total_volume_zar") / F.col("_total_volume_all") * 100, 2)
    )
    .drop("_total_volume_all")

    .withColumn("_gold_generated_at", F.current_timestamp())
)

# COMMAND ----------

(
    product_perf.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("transaction_year", "transaction_month")
    .save(GOLD_PRODUCT_PERF)
)

print(f"✓ Product performance written to Gold ({product_perf.count():,} rows)")
print("\nTop Products by Volume:")
product_perf.orderBy(F.desc("total_volume_zar")).select(
    "product_name", "total_transactions", "total_volume_zar",
    "completion_rate", "digital_adoption_rate"
).show(10, truncate=False)

