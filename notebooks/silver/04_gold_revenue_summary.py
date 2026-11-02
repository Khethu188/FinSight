
# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold Layer: Monthly Revenue Summary
# MAGIC
# MAGIC **Purpose:** Aggregate Silver transactions into monthly revenue metrics
# MAGIC for executive dashboards. Answers: *"How much revenue are we generating each month?"*
# MAGIC
# MAGIC **Output:** `gold/revenue_summary` — Monthly revenue by product, region, channel

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = spark.read.format("delta").load(SILVER_TRANSACTIONS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Revenue Summary

# COMMAND ----------

revenue_summary = (
    silver_df
    .filter(F.col("status") == "completed")  # Only completed transactions count as revenue
    .groupBy(
        "transaction_year",
        "transaction_month",
        "product_category",
        "product_name",
        "region_code",
        "region_name",
        "channel",
        "currency",
    )
    .agg(
        F.count("transaction_id").alias("transaction_count"),
        F.sum("amount_in_zar").alias("total_revenue_zar"),
        F.avg("amount_in_zar").alias("avg_transaction_zar"),
        F.min("amount_in_zar").alias("min_transaction_zar"),
        F.max("amount_in_zar").alias("max_transaction_zar"),
        F.stddev("amount_in_zar").alias("stddev_transaction_zar"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("account_id").alias("unique_accounts"),

        # Volume by type
        F.sum(F.when(F.col("transaction_type") == "debit", 1).otherwise(0)).alias("debit_count"),
        F.sum(F.when(F.col("transaction_type") == "credit", 1).otherwise(0)).alias("credit_count"),
        F.sum(F.when(F.col("transaction_type") == "fee", 1).otherwise(0)).alias("fee_count"),

        # Revenue by type
        F.sum(F.when(F.col("transaction_type") == "debit", F.col("amount_in_zar")).otherwise(0)).alias("debit_revenue_zar"),
        F.sum(F.when(F.col("transaction_type") == "credit", F.col("amount_in_zar")).otherwise(0)).alias("credit_revenue_zar"),
        F.sum(F.when(F.col("transaction_type") == "fee", F.col("amount_in_zar")).otherwise(0)).alias("fee_revenue_zar"),

        # Time patterns
        F.sum(F.when(F.col("is_weekend") == True, 1).otherwise(0)).alias("weekend_transactions"),
        F.sum(F.when(F.col("is_business_hours") == True, 1).otherwise(0)).alias("business_hours_transactions"),
    )

    # Month-over-month growth (calculated via window)
    .withColumn("_gold_generated_at", F.current_timestamp())
)

# COMMAND ----------

# Write to Gold
(
    revenue_summary.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("transaction_year", "transaction_month")
    .save(GOLD_REVENUE)
)

total_revenue = revenue_summary.agg(F.sum("total_revenue_zar")).collect()[0][0]
print(f"✓ Revenue summary written to Gold")
print(f"  Total revenue (ZAR): R{total_revenue:,.2f}")
print(f"  Rows: {revenue_summary.count():,}")

