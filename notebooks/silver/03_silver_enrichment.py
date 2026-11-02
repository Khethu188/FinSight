
# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver Enrichment: Customer Dimension & Derived Metrics
# MAGIC
# MAGIC **Purpose:** Build a customer dimension table from Silver transactions
# MAGIC and add derived metrics for downstream Gold aggregations.
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `silver/customers` — Customer dimension with lifetime metrics
# MAGIC - Updated Silver transactions with customer risk tier

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Customer Dimension

# COMMAND ----------

silver_df = spark.read.format("delta").load(SILVER_TRANSACTIONS)

customer_dim = (
    silver_df
    .groupBy("customer_id")
    .agg(
        F.first("customer_name").alias("customer_name"),
        F.first("account_id").alias("primary_account_id"),
        F.countDistinct("account_id").alias("num_accounts"),
        F.count("transaction_id").alias("total_transactions"),
        F.sum("amount_in_zar").alias("total_amount_zar"),
        F.avg("amount_in_zar").alias("avg_transaction_zar"),
        F.max("amount_in_zar").alias("max_transaction_zar"),
        F.min("transaction_date").alias("first_transaction_date"),
        F.max("transaction_date").alias("last_transaction_date"),
        F.countDistinct("product_category").alias("num_product_categories"),
        F.countDistinct("channel").alias("num_channels_used"),

        # Status distribution
        F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_count"),
        F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_count"),
        F.sum(F.when(F.col("status") == "flagged", 1).otherwise(0)).alias("flagged_count"),
        F.sum(F.when(F.col("status") == "reversed", 1).otherwise(0)).alias("reversed_count"),

        # Region
        F.first("region_code").alias("primary_region"),
        F.first("region_name").alias("primary_region_name"),
    )

    # Derived metrics
    .withColumn("failure_rate",
        F.round(F.col("failed_count") / F.col("total_transactions"), 4)
    )
    .withColumn("flag_rate",
        F.round(F.col("flagged_count") / F.col("total_transactions"), 4)
    )
    .withColumn("customer_tenure_days",
        F.datediff(F.col("last_transaction_date"), F.col("first_transaction_date"))
    )

    # Risk tier based on flag rate and transaction patterns
    .withColumn("risk_tier",
        F.when(F.col("flag_rate") > 0.10, "high")
         .when(F.col("flag_rate") > 0.05, "medium")
         .otherwise("low")
    )

    # Value tier based on total amount
    .withColumn("value_tier",
        F.when(F.col("total_amount_zar") > 5_000_000, "platinum")
         .when(F.col("total_amount_zar") > 1_000_000, "gold")
         .when(F.col("total_amount_zar") > 100_000, "silver")
         .otherwise("bronze")
    )

    .withColumn("_customer_dim_updated_at", F.current_timestamp())
)

# COMMAND ----------

# Write customer dimension (overwrite — full rebuild each run)
(
    customer_dim.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_CUSTOMERS)
)

print(f"✓ Customer dimension: {customer_dim.count():,} customers")
print(f"  Risk distribution:")
customer_dim.groupBy("risk_tier").count().orderBy("risk_tier").show()
print(f"  Value distribution:")
customer_dim.groupBy("value_tier").count().orderBy("value_tier").show()

