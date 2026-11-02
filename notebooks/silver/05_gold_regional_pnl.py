
# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Gold Layer: Regional Profit & Loss
# MAGIC
# MAGIC **Purpose:** Calculate profit/loss by region and product.
# MAGIC Answers: *"Which products or regions are losing money?"*
# MAGIC
# MAGIC **Logic:**
# MAGIC - Revenue = completed debits + fees
# MAGIC - Costs = credits + interest payouts + reversals
# MAGIC - Net P&L = Revenue - Costs

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = spark.read.format("delta").load(SILVER_TRANSACTIONS)

# COMMAND ----------

regional_pnl = (
    silver_df
    .filter(F.col("status").isin("completed", "reversed"))
    .groupBy(
        "transaction_year",
        "transaction_month",
        "region_code",
        "region_name",
        "product_category",
        "product_name",
    )
    .agg(
        # Revenue side (money coming in)
        F.sum(F.when(
            F.col("transaction_type").isin("debit", "fee"),
            F.col("amount_in_zar")
        ).otherwise(0)).alias("gross_revenue_zar"),

        # Cost side (money going out)
        F.sum(F.when(
            F.col("transaction_type").isin("credit", "interest", "reversal"),
            F.col("amount_in_zar")
        ).otherwise(0)).alias("total_costs_zar"),

        F.count("transaction_id").alias("transaction_count"),
        F.countDistinct("customer_id").alias("unique_customers"),

        # Breakdown
        F.sum(F.when(F.col("transaction_type") == "fee", F.col("amount_in_zar")).otherwise(0)).alias("fee_income_zar"),
        F.sum(F.when(F.col("transaction_type") == "interest", F.col("amount_in_zar")).otherwise(0)).alias("interest_expense_zar"),
        F.sum(F.when(F.col("transaction_type") == "reversal", F.col("amount_in_zar")).otherwise(0)).alias("reversal_losses_zar"),
    )

    # Calculate P&L
    .withColumn("net_pnl_zar", F.col("gross_revenue_zar") - F.col("total_costs_zar"))
    .withColumn("profit_margin",
        F.when(F.col("gross_revenue_zar") > 0,
            F.round(F.col("net_pnl_zar") / F.col("gross_revenue_zar") * 100, 2)
        ).otherwise(0)
    )
    .withColumn("is_profitable", F.col("net_pnl_zar") > 0)

    .withColumn("_gold_generated_at", F.current_timestamp())
)

# COMMAND ----------

(
    regional_pnl.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("transaction_year", "transaction_month")
    .save(GOLD_REGIONAL_PNL)
)

# Show loss-making segments
print("⚠ Loss-Making Segments:")
regional_pnl.filter(F.col("is_profitable") == False).orderBy("net_pnl_zar").show(20, truncate=False)

print(f"✓ Regional P&L written to Gold ({regional_pnl.count():,} rows)")

