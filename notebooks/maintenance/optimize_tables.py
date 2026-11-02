
# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table Maintenance — OPTIMIZE, VACUUM, Z-ORDER
# MAGIC
# MAGIC **Purpose:** Weekly maintenance to keep Delta tables performant.
# MAGIC - **OPTIMIZE** — Compacts small files into larger ones
# MAGIC - **Z-ORDER** — Co-locates related data for faster queries
# MAGIC - **VACUUM** — Removes old file versions to save storage
# MAGIC
# MAGIC **Schedule:** Every Saturday at 3 AM SAST

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

from datetime import datetime

# ─── Table Definitions with Optimization Strategy ────────────────────────────

TABLES_TO_OPTIMIZE = [
    {
        "name": "Bronze Transactions",
        "path": BRONZE_TRANSACTIONS,
        "z_order_cols": ["transaction_id", "customer_id"],
        "vacuum_hours": 168,  # 7 days
    },
    {
        "name": "Silver Transactions",
        "path": SILVER_TRANSACTIONS,
        "z_order_cols": ["customer_id", "transaction_date_key", "product_category"],
        "vacuum_hours": 168,
    },
    {
        "name": "Silver Customers",
        "path": SILVER_CUSTOMERS,
        "z_order_cols": ["customer_id", "risk_tier"],
        "vacuum_hours": 168,
    },
    {
        "name": "Gold Revenue Summary",
        "path": GOLD_REVENUE,
        "z_order_cols": ["region_code", "product_category"],
        "vacuum_hours": 168,
    },
    {
        "name": "Gold Regional P&L",
        "path": GOLD_REGIONAL_PNL,
        "z_order_cols": ["region_code", "product_category"],
        "vacuum_hours": 168,
    },
    {
        "name": "Gold Product Performance",
        "path": GOLD_PRODUCT_PERF,
        "z_order_cols": ["product_category", "product_name"],
        "vacuum_hours": 168,
    },
    {
        "name": "Gold Anomalies",
        "path": GOLD_ANOMALIES,
        "z_order_cols": ["customer_id", "anomaly_type"],
        "vacuum_hours": 168,
    },
    {
        "name": "Quarantine",
        "path": QUARANTINE_PATH,
        "z_order_cols": ["_quarantine_reason"],
        "vacuum_hours": 72,  # 3 days (shorter — less critical)
    },
    {
        "name": "Quality Metrics",
        "path": f"{S3_BUCKET_GOLD}/quality_metrics",
        "z_order_cols": ["layer", "rule_id"],
        "vacuum_hours": 168,
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Maintenance

# COMMAND ----------

results = []

for table in TABLES_TO_OPTIMIZE:
    print(f"\n{'─' * 60}")
    print(f"  Maintaining: {table['name']}")
    print(f"  Path: {table['path']}")
    print(f"{'─' * 60}")

    try:
        start = datetime.now()

        # Step 1: OPTIMIZE with Z-ORDER
        z_cols = ", ".join(table["z_order_cols"])
        print(f"  → OPTIMIZE with Z-ORDER BY ({z_cols})...")
        spark.sql(f"OPTIMIZE delta.`{table['path']}` ZORDER BY ({z_cols})")

        # Step 2: VACUUM
        vacuum_hours = table["vacuum_hours"]
        print(f"  → VACUUM (retain {vacuum_hours}h)...")
        spark.sql(f"SET spark.databricks.delta.retentionDurationCheck.enabled = false")
        spark.sql(f"VACUUM delta.`{table['path']}` RETAIN {vacuum_hours} HOURS")
        spark.sql(f"SET spark.databricks.delta.retentionDurationCheck.enabled = true")

        # Step 3: Get table stats
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{table['path']}`").collect()[0]
        duration = (datetime.now() - start).total_seconds()

        results.append({
            "table": table["name"],
            "status": "✓ Success",
            "duration_s": round(duration, 1),
            "num_files": detail["numFiles"],
            "size_mb": round(detail["sizeInBytes"] / 1024 / 1024, 1),
        })

        print(f"  ✓ Done in {duration:.1f}s | {detail['numFiles']} files | {detail['sizeInBytes'] / 1024 / 1024:.1f} MB")

    except Exception as e:
        results.append({
            "table": table["name"],
            "status": f"✗ Failed: {str(e)[:80]}",
            "duration_s": 0,
            "num_files": 0,
            "size_mb": 0,
        })
        print(f"  ✗ Failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maintenance Summary

# COMMAND ----------

results_df = spark.createDataFrame(results)

print("\n" + "=" * 70)
print("  📊 MAINTENANCE SUMMARY")
print("=" * 70)
results_df.show(truncate=False)

total_size = sum(r["size_mb"] for r in results)
total_time = sum(r["duration_s"] for r in results)
success_count = sum(1 for r in results if "Success" in r["status"])

print(f"  Tables maintained : {success_count}/{len(results)}")
print(f"  Total size        : {total_size:.1f} MB")
print(f"  Total duration    : {total_time:.1f}s")
print("=" * 70)

