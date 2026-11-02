
# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Quarantine Reprocessor (Self-Healing)
# MAGIC
# MAGIC **Purpose:** Attempt to recover quarantined records by applying
# MAGIC fix-up rules. Records that can be repaired are promoted to Silver.
# MAGIC
# MAGIC **Fix-Up Strategies:**
# MAGIC 1. Missing amounts → Impute from product category median
# MAGIC 2. Invalid currencies → Default to ZAR for South African regions
# MAGIC 3. Missing customer IDs → Flag as "UNKNOWN" with synthetic ID
# MAGIC 4. Future dates → Cap to current timestamp
# MAGIC 5. Negative amounts → Take absolute value (if reversal type)
# MAGIC
# MAGIC **Principle:** Better to recover with documented assumptions than to lose data.

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# ─── Configuration ───────────────────────────────────────────────────────────

dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run Mode")
dry_run = dbutils.widgets.get("dry_run") == "true"

if dry_run:
    print("🔍 DRY RUN MODE — No records will be promoted to Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Quarantined Records

# COMMAND ----------

quarantine_df = spark.read.format("delta").load(QUARANTINE_PATH)
total_quarantined = quarantine_df.count()

print(f"Total quarantined records: {total_quarantined:,}")
print("\nQuarantine Reason Distribution:")
quarantine_df.groupBy("_quarantine_reason").count().orderBy(F.desc("count")).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Calculate Product Medians (for amount imputation)

# COMMAND ----------

silver_df = spark.read.format("delta").load(SILVER_TRANSACTIONS)

product_medians = (
    silver_df
    .groupBy("product_category")
    .agg(
        F.expr("percentile_approx(amount_in_zar, 0.5)").alias("median_amount_zar"),
        F.avg("amount_in_zar").alias("avg_amount_zar"),
    )
)

print("Product Category Medians (for imputation):")
product_medians.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply Fix-Up Rules

# COMMAND ----------

def apply_fixups(df, product_medians_df):
    """
    Apply fix-up rules to quarantined records.
    Each fix adds a _fixup_applied column documenting what was changed.
    """

    fixed = df

    # ─── Fix 1: Missing Amounts → Impute from product median ────────
    fixed = fixed.join(
        product_medians_df.select("product_category", "median_amount_zar"),
        on="product_category",
        how="left",
    )

    fixed = fixed.withColumn(
        "transaction_amount",
        F.when(
            F.col("transaction_amount").isNull() & F.col("median_amount_zar").isNotNull(),
            F.col("median_amount_zar")
        ).otherwise(F.col("transaction_amount"))
    )

    fixed = fixed.withColumn(
        "_fix_amount_imputed",
        F.when(
            F.col("_quarantine_reason").contains("invalid_amount") &
            F.col("transaction_amount").isNotNull(),
            True
        ).otherwise(False)
    )

    # ─── Fix 2: Negative Amounts → Absolute value for reversals ─────
    fixed = fixed.withColumn(
        "transaction_amount",
        F.when(
            (F.col("transaction_amount") < 0) &
            (F.col("transaction_type").isin("reversal", "credit")),
            F.abs(F.col("transaction_amount"))
        ).otherwise(F.col("transaction_amount"))
    )

    fixed = fixed.withColumn(
        "_fix_negative_corrected",
        F.when(
            F.col("_quarantine_reason").contains("invalid_amount") &
            (F.col("transaction_amount") > 0),
            True
        ).otherwise(False)
    )

    # ─── Fix 3: Invalid Currency → Default to ZAR for SA regions ────
    fixed = fixed.withColumn(
        "currency",
        F.when(
            ~F.col("currency").isin(VALID_CURRENCIES) &
            F.col("region_code").isin(["GP", "WC", "KZN", "EC", "FS", "LP", "MP", "NW", "NC"]),
            F.lit("ZAR")
        ).when(
            ~F.col("currency").isin(VALID_CURRENCIES),
            F.lit("USD")  # Default for international
        ).otherwise(F.col("currency"))
    )

    fixed = fixed.withColumn(
        "_fix_currency_defaulted",
        F.when(F.col("_quarantine_reason").contains("invalid_currency"), True).otherwise(False)
    )

    # ─── Fix 4: Future Dates → Cap to current timestamp ─────────────
    fixed = fixed.withColumn(
        "transaction_date",
        F.when(
            F.to_timestamp(F.col("transaction_date")) > F.current_timestamp(),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")
        ).otherwise(F.col("transaction_date"))
    )

    fixed = fixed.withColumn(
        "_fix_date_capped",
        F.when(F.col("_quarantine_reason").contains("invalid_date"), True).otherwise(False)
    )

    # ─── Fix 5: Missing Customer → Synthetic ID ─────────────────────
    fixed = fixed.withColumn(
        "customer_id",
        F.when(
            F.col("customer_id").isNull(),
            F.concat(F.lit("UNKNOWN-"), F.substring(F.col("transaction_id"), 1, 8))
        ).otherwise(F.col("customer_id"))
    )

    fixed = fixed.withColumn(
        "customer_name",
        F.when(F.col("customer_name").isNull(), F.lit("Unknown Customer"))
         .otherwise(F.col("customer_name"))
    )

    fixed = fixed.withColumn(
        "_fix_customer_synthetic",
        F.when(F.col("_quarantine_reason").contains("missing_customer"), True).otherwise(False)
    )

    # ─── Composite: Document all fixes applied ──────────────────────
    fixed = fixed.withColumn(
        "_fixups_applied",
        F.concat_ws(", ",
            F.when(F.col("_fix_amount_imputed"), F.lit("amount_imputed")),
            F.when(F.col("_fix_negative_corrected"), F.lit("negative_corrected")),
            F.when(F.col("_fix_currency_defaulted"), F.lit("currency_defaulted")),
            F.when(F.col("_fix_date_capped"), F.lit("date_capped")),
            F.when(F.col("_fix_customer_synthetic"), F.lit("customer_synthetic")),
        )
    )

    fixed = fixed.withColumn(
        "_was_recovered",
        F.col("_fixups_applied") != ""
    )

    return fixed

# COMMAND ----------

fixed_df = apply_fixups(quarantine_df, product_medians)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Re-Validate Fixed Records

# COMMAND ----------

# Re-apply critical validation rules
revalidated = (
    fixed_df
    .withColumn("_revalid_has_id",
        F.col("transaction_id").isNotNull() & (F.length(F.col("transaction_id")) > 0))
    .withColumn("_revalid_has_date",
        F.to_timestamp(F.col("transaction_date")).isNotNull())
    .withColumn("_revalid_has_amount",
        F.col("transaction_amount").isNotNull() & (F.col("transaction_amount") > 0))
    .withColumn("_revalid_has_currency",
        F.col("currency").isin(VALID_CURRENCIES))
    .withColumn("_revalid_has_customer",
        F.col("customer_id").isNotNull())
    .withColumn("_passes_revalidation",
        F.col("_revalid_has_id") &
        F.col("_revalid_has_date") &
        F.col("_revalid_has_amount") &
        F.col("_revalid_has_currency") &
        F.col("_revalid_has_customer")
    )
)

recoverable = revalidated.filter(F.col("_passes_revalidation") == True)
unrecoverable = revalidated.filter(F.col("_passes_revalidation") == False)

recoverable_count = recoverable.count()
unrecoverable_count = unrecoverable.count()
recovery_rate = round(recoverable_count / total_quarantined * 100, 1) if total_quarantined > 0 else 0

print(f"\nRecovery Results:")
print(f"  ✓ Recoverable   : {recoverable_count:,} ({recovery_rate}%)")
print(f"  ✗ Unrecoverable : {unrecoverable_count:,} ({100 - recovery_rate}%)")

print(f"\nFix-Up Distribution:")
recoverable.groupBy("_fixups_applied").count().orderBy(F.desc("count")).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Promote Recovered Records to Silver

# COMMAND ----------

if not dry_run and recoverable_count > 0:
    # Prepare for Silver (apply same transforms as notebook 02)
    from pyspark.sql import functions as F

    exchange_mapping = F.create_map([
        F.lit(x) for pair in EXCHANGE_RATES_TO_ZAR.items() for x in pair
    ])

    promoted = (
        recoverable
        .withColumn("transaction_date", F.to_timestamp(F.col("transaction_date")))
        .withColumn("exchange_rate_to_zar",
            F.coalesce(exchange_mapping[F.col("currency")], F.lit(1.0)))
        .withColumn("amount_in_zar",
            F.round(F.col("transaction_amount") * F.col("exchange_rate_to_zar"), 2))
        .withColumn("transaction_date_key", F.to_date(F.col("transaction_date")))
        .withColumn("transaction_year", F.year(F.col("transaction_date")))
        .withColumn("transaction_month", F.month(F.col("transaction_date")))
        .withColumn("transaction_day", F.dayofmonth(F.col("transaction_date")))
        .withColumn("transaction_hour", F.hour(F.col("transaction_date")))
        .withColumn("day_of_week", F.date_format(F.col("transaction_date"), "EEEE"))
        .withColumn("is_business_hours",
            F.when(
                (F.hour(F.col("transaction_date")).between(8, 16)) &
                (F.dayofweek(F.col("transaction_date")).between(2, 6)),
                True
            ).otherwise(False))
        .withColumn("amount_bucket",
            F.when(F.col("amount_in_zar") < 100, "micro (<R100)")
             .when(F.col("amount_in_zar") < 1_000, "small (R100-R1K)")
             .when(F.col("amount_in_zar") < 10_000, "medium (R1K-R10K)")
             .when(F.col("amount_in_zar") < 100_000, "large (R10K-R100K)")
             .when(F.col("amount_in_zar") < 1_000_000, "major (R100K-R1M)")
             .otherwise("enterprise (R1M+)"))
        .withColumn("is_weekend", F.col("is_weekend").cast("boolean"))
        .withColumn("is_month_end", F.col("is_month_end").cast("boolean"))
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .withColumn("_recovered_from_quarantine", F.lit(True))
        .withColumn("_recovery_timestamp", F.current_timestamp())
    )

    # Select only Silver-compatible columns
    silver_cols = [f.name for f in spark.read.format("delta").load(SILVER_TRANSACTIONS).schema.fields]
    extra_cols = ["_recovered_from_quarantine", "_recovery_timestamp", "_fixups_applied"]

    promoted_final = promoted.select(*[c for c in silver_cols + extra_cols if c in promoted.columns])

    (
        promoted_final.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("transaction_year", "transaction_month")
        .save(SILVER_TRANSACTIONS)
    )

    print(f"\n✓ {recoverable_count:,} records promoted from Quarantine → Silver")
else:
    if dry_run:
        print(f"\n🔍 DRY RUN: Would promote {recoverable_count:,} records to Silver")
    else:
        print("\n✓ No recoverable records to promote")

