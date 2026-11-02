
"""
Schema Definitions
====================
Enforced schemas for each Medallion layer.
Ensures type safety and catches schema drift early.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, BooleanType, LongType, DateType,
)

# ─── Bronze Schema (matches raw CSV structure) ──────────────────────────────

BRONZE_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("batch_id", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("customer_name", StringType(), nullable=True),
    StructField("account_id", StringType(), nullable=True),
    StructField("transaction_date", StringType(), nullable=True),
    StructField("transaction_type", StringType(), nullable=True),
    StructField("transaction_amount", DoubleType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("exchange_rate_to_zar", DoubleType(), nullable=True),
    StructField("amount_in_zar", DoubleType(), nullable=True),
    StructField("product_name", StringType(), nullable=True),
    StructField("product_category", StringType(), nullable=True),
    StructField("region_code", StringType(), nullable=True),
    StructField("region_name", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("merchant_name", StringType(), nullable=True),
    StructField("reference_number", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("is_weekend", BooleanType(), nullable=True),
    StructField("is_month_end", BooleanType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("_defect_injected", StringType(), nullable=True),
])

# ─── Silver Schema (cleansed, typed, enriched) ──────────────────────────────

SILVER_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("batch_id", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=False),
    StructField("customer_name", StringType(), nullable=True),
    StructField("account_id", StringType(), nullable=True),
    StructField("transaction_date", TimestampType(), nullable=False),
    StructField("transaction_date_key", DateType(), nullable=False),
    StructField("transaction_year", LongType(), nullable=False),
    StructField("transaction_month", LongType(), nullable=False),
    StructField("transaction_day", LongType(), nullable=False),
    StructField("transaction_hour", LongType(), nullable=False),
    StructField("transaction_type", StringType(), nullable=False),
    StructField("transaction_amount", DoubleType(), nullable=False),
    StructField("currency", StringType(), nullable=False),
    StructField("exchange_rate_to_zar", DoubleType(), nullable=False),
    StructField("amount_in_zar", DoubleType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("product_category", StringType(), nullable=False),
    StructField("region_code", StringType(), nullable=False),
    StructField("region_name", StringType(), nullable=False),
    StructField("channel", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("merchant_name", StringType(), nullable=True),
    StructField("reference_number", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("is_weekend", BooleanType(), nullable=False),
    StructField("is_month_end", BooleanType(), nullable=False),
    StructField("day_of_week", StringType(), nullable=False),
    StructField("is_business_hours", BooleanType(), nullable=False),
    StructField("amount_bucket", StringType(), nullable=False),
    StructField("_silver_loaded_at", TimestampType(), nullable=False),
    StructField("_source_file", StringType(), nullable=True),
])

