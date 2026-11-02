
"""
Pipeline Configuration
========================
Central configuration for all Databricks pipeline parameters.
Reads from Databricks widgets or environment variables with sensible defaults.
"""

# ─── S3 Paths ────────────────────────────────────────────────────────────────

S3_BUCKET_LANDING    = "s3://financial-data-platform-dev-landing"
S3_BUCKET_BRONZE     = "s3://financial-data-platform-dev-bronze"
S3_BUCKET_SILVER     = "s3://financial-data-platform-dev-silver"
S3_BUCKET_GOLD       = "s3://financial-data-platform-dev-gold"
S3_BUCKET_QUARANTINE = "s3://financial-data-platform-dev-quarantine"

# ─── Landing Paths ───────────────────────────────────────────────────────────

LANDING_TRANSACTIONS_PATH = f"{S3_BUCKET_LANDING}/landing/transactions"
LANDING_CHECKPOINT_PATH   = f"{S3_BUCKET_BRONZE}/_checkpoints/landing_to_bronze"

# ─── Delta Table Paths ───────────────────────────────────────────────────────

BRONZE_TRANSACTIONS = f"{S3_BUCKET_BRONZE}/transactions"
SILVER_TRANSACTIONS = f"{S3_BUCKET_SILVER}/transactions"
SILVER_CUSTOMERS    = f"{S3_BUCKET_SILVER}/customers"
GOLD_REVENUE        = f"{S3_BUCKET_GOLD}/revenue_summary"
GOLD_REGIONAL_PNL   = f"{S3_BUCKET_GOLD}/regional_pnl"
GOLD_PRODUCT_PERF   = f"{S3_BUCKET_GOLD}/product_performance"
GOLD_ANOMALIES      = f"{S3_BUCKET_GOLD}/anomaly_flags"
QUARANTINE_PATH     = f"{S3_BUCKET_QUARANTINE}/bad_records"

# ─── Database Names ──────────────────────────────────────────────────────────

CATALOG  = "financial_platform"
DB_BRONZE = "bronze"
DB_SILVER = "silver"
DB_GOLD   = "gold"

# ─── Quality Thresholds ─────────────────────────────────────────────────────

QUALITY_THRESHOLDS = {
    "completeness_min": 0.95,       # 95% of required fields must be non-null
    "uniqueness_min": 0.99,         # 99% unique transaction IDs expected
    "amount_max": 50_000_000.00,    # Max realistic transaction amount (ZAR)
    "amount_min": 0.01,             # Min realistic transaction amount
    "future_date_tolerance_hours": 2, # Allow up to 2 hours ahead (timezone drift)
    "duplicate_window_hours": 24,   # Check duplicates within 24-hour window
    "anomaly_zscore_threshold": 3.0, # Z-score threshold for anomaly detection
    "anomaly_iqr_multiplier": 1.5,  # IQR multiplier for outlier detection
}

# ─── Valid Value Sets ────────────────────────────────────────────────────────

VALID_CURRENCIES = ["ZAR", "USD", "EUR", "GBP"]
VALID_STATUSES = ["completed", "pending", "failed", "reversed", "flagged"]
VALID_CHANNELS = ["branch", "online", "mobile_app", "atm", "pos_terminal", "api"]
VALID_TRANSACTION_TYPES = ["debit", "credit", "transfer", "reversal", "fee", "interest"]
VALID_REGIONS = ["GP", "WC", "KZN", "EC", "FS", "LP", "MP", "NW", "NC", "INT"]

EXCHANGE_RATES_TO_ZAR = {
    "ZAR": 1.00,
    "USD": 18.45,
    "EUR": 19.82,
    "GBP": 23.15,
}

