
"""
Common Utility Functions
==========================
Shared PySpark functions used across Bronze, Silver, and Gold layers.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime


def get_spark() -> SparkSession:
    """Get or create SparkSession."""
    return SparkSession.builder.getOrCreate()


# ─── Metadata Columns ───────────────────────────────────────────────────────

def add_audit_columns(df: DataFrame, layer: str) -> DataFrame:
    """Add standard audit/metadata columns to a DataFrame."""
    return (
        df
        .withColumn(f"_{layer}_loaded_at", F.current_timestamp())
        .withColumn(f"_{layer}_load_date", F.current_date())
        .withColumn("_pipeline_run_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    )


def add_source_file_column(df: DataFrame) -> DataFrame:
    """Add the source file path as a column (works with Auto Loader)."""
    return df.withColumn("_source_file", F.input_file_name())


# ─── Data Quality Helpers ────────────────────────────────────────────────────

def calculate_completeness(df: DataFrame, columns: list[str]) -> dict:
    """Calculate completeness (non-null rate) for specified columns."""
    total = df.count()
    if total == 0:
        return {col: 0.0 for col in columns}

    results = {}
    for col in columns:
        non_null = df.filter(F.col(col).isNotNull()).count()
        results[col] = round(non_null / total, 4)
    return results


def calculate_uniqueness(df: DataFrame, column: str) -> float:
    """Calculate uniqueness rate for a column."""
    total = df.count()
    if total == 0:
        return 0.0
    distinct = df.select(column).distinct().count()
    return round(distinct / total, 4)


def flag_duplicates(df: DataFrame, key_column: str, order_column: str) -> DataFrame:
    """
    Flag duplicate records based on a key column.
    Keeps the first occurrence (by order_column) and flags the rest.
    """
    window = Window.partitionBy(key_column).orderBy(F.col(order_column).asc())
    return (
        df
        .withColumn("_row_number", F.row_number().over(window))
        .withColumn("_is_duplicate", F.when(F.col("_row_number") > 1, True).otherwise(False))
        .drop("_row_number")
    )


# ─── Amount Bucketing ────────────────────────────────────────────────────────

def add_amount_bucket(df: DataFrame, amount_col: str = "amount_in_zar") -> DataFrame:
    """Categorize transactions into amount buckets for analysis."""
    return df.withColumn(
        "amount_bucket",
        F.when(F.col(amount_col) < 100, "micro (<R100)")
         .when(F.col(amount_col) < 1_000, "small (R100-R1K)")
         .when(F.col(amount_col) < 10_000, "medium (R1K-R10K)")
         .when(F.col(amount_col) < 100_000, "large (R10K-R100K)")
         .when(F.col(amount_col) < 1_000_000, "major (R100K-R1M)")
         .otherwise("enterprise (R1M+)")
    )


# ─── Date Enrichment ────────────────────────────────────────────────────────

def add_date_dimensions(df: DataFrame, date_col: str = "transaction_date") -> DataFrame:
    """Extract date dimension columns from a timestamp column."""
    return (
        df
        .withColumn("transaction_date_key", F.to_date(F.col(date_col)))
        .withColumn("transaction_year", F.year(F.col(date_col)))
        .withColumn("transaction_month", F.month(F.col(date_col)))
        .withColumn("transaction_day", F.dayofmonth(F.col(date_col)))
        .withColumn("transaction_hour", F.hour(F.col(date_col)))
        .withColumn("day_of_week", F.date_format(F.col(date_col), "EEEE"))
        .withColumn("is_business_hours",
            F.when(
                (F.hour(F.col(date_col)) >= 8) & (F.hour(F.col(date_col)) < 17) &
                (F.dayofweek(F.col(date_col)).between(2, 6)),  # Mon=2 to Fri=6
                True
            ).otherwise(False)
        )
    )


# ─── Write Helpers ───────────────────────────────────────────────────────────

def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    partition_by: list[str] = None,
    merge_schema: bool = True,
) -> None:
    """Write DataFrame to Delta Lake with standard options."""
    writer = (
        df.write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", str(merge_schema).lower())
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)


def optimize_delta(spark: SparkSession, path: str, z_order_cols: list[str] = None) -> None:
    """Run OPTIMIZE and optional Z-ORDER on a Delta table."""
    spark.sql(f"OPTIMIZE delta.`{path}`")
    if z_order_cols:
        cols = ", ".join(z_order_cols)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")

