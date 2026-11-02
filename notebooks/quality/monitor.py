
# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Data Quality Monitor
# MAGIC
# MAGIC **Purpose:** Run comprehensive data quality checks across all Medallion layers.
# MAGIC Produces a quality scorecard, logs results to a metrics Delta table,
# MAGIC and triggers alerts for critical failures.
# MAGIC
# MAGIC **Runs:** After every pipeline execution (orchestrated via Databricks Workflow)
# MAGIC
# MAGIC **Outputs:**
# MAGIC - Quality scorecard (printed + logged)
# MAGIC - Quality metrics Delta table (historical tracking)
# MAGIC - Alert payloads for SNS (critical failures)

# COMMAND ----------

# MAGIC %run ../utilities/common_functions

# COMMAND ----------

# MAGIC %run ../../config/pipeline_config

# COMMAND ----------

# MAGIC %run ../../config/quality_rules

# COMMAND ----------

import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, BooleanType, LongType,
)

# ─── Configuration ───────────────────────────────────────────────────────────

QUALITY_METRICS_PATH = f"{S3_BUCKET_GOLD}/quality_metrics"

dbutils.widgets.text("run_id", datetime.now().strftime("%Y%m%d_%H%M%S"), "Pipeline Run ID")
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Check Engine

# COMMAND ----------

class QualityCheckEngine:
    """
    Executes data quality rules against DataFrames and collects results.
    """

    def __init__(self, spark, run_id: str):
        self.spark = spark
        self.run_id = run_id
        self.results: list[dict] = []
        self.check_timestamp = datetime.now()

    def _record_result(self, rule: QualityRule, actual_value: float, record_count: int, details: str = ""):
        """Record a single check result."""
        passed = actual_value >= rule.threshold
        result = {
            "run_id": self.run_id,
            "check_timestamp": self.check_timestamp.isoformat(),
            "rule_id": rule.rule_id,
            "rule_name": rule.name,
            "layer": rule.layer,
            "severity": rule.severity.value,
            "check_type": rule.check_type.value,
            "column": rule.column or "table_level",
            "threshold": rule.threshold,
            "actual_value": round(actual_value, 6),
            "passed": passed,
            "record_count": record_count,
            "details": details,
            "description": rule.description,
        }
        self.results.append(result)

        status = "✓ PASS" if passed else "✗ FAIL"
        severity_icon = {"critical": "🔴", "warning": "🟡", "info": "🔵"}
        icon = severity_icon.get(rule.severity.value, "⚪")

        print(f"  {icon} {status} | {rule.name}: {actual_value:.4f} (threshold: {rule.threshold}) {details}")

    def check_completeness(self, df, rule: QualityRule):
        """Check what percentage of a column is non-null."""
        total = df.count()
        if total == 0:
            self._record_result(rule, 0.0, 0, "Empty DataFrame")
            return

        non_null = df.filter(F.col(rule.column).isNotNull() & (F.col(rule.column) != "")).count()
        rate = non_null / total
        self._record_result(rule, rate, total)

    def check_uniqueness(self, df, rule: QualityRule):
        """Check what percentage of values in a column are unique."""
        total = df.count()
        if total == 0:
            self._record_result(rule, 0.0, 0, "Empty DataFrame")
            return

        distinct = df.select(rule.column).distinct().count()
        rate = distinct / total
        self._record_result(rule, rate, total, f"({distinct:,} unique of {total:,})")

    def check_validity(self, df, rule: QualityRule):
        """Check values against valid sets or conditions."""
        total = df.count()
        if total == 0:
            self._record_result(rule, 0.0, 0, "Empty DataFrame")
            return

        if "valid_values" in rule.params:
            valid = df.filter(F.col(rule.column).isin(rule.params["valid_values"])).count()
        elif "condition" in rule.params:
            valid = df.filter(F.expr(rule.params["condition"])).count()
        else:
            valid = total

        rate = valid / total
        self._record_result(rule, rate, total, f"({valid:,} valid of {total:,})")

    def check_freshness(self, df, rule: QualityRule):
        """Check that the most recent record is within the freshness window."""
        max_hours = rule.params.get("max_hours", 24)

        latest = df.agg(F.max(rule.column)).collect()[0][0]
        if latest is None:
            self._record_result(rule, 0.0, 0, "No data found")
            return

        hours_old = (datetime.now() - latest).total_seconds() / 3600
        # Freshness passes if hours_old <= max_hours
        # Normalize: 1.0 = perfectly fresh, 0.0 = stale
        freshness_score = max(0.0, 1.0 - (hours_old / max_hours))
        self._record_result(rule, freshness_score, df.count(), f"({hours_old:.1f}h old, max {max_hours}h)")

    def check_volume(self, df, rule: QualityRule):
        """Check record count is within expected range."""
        count = df.count()
        min_records = rule.params.get("min_records", rule.threshold)
        max_records = rule.params.get("max_records", float("inf"))

        if isinstance(rule.threshold, float) and rule.threshold <= 1.0:
            # Threshold is a ratio (e.g., recovery rate)
            self._record_result(rule, count, count, f"({count:,} records)")
        else:
            # Threshold is an absolute count
            passed_value = 1.0 if min_records <= count <= max_records else 0.0
            self._record_result(rule, passed_value, count, f"({count:,} records, expected {min_records:,}-{max_records:,})")

    def check_statistical(self, df, rule: QualityRule):
        """Check statistical properties (e.g., z-score distribution)."""
        total = df.count()
        if total == 0:
            self._record_result(rule, 0.0, 0, "Empty DataFrame")
            return

        max_zscore = rule.params.get("max_zscore", 4.0)

        stats = df.agg(
            F.mean(rule.column).alias("mean_val"),
            F.stddev(rule.column).alias("std_val"),
        ).collect()[0]

        if stats["std_val"] and stats["std_val"] > 0:
            within_range = df.filter(
                F.abs((F.col(rule.column) - F.lit(stats["mean_val"])) / F.lit(stats["std_val"])) <= max_zscore
            ).count()
            rate = within_range / total
        else:
            rate = 1.0

        self._record_result(rule, rate, total)

    def check_consistency(self, df, rule: QualityRule):
        """Check cross-field or cross-table consistency."""
        total = df.count()
        if total == 0:
            self._record_result(rule, 0.0, 0, "Empty DataFrame")
            return

        if "condition" in rule.params:
            consistent = df.filter(F.expr(rule.params["condition"])).count()
            rate = consistent / total
            self._record_result(rule, rate, total)
        else:
            self._record_result(rule, 1.0, total, "No condition specified")

    def check_referential(self, df, rule: QualityRule):
        """Check that expected reference values are present."""
        expected = set(rule.params.get("expected_values", []))
        if not expected:
            self._record_result(rule, 1.0, df.count(), "No expected values")
            return

        actual = set(row[0] for row in df.select(rule.column).distinct().collect())
        coverage = len(actual & expected) / len(expected)
        missing = expected - actual
        details = f"Missing: {missing}" if missing else "All present"
        self._record_result(rule, coverage, df.count(), details)

    def run_check(self, df, rule: QualityRule):
        """Route a rule to the appropriate check method."""
        check_map = {
            CheckType.COMPLETENESS: self.check_completeness,
            CheckType.UNIQUENESS: self.check_uniqueness,
            CheckType.VALIDITY: self.check_validity,
            CheckType.FRESHNESS: self.check_freshness,
            CheckType.VOLUME: self.check_volume,
            CheckType.STATISTICAL: self.check_statistical,
            CheckType.CONSISTENCY: self.check_consistency,
            CheckType.REFERENTIAL: self.check_referential,
        }

        checker = check_map.get(rule.check_type)
        if checker:
            try:
                checker(df, rule)
            except Exception as e:
                self._record_result(rule, 0.0, 0, f"ERROR: {str(e)}")
        else:
            print(f"  ⚠ Unknown check type: {rule.check_type}")

    def get_results_df(self):
        """Convert results to a Spark DataFrame."""
        schema = StructType([
            StructField("run_id", StringType()),
            StructField("check_timestamp", StringType()),
            StructField("rule_id", StringType()),
            StructField("rule_name", StringType()),
            StructField("layer", StringType()),
            StructField("severity", StringType()),
            StructField("check_type", StringType()),
            StructField("column", StringType()),
            StructField("threshold", DoubleType()),
            StructField("actual_value", DoubleType()),
            StructField("passed", BooleanType()),
            StructField("record_count", LongType()),
            StructField("details", StringType()),
            StructField("description", StringType()),
        ])
        return self.spark.createDataFrame(self.results, schema)

    def get_scorecard(self) -> dict:
        """Generate a summary scorecard."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r["passed"])
        failed = total - passed

        critical_failures = [r for r in self.results if not r["passed"] and r["severity"] == "critical"]
        warning_failures = [r for r in self.results if not r["passed"] and r["severity"] == "warning"]

        return {
            "run_id": self.run_id,
            "timestamp": self.check_timestamp.isoformat(),
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": round(passed / total * 100, 1) if total > 0 else 0,
            "critical_failures": len(critical_failures),
            "warning_failures": len(warning_failures),
            "critical_failure_details": [
                {"rule": r["rule_name"], "actual": r["actual_value"], "threshold": r["threshold"]}
                for r in critical_failures
            ],
            "overall_health": "HEALTHY" if len(critical_failures) == 0 else "DEGRADED" if len(critical_failures) <= 2 else "CRITICAL",
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute All Quality Checks

# COMMAND ----------

engine = QualityCheckEngine(spark, run_id)

# Load DataFrames
bronze_df = spark.read.format("delta").load(BRONZE_TRANSACTIONS)
silver_df = spark.read.format("delta").load(SILVER_TRANSACTIONS)
quarantine_df = spark.read.format("delta").load(QUARANTINE_PATH)

# ─── Bronze Checks ──────────────────────────────────────────────────────────
print("=" * 65)
print("  BRONZE LAYER CHECKS")
print("=" * 65)
for rule in BRONZE_RULES:
    engine.run_check(bronze_df, rule)

# ─── Silver Checks ──────────────────────────────────────────────────────────
print("\n" + "=" * 65)
print("  SILVER LAYER CHECKS")
print("=" * 65)
for rule in SILVER_RULES:
    if rule.name == "bronze_to_silver_recovery":
        # Special: calculate recovery rate
        bronze_count = bronze_df.count()
        silver_count = silver_df.count()
        recovery_rate = silver_count / bronze_count if bronze_count > 0 else 0
        engine._record_result(rule, recovery_rate, silver_count,
                              f"({silver_count:,} of {bronze_count:,} Bronze records)")
    else:
        engine.run_check(silver_df, rule)

# ─── Gold Checks ────────────────────────────────────────────────────────────
print("\n" + "=" * 65)
print("  GOLD LAYER CHECKS")
print("=" * 65)

try:
    gold_revenue_df = spark.read.format("delta").load(GOLD_REVENUE)
    for rule in GOLD_RULES:
        engine.run_check(gold_revenue_df, rule)
except Exception as e:
    print(f"  ⚠ Gold tables not yet available: {e}")

# ─── Cross-Layer Checks ─────────────────────────────────────────────────────
print("\n" + "=" * 65)
print("  CROSS-LAYER RECONCILIATION")
print("=" * 65)

bronze_count = bronze_df.count()
silver_count = silver_df.count()
quarantine_count = quarantine_df.count()

reconciled = silver_count + quarantine_count
tolerance = CROSS_LAYER_RULES[0].params.get("tolerance_pct", 0.05)
reconciliation_rate = reconciled / bronze_count if bronze_count > 0 else 0

engine._record_result(
    CROSS_LAYER_RULES[0],
    reconciliation_rate,
    bronze_count,
    f"Silver({silver_count:,}) + Quarantine({quarantine_count:,}) = {reconciled:,} vs Bronze({bronze_count:,})"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Scorecard

# COMMAND ----------

scorecard = engine.get_scorecard()

print("\n" + "=" * 65)
print("  📊 DATA QUALITY SCORECARD")
print("=" * 65)
print(f"  Run ID          : {scorecard['run_id']}")
print(f"  Timestamp       : {scorecard['timestamp']}")
print(f"  Total Checks    : {scorecard['total_checks']}")
print(f"  Passed          : {scorecard['passed']} ✓")
print(f"  Failed          : {scorecard['failed']} ✗")
print(f"  Pass Rate       : {scorecard['pass_rate']}%")
print(f"  Critical Fails  : {scorecard['critical_failures']} 🔴")
print(f"  Warning Fails   : {scorecard['warning_failures']} 🟡")
print(f"  Overall Health  : {scorecard['overall_health']}")
print("=" * 65)

if scorecard["critical_failure_details"]:
    print("\n  🔴 CRITICAL FAILURES:")
    for cf in scorecard["critical_failure_details"]:
        print(f"     → {cf['rule']}: {cf['actual']:.4f} (need {cf['threshold']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Quality Metrics

# COMMAND ----------

results_df = engine.get_results_df()

(
    results_df.write
    .format("delta")
    .mode("append")
    .save(QUALITY_METRICS_PATH)
)

print(f"✓ {len(engine.results)} quality metrics persisted to {QUALITY_METRICS_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Alert Payload (for SNS)

# COMMAND ----------

if scorecard["overall_health"] != "HEALTHY":
    alert_payload = {
        "source": "data-quality-monitor",
        "severity": "critical" if scorecard["critical_failures"] > 0 else "warning",
        "pipeline_run_id": run_id,
        "timestamp": scorecard["timestamp"],
        "message": f"Data Quality Alert: {scorecard['overall_health']} — "
                   f"{scorecard['critical_failures']} critical, "
                   f"{scorecard['warning_failures']} warning failures "
                   f"(pass rate: {scorecard['pass_rate']}%)",
        "failures": scorecard["critical_failure_details"],
        "action_required": "Review quarantine table and pipeline logs",
    }

    # Store alert for the alerting framework to pick up
    dbutils.jobs.taskValues.set(key="quality_alert", value=json.dumps(alert_payload))
    print(f"\n⚠ Alert payload generated: {alert_payload['message']}")
else:
    print("\n✓ All checks passed — no alerts needed")

