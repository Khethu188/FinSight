
"""
Data Quality Rules Configuration
===================================
Centralized, declarative quality rules for all Medallion layers.
Each rule is a dictionary with:
  - name: Human-readable rule name
  - layer: Which layer it applies to (bronze, silver, gold)
  - severity: critical | warning | info
  - check_type: completeness | validity | uniqueness | freshness | consistency | statistical
  - column: Target column (or None for table-level checks)
  - threshold: Pass/fail threshold
  - description: What this rule checks
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Severity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class CheckType(str, Enum):
    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    FRESHNESS = "freshness"
    CONSISTENCY = "consistency"
    STATISTICAL = "statistical"
    VOLUME = "volume"
    REFERENTIAL = "referential"


@dataclass
class QualityRule:
    """A single data quality rule definition."""
    name: str
    layer: str
    severity: Severity
    check_type: CheckType
    description: str
    column: Optional[str] = None
    threshold: float = 0.95
    params: dict = field(default_factory=dict)

    @property
    def rule_id(self) -> str:
        return f"{self.layer}_{self.check_type.value}_{self.name}".lower().replace(" ", "_")


# ─── Bronze Layer Rules ─────────────────────────────────────────────────────

BRONZE_RULES = [
    QualityRule(
        name="transaction_id_completeness",
        layer="bronze",
        severity=Severity.CRITICAL,
        check_type=CheckType.COMPLETENESS,
        column="transaction_id",
        threshold=0.99,
        description="Transaction ID must be present in 99%+ of records",
    ),
    QualityRule(
        name="transaction_date_completeness",
        layer="bronze",
        severity=Severity.CRITICAL,
        check_type=CheckType.COMPLETENESS,
        column="transaction_date",
        threshold=0.98,
        description="Transaction date must be present in 98%+ of records",
    ),
    QualityRule(
        name="amount_completeness",
        layer="bronze",
        severity=Severity.WARNING,
        check_type=CheckType.COMPLETENESS,
        column="transaction_amount",
        threshold=0.95,
        description="Transaction amount should be present in 95%+ of records",
    ),
    QualityRule(
        name="daily_volume_check",
        layer="bronze",
        severity=Severity.WARNING,
        check_type=CheckType.VOLUME,
        threshold=100,
        description="Bronze should receive at least 100 records per ingestion",
        params={"min_records": 100, "max_records": 500_000},
    ),
    QualityRule(
        name="ingestion_freshness",
        layer="bronze",
        severity=Severity.CRITICAL,
        check_type=CheckType.FRESHNESS,
        column="_bronze_ingested_at",
        threshold=24,
        description="Bronze data must be no older than 24 hours",
        params={"max_hours": 24},
    ),
]

# ─── Silver Layer Rules ─────────────────────────────────────────────────────

SILVER_RULES = [
    QualityRule(
        name="transaction_id_uniqueness",
        layer="silver",
        severity=Severity.CRITICAL,
        check_type=CheckType.UNIQUENESS,
        column="transaction_id",
        threshold=0.999,
        description="Transaction IDs must be 99.9%+ unique after deduplication",
    ),
    QualityRule(
        name="customer_id_completeness",
        layer="silver",
        severity=Severity.CRITICAL,
        check_type=CheckType.COMPLETENESS,
        column="customer_id",
        threshold=1.0,
        description="All Silver records must have a customer_id (quarantine removes nulls)",
    ),
    QualityRule(
        name="amount_positivity",
        layer="silver",
        severity=Severity.CRITICAL,
        check_type=CheckType.VALIDITY,
        column="amount_in_zar",
        threshold=1.0,
        description="All Silver amounts must be positive",
        params={"condition": "amount_in_zar > 0"},
    ),
    QualityRule(
        name="currency_validity",
        layer="silver",
        severity=Severity.CRITICAL,
        check_type=CheckType.VALIDITY,
        column="currency",
        threshold=1.0,
        description="All currencies must be in the valid set",
        params={"valid_values": ["ZAR", "USD", "EUR", "GBP"]},
    ),
    QualityRule(
        name="status_validity",
        layer="silver",
        severity=Severity.CRITICAL,
        check_type=CheckType.VALIDITY,
        column="status",
        threshold=1.0,
        description="All statuses must be in the valid set",
        params={"valid_values": ["completed", "pending", "failed", "reversed", "flagged"]},
    ),
    QualityRule(
        name="date_range_validity",
        layer="silver",
        severity=Severity.WARNING,
        check_type=CheckType.VALIDITY,
        column="transaction_date",
        threshold=1.0,
        description="No transaction dates in the future",
        params={"condition": "transaction_date <= current_timestamp()"},
    ),
    QualityRule(
        name="amount_statistical_range",
        layer="silver",
        severity=Severity.WARNING,
        check_type=CheckType.STATISTICAL,
        column="amount_in_zar",
        threshold=0.99,
        description="99% of amounts should be within 4 standard deviations",
        params={"max_zscore": 4.0},
    ),
    QualityRule(
        name="exchange_rate_consistency",
        layer="silver",
        severity=Severity.WARNING,
        check_type=CheckType.CONSISTENCY,
        column="exchange_rate_to_zar",
        threshold=0.99,
        description="Exchange rates must be consistent with currency",
        params={"expected_rates": {"ZAR": 1.0, "USD": 18.45, "EUR": 19.82, "GBP": 23.15}},
    ),
    QualityRule(
        name="bronze_to_silver_recovery",
        layer="silver",
        severity=Severity.WARNING,
        check_type=CheckType.VOLUME,
        threshold=0.90,
        description="At least 90% of Bronze records should make it to Silver",
        params={"min_recovery_rate": 0.90},
    ),
]

# ─── Gold Layer Rules ────────────────────────────────────────────────────────

GOLD_RULES = [
    QualityRule(
        name="revenue_non_negative",
        layer="gold",
        severity=Severity.CRITICAL,
        check_type=CheckType.VALIDITY,
        column="total_revenue_zar",
        threshold=1.0,
        description="Revenue aggregates must be non-negative",
        params={"condition": "total_revenue_zar >= 0"},
    ),
    QualityRule(
        name="customer_count_consistency",
        layer="gold",
        severity=Severity.WARNING,
        check_type=CheckType.CONSISTENCY,
        column="unique_customers",
        threshold=1.0,
        description="Unique customer count must not exceed transaction count",
        params={"condition": "unique_customers <= transaction_count"},
    ),
    QualityRule(
        name="all_regions_represented",
        layer="gold",
        severity=Severity.INFO,
        check_type=CheckType.REFERENTIAL,
        column="region_code",
        threshold=0.80,
        description="At least 80% of known regions should appear in Gold aggregates",
        params={"expected_values": ["GP", "WC", "KZN", "EC", "FS", "LP", "MP", "NW", "NC", "INT"]},
    ),
    QualityRule(
        name="gold_freshness",
        layer="gold",
        severity=Severity.WARNING,
        check_type=CheckType.FRESHNESS,
        column="_gold_generated_at",
        threshold=48,
        description="Gold tables must be refreshed within 48 hours",
        params={"max_hours": 48},
    ),
]

# ─── Cross-Layer Rules ───────────────────────────────────────────────────────

CROSS_LAYER_RULES = [
    QualityRule(
        name="bronze_silver_count_reconciliation",
        layer="cross",
        severity=Severity.CRITICAL,
        check_type=CheckType.CONSISTENCY,
        threshold=0.95,
        description="Silver + Quarantine count should equal Bronze count (±5%)",
        params={"tolerance_pct": 0.05},
    ),
    QualityRule(
        name="silver_gold_amount_reconciliation",
        layer="cross",
        severity=Severity.WARNING,
        check_type=CheckType.CONSISTENCY,
        threshold=0.99,
        description="Gold revenue totals should reconcile with Silver completed transactions",
        params={"tolerance_pct": 0.01},
    ),
]

# ─── All Rules ───────────────────────────────────────────────────────────────

ALL_RULES = BRONZE_RULES + SILVER_RULES + GOLD_RULES + CROSS_LAYER_RULES

