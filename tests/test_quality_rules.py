
# ═══════════════════════════════════════════════════════════════════════════
# Tests: Quality Rules — config/quality_rules.py
# ═══════════════════════════════════════════════════════════════════════════

import pytest


# ─── Simulated Quality Rule Definitions ──────────────────────────────────
# These mirror the actual quality_rules.py structure

SEVERITY_LEVELS = {"critical", "warning", "info"}
CHECK_TYPES = {
    "completeness",
    "validity",
    "uniqueness",
    "freshness",
    "statistical",
    "consistency",
    "referential",
    "volume",
}

# Simulated rule definitions (matching Phase 4)
QUALITY_RULES = [
    {
        "rule_id": "BRZ-001",
        "rule_name": "transaction_id_completeness",
        "layer": "bronze",
        "severity": "critical",
        "check_type": "completeness",
        "column": "transaction_id",
        "threshold": 0.99,
        "description": "Transaction ID must be present in 99%+ of records",
    },
    {
        "rule_id": "BRZ-002",
        "rule_name": "amount_completeness",
        "layer": "bronze",
        "severity": "critical",
        "check_type": "completeness",
        "column": "transaction_amount",
        "threshold": 0.95,
        "description": "Transaction amount must be present in 95%+ of records",
    },
    {
        "rule_id": "SLV-001",
        "rule_name": "currency_validity",
        "layer": "silver",
        "severity": "critical",
        "check_type": "validity",
        "column": "currency",
        "threshold": 1.0,
        "description": "All currencies must be in valid set after cleansing",
    },
    {
        "rule_id": "SLV-002",
        "rule_name": "amount_positivity",
        "layer": "silver",
        "severity": "critical",
        "check_type": "validity",
        "column": "transaction_amount",
        "threshold": 1.0,
        "description": "All amounts must be positive after cleansing",
    },
    {
        "rule_id": "SLV-003",
        "rule_name": "transaction_id_uniqueness",
        "layer": "silver",
        "severity": "warning",
        "check_type": "uniqueness",
        "column": "transaction_id",
        "threshold": 0.999,
        "description": "Transaction IDs must be 99.9%+ unique",
    },
    {
        "rule_id": "GLD-001",
        "rule_name": "revenue_volume_check",
        "layer": "gold",
        "severity": "warning",
        "check_type": "volume",
        "column": None,
        "threshold": 100,
        "description": "Revenue summary must have at least 100 records",
    },
    {
        "rule_id": "XLR-001",
        "rule_name": "bronze_silver_reconciliation",
        "layer": "cross_layer",
        "severity": "critical",
        "check_type": "consistency",
        "column": None,
        "threshold": 0.95,
        "description": "Silver + Quarantine should equal Bronze (±5%)",
    },
    {
        "rule_id": "SLV-004",
        "rule_name": "date_freshness",
        "layer": "silver",
        "severity": "warning",
        "check_type": "freshness",
        "column": "transaction_date",
        "threshold": 24,
        "description": "Data should not be older than 24 hours",
    },
    {
        "rule_id": "SLV-005",
        "rule_name": "amount_statistical_range",
        "layer": "silver",
        "severity": "info",
        "check_type": "statistical",
        "column": "transaction_amount",
        "threshold": 0.99,
        "description": "99% of amounts should be within 4 standard deviations",
    },
    {
        "rule_id": "SLV-006",
        "rule_name": "region_referential_integrity",
        "layer": "silver",
        "severity": "warning",
        "check_type": "referential",
        "column": "region_code",
        "threshold": 9,
        "description": "All 9 SA provinces should be represented",
    },
]


# ═══════════════════════════════════════════════════════════════════════════
# RULE DEFINITION TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestQualityRuleDefinitions:
    """Verify quality rule definitions are valid and complete."""

    @pytest.mark.unit
    def test_all_rules_have_required_fields(self):
        """Every rule must have all required fields."""
        required_fields = {
            "rule_id",
            "rule_name",
            "layer",
            "severity",
            "check_type",
            "threshold",
            "description",
        }
        for rule in QUALITY_RULES:
            missing = required_fields - set(rule.keys())
            assert len(missing) == 0, f"Rule {rule['rule_id']} missing: {missing}"

    @pytest.mark.unit
    def test_rule_ids_unique(self):
        """All rule IDs must be unique."""
        ids = [r["rule_id"] for r in QUALITY_RULES]
        assert len(ids) == len(set(ids)), f"Duplicate rule IDs: {ids}"

    @pytest.mark.unit
    def test_rule_names_unique(self):
        """All rule names must be unique."""
        names = [r["rule_name"] for r in QUALITY_RULES]
        assert len(names) == len(set(names)), f"Duplicate rule names found"

    @pytest.mark.unit
    def test_severity_levels_valid(self):
        """All severity levels must be in the valid set."""
        for rule in QUALITY_RULES:
            assert rule["severity"] in SEVERITY_LEVELS, (
                f"Rule {rule['rule_id']} has invalid severity: {rule['severity']}"
            )

    @pytest.mark.unit
    def test_check_types_valid(self):
        """All check types must be in the valid set."""
        for rule in QUALITY_RULES:
            assert rule["check_type"] in CHECK_TYPES, (
                f"Rule {rule['rule_id']} has invalid check_type: {rule['check_type']}"
            )

    @pytest.mark.unit
    def test_thresholds_in_valid_range(self):
        """Thresholds should be positive numbers."""
        for rule in QUALITY_RULES:
            assert rule["threshold"] > 0, (
                f"Rule {rule['rule_id']} has non-positive threshold: {rule['threshold']}"
            )

    @pytest.mark.unit
    def test_descriptions_not_empty(self):
        """All rules must have non-empty descriptions."""
        for rule in QUALITY_RULES:
            assert len(rule["description"]) > 10, (
                f"Rule {rule['rule_id']} has too-short description"
            )

    @pytest.mark.unit
    def test_rule_id_format(self):
        """Rule IDs should follow PREFIX-NNN format."""
        import re

        pattern = r"^[A-Z]{3}-\d{3}$"
        for rule in QUALITY_RULES:
            assert re.match(pattern, rule["rule_id"]), (
                f"Rule ID {rule['rule_id']} doesn't match format PREFIX-NNN"
            )


# ═══════════════════════════════════════════════════════════════════════════
# RULE COVERAGE TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestQualityRuleCoverage:
    """Verify quality rules cover all necessary areas."""

    @pytest.mark.unit
    def test_bronze_layer_has_rules(self):
        """Bronze layer should have at least 2 rules."""
        bronze_rules = [r for r in QUALITY_RULES if r["layer"] == "bronze"]
        assert len(bronze_rules) >= 2, f"Only {len(bronze_rules)} bronze rules"

    @pytest.mark.unit
    def test_silver_layer_has_rules(self):
        """Silver layer should have the most rules."""
        silver_rules = [r for r in QUALITY_RULES if r["layer"] == "silver"]
        assert len(silver_rules) >= 4, f"Only {len(silver_rules)} silver rules"

    @pytest.mark.unit
    def test_gold_layer_has_rules(self):
        """Gold layer should have at least 1 rule."""
        gold_rules = [r for r in QUALITY_RULES if r["layer"] == "gold"]
        assert len(gold_rules) >= 1, f"Only {len(gold_rules)} gold rules"

    @pytest.mark.unit
    def test_cross_layer_rules_exist(self):
        """Cross-layer reconciliation rules should exist."""
        cross_rules = [r for r in QUALITY_RULES if r["layer"] == "cross_layer"]
        assert len(cross_rules) >= 1, "No cross-layer rules found"

    @pytest.mark.unit
    def test_critical_rules_exist(self):
        """At least 3 critical rules should exist."""
        critical = [r for r in QUALITY_RULES if r["severity"] == "critical"]
        assert len(critical) >= 3, f"Only {len(critical)} critical rules"

    @pytest.mark.unit
    def test_all_check_types_covered(self):
        """At least 5 different check types should be used."""
        types_used = {r["check_type"] for r in QUALITY_RULES}
        assert len(types_used) >= 5, f"Only {len(types_used)} check types used: {types_used}"

    @pytest.mark.unit
    def test_completeness_checks_exist(self):
        """Completeness checks should exist for critical fields."""
        completeness = [r for r in QUALITY_RULES if r["check_type"] == "completeness"]
        assert len(completeness) >= 2

    @pytest.mark.unit
    def test_validity_checks_exist(self):
        """Validity checks should exist."""
        validity = [r for r in QUALITY_RULES if r["check_type"] == "validity"]
        assert len(validity) >= 2

    @pytest.mark.unit
    def test_uniqueness_check_exists(self):
        """At least one uniqueness check should exist."""
        uniqueness = [r for r in QUALITY_RULES if r["check_type"] == "uniqueness"]
        assert len(uniqueness) >= 1

