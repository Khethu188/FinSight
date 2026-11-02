
# ═══════════════════════════════════════════════════════════════════════════
# Tests: Quality Check Engine — databricks/notebooks/quality
# ═══════════════════════════════════════════════════════════════════════════
# These tests validate quality check logic using pandas (simulating PySpark).
# In production, these run on Spark DataFrames; here we test the logic.
# ═══════════════════════════════════════════════════════════════════════════

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest


# ─── Quality Check Functions (Pandas equivalents of PySpark logic) ───────


def check_completeness(df: pd.DataFrame, column: str) -> float:
    """Calculate the completeness ratio of a column (non-null / total)."""
    if len(df) == 0:
        return 0.0
    return df[column].notna().sum() / len(df)


def check_uniqueness(df: pd.DataFrame, column: str) -> float:
    """Calculate the uniqueness ratio of a column (distinct / total)."""
    if len(df) == 0:
        return 0.0
    non_null = df[column].dropna()
    if len(non_null) == 0:
        return 0.0
    return non_null.nunique() / len(non_null)


def check_validity(df: pd.DataFrame, column: str, valid_values: set) -> float:
    """Calculate the validity ratio (values in valid set / total non-null)."""
    non_null = df[column].dropna()
    if len(non_null) == 0:
        return 0.0
    return non_null.isin(valid_values).sum() / len(non_null)


def check_positivity(df: pd.DataFrame, column: str) -> float:
    """Calculate the ratio of positive values."""
    non_null = df[column].dropna()
    if len(non_null) == 0:
        return 0.0
    return (non_null > 0).sum() / len(non_null)


def check_statistical_range(df: pd.DataFrame, column: str, num_stddev: int = 4) -> float:
    """Calculate the ratio of values within N standard deviations."""
    non_null = df[column].dropna()
    if len(non_null) < 2:
        return 1.0
    mean = non_null.mean()
    std = non_null.std()
    if std == 0:
        return 1.0
    within_range = ((non_null >= mean - num_stddev * std) & (non_null <= mean + num_stddev * std))
    return within_range.sum() / len(non_null)


def check_freshness_hours(df: pd.DataFrame, column: str) -> float:
    """Calculate hours since the most recent timestamp."""
    dates = pd.to_datetime(df[column], errors="coerce").dropna()
    if len(dates) == 0:
        return float("inf")
    most_recent = dates.max()
    hours = (pd.Timestamp.now() - most_recent).total_seconds() / 3600
    return hours


def check_volume(df: pd.DataFrame, min_records: int) -> bool:
    """Check if DataFrame has at least min_records rows."""
    return len(df) >= min_records


def evaluate_rule(actual_value: float, threshold: float, check_type: str) -> bool:
    """Evaluate whether a quality check passed."""
    if check_type == "freshness":
        return actual_value <= threshold  # Hours should be LESS than threshold
    elif check_type == "volume":
        return actual_value >= threshold
    else:
        return actual_value >= threshold


# ═══════════════════════════════════════════════════════════════════════════
# COMPLETENESS CHECK TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestCompletenessChecks:
    """Test completeness quality checks."""

    @pytest.mark.unit
    # CHANGE TO:
    def test_completeness_threshold_pass(self, sample_transactions_df):
        """Clean data should pass 99% completeness threshold."""
        score = check_completeness(sample_transactions_df, "transaction_id")
        assert evaluate_rule(score, 0.99, "completeness")

    
    @pytest.mark.unit
    def test_partially_complete_column(self, corrupted_transactions_df):
        """Column with nulls should return < 1.0."""
        score = check_completeness(corrupted_transactions_df, "transaction_id")
        assert 0.0 < score < 1.0

    @pytest.mark.unit
    def test_empty_dataframe_returns_zero(self, empty_df):
        """Empty DataFrame should return 0.0 completeness."""
        score = check_completeness(empty_df, "transaction_id")
        assert score == 0.0

    @pytest.mark.unit
    def test_all_null_column(self, sample_transactions_df):
        """All-null column should return 0.0."""
        df = sample_transactions_df.copy()
        df["transaction_id"] = None
        score = check_completeness(df, "transaction_id")
        assert score == 0.0

    @pytest.mark.unit
    def test_completeness_threshold_pass(self, sample_transactions_df):
        """Clean data should pass 99% completeness threshold."""
        score = check_completeness(sample_transactions_df, "transaction_id")
        assert evaluate_rule(score, 0.99, "completeness")

    @pytest.mark.unit
    def test_completeness_threshold_fail(self, corrupted_transactions_df):
        """Corrupted data may fail strict completeness threshold."""
        score = check_completeness(corrupted_transactions_df, "customer_id")
        # We injected 2 null customers out of 100 = 98%
        assert score < 1.0


# ═══════════════════════════════════════════════════════════════════════════
# UNIQUENESS CHECK TESTS
# ═══════════════════════════════════════════════════════════════════════════
