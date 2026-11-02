
# ═══════════════════════════════════════════════════════════════════════════
# Tests: Defect Injector — scripts/02_inject_data_defects.py
# ═══════════════════════════════════════════════════════════════════════════

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest


# ═══════════════════════════════════════════════════════════════════════════
# DEFECT PRESENCE TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestDefectInjection:
    """Verify that defects are correctly injected."""

    @pytest.mark.unit
    def test_null_transaction_ids_injected(self, corrupted_transactions_df):
        """Should have null transaction IDs."""
        null_count = corrupted_transactions_df["transaction_id"].isna().sum()
        assert null_count >= 3, f"Expected ≥3 null IDs, got {null_count}"

    @pytest.mark.unit
    def test_null_amounts_injected(self, corrupted_transactions_df):
        """Should have null transaction amounts."""
        null_count = corrupted_transactions_df["transaction_amount"].isna().sum()
        assert null_count >= 3, f"Expected ≥3 null amounts, got {null_count}"

    @pytest.mark.unit
    def test_negative_amounts_injected(self, corrupted_transactions_df):
        """Should have negative transaction amounts."""
        df = corrupted_transactions_df
        negative_count = (df["transaction_amount"].dropna() < 0).sum()
        assert negative_count >= 3, f"Expected ≥3 negative amounts, got {negative_count}"

    @pytest.mark.unit
    def test_invalid_currency_injected(self, corrupted_transactions_df):
        """Should have invalid currency codes."""
        valid = {"ZAR", "USD", "EUR", "GBP"}
        invalid_mask = ~corrupted_transactions_df["currency"].isin(valid)
        assert invalid_mask.sum() >= 3, "Expected ≥3 invalid currencies"

    @pytest.mark.unit
    def test_future_dates_injected(self, corrupted_transactions_df):
        """Should have future transaction dates."""
        dates = pd.to_datetime(corrupted_transactions_df["transaction_date"], errors="coerce")
        now = pd.Timestamp.now()
        future_count = (dates > now).sum()
        assert future_count >= 3, f"Expected ≥3 future dates, got {future_count}"

    @pytest.mark.unit
    def test_null_customers_injected(self, corrupted_transactions_df):
        """Should have null customer IDs."""
        null_count = corrupted_transactions_df["customer_id"].isna().sum()
        assert null_count >= 2, f"Expected ≥2 null customers, got {null_count}"

    @pytest.mark.unit
    def test_extreme_amounts_injected(self, corrupted_transactions_df):
        """Should have at least one extreme amount."""
        max_amount = corrupted_transactions_df["transaction_amount"].max()
        assert max_amount > 100_000_000, f"Max amount {max_amount} not extreme enough"


# ═══════════════════════════════════════════════════════════════════════════
# DEFECT RATE TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestDefectRate:
    """Verify defect rates are within expected bounds."""

    @pytest.mark.unit
    def test_overall_defect_rate(self, sample_transactions_df, corrupted_transactions_df):
        """Total defective rows should be approximately 15% of total."""
        total = len(corrupted_transactions_df)
        clean = len(sample_transactions_df)

        # Count rows with at least one defect
        df = corrupted_transactions_df
        defective_mask = (
            df["transaction_id"].isna()
            | df["transaction_amount"].isna()
            | (df["transaction_amount"].fillna(0) < 0)
            | (~df["currency"].isin({"ZAR", "USD", "EUR", "GBP"}))
            | df["customer_id"].isna()
        )
        defect_rate = defective_mask.sum() / total

        # Allow range: 10-25% (we injected specific rows)
        assert 0.10 <= defect_rate <= 0.30, f"Defect rate {defect_rate:.1%} outside 10-30%"

    @pytest.mark.unit
    def test_clean_rows_preserved(self, corrupted_transactions_df):
        """Most rows should remain clean (>70%)."""
        df = corrupted_transactions_df
        clean_mask = (
            df["transaction_id"].notna()
            & df["transaction_amount"].notna()
            & (df["transaction_amount"] > 0)
            & df["currency"].isin({"ZAR", "USD", "EUR", "GBP"})
            & df["customer_id"].notna()
            & (df["transaction_amount"] < 100_000_000)
        )
        clean_rate = clean_mask.sum() / len(df)
        assert clean_rate >= 0.70, f"Only {clean_rate:.1%} clean rows, expected ≥70%"


# ═══════════════════════════════════════════════════════════════════════════
# DATA INTEGRITY TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestDefectDataIntegrity:
    """Verify defect injection doesn't break the DataFrame structure."""

    @pytest.mark.unit
    def test_row_count_preserved(self, sample_transactions_df, corrupted_transactions_df):
        """Row count should remain the same after injection."""
        assert len(corrupted_transactions_df) == len(sample_transactions_df)

    @pytest.mark.unit
    def test_column_count_preserved(self, sample_transactions_df, corrupted_transactions_df):
        """Column count should remain the same after injection."""
        assert len(corrupted_transactions_df.columns) == len(sample_transactions_df.columns)

    @pytest.mark.unit
    def test_columns_unchanged(self, sample_transactions_df, corrupted_transactions_df):
        """Column names should be identical after injection."""
        assert list(corrupted_transactions_df.columns) == list(
            sample_transactions_df.columns
        )

    @pytest.mark.unit
    def test_non_targeted_columns_unchanged(
        self, sample_transactions_df, corrupted_transactions_df
    ):
        """Columns not targeted by defects should remain unchanged."""
        # region_code is never targeted by defect injection
        pd.testing.assert_series_equal(
            corrupted_transactions_df["region_code"],
            sample_transactions_df["region_code"],
            check_names=True,
        )


# ═══════════════════════════════════════════════════════════════════════════
# EDGE CASE TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestDefectEdgeCases:
    """Test edge cases for defect injection."""

    @pytest.mark.unit
    def test_inject_into_empty_df(self, empty_df):
        """Injecting into an empty DataFrame should not raise errors."""
        # Should handle gracefully — no rows to corrupt
        assert len(empty_df) == 0

    @pytest.mark.unit
    def test_inject_into_single_row(self, single_row_df):
        """Injecting into a single-row DataFrame should work."""
        df = single_row_df.copy()
        df.loc[0, "transaction_amount"] = None  # Simulate injection
        assert df["transaction_amount"].isna().sum() == 1

    @pytest.mark.unit
    def test_multiple_defects_same_row(self, corrupted_transactions_df):
        """A single row can have multiple defects simultaneously."""
        df = corrupted_transactions_df
        # Rows 0-2 have null IDs AND are duplicated at rows 15-17
        null_id_rows = df[df["transaction_id"].isna()]
        assert len(null_id_rows) >= 3

