
# ═══════════════════════════════════════════════════════════════════════════
# Tests: Data Generator — scripts/01_generate_transactions.py
# ═══════════════════════════════════════════════════════════════════════════

import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ─── Import the module under test ────────────────────────────────────────
# Adjust the import path based on your project structure
# from scripts.generate_transactions import TransactionGenerator


# ─── Helpers ─────────────────────────────────────────────────────────────

REQUIRED_COLUMNS = [
    "transaction_id",
    "customer_id",
    "customer_name",
    "account_id",
    "transaction_date",
    "transaction_amount",
    "currency",
    "transaction_type",
    "status",
    "product_category",
    "product_name",
    "channel",
    "region_code",
    "region_name",
    "merchant_name",
    "is_weekend",
    "is_month_end",
    "is_business_hours",
]

VALID_CURRENCIES = {"ZAR", "USD", "EUR", "GBP"}
VALID_STATUSES = {"completed", "pending", "failed", "flagged"}
VALID_TX_TYPES = {"debit", "credit", "fee"}
VALID_CHANNELS = {"mobile", "online", "branch", "atm"}
VALID_CATEGORIES = {"savings", "investments", "lending", "insurance", "payments", "forex"}
VALID_REGIONS = {"GP", "WC", "KZN", "EC", "FS", "LP", "MP", "NW", "NC"}


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMA TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestGeneratorSchema:
    """Verify the generated DataFrame has the correct structure."""

    @pytest.mark.unit
    def test_has_all_required_columns(self, sample_transactions_df):
        """All 18 required columns must be present."""
        for col in REQUIRED_COLUMNS:
            assert col in sample_transactions_df.columns, f"Missing column: {col}"

    @pytest.mark.unit
    def test_no_extra_columns(self, sample_transactions_df):
        """No unexpected columns should exist."""
        extra = set(sample_transactions_df.columns) - set(REQUIRED_COLUMNS)
        assert len(extra) == 0, f"Unexpected columns: {extra}"

    @pytest.mark.unit
    def test_correct_row_count(self, sample_transactions_df):
        """Should generate exactly the requested number of rows."""
        assert len(sample_transactions_df) == 100

    @pytest.mark.unit
    def test_column_dtypes(self, sample_transactions_df):
        """Key columns should have appropriate data types."""
        df = sample_transactions_df
        assert df["transaction_amount"].dtype in [np.float64, np.float32]
        assert df["is_weekend"].dtype == bool
        assert df["is_month_end"].dtype == bool
        assert df["is_business_hours"].dtype == bool


# ═══════════════════════════════════════════════════════════════════════════
# DATA VALIDITY TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestGeneratorDataValidity:
    """Verify generated data contains valid values."""

    @pytest.mark.unit
    def test_transaction_ids_unique(self, sample_transactions_df):
        """All transaction IDs should be unique."""
        assert sample_transactions_df["transaction_id"].is_unique

    @pytest.mark.unit
    def test_transaction_ids_format(self, sample_transactions_df):
        """Transaction IDs should follow TXN-XXXXXX format."""
        pattern = r"^TXN-\d{6}$"
        matches = sample_transactions_df["transaction_id"].str.match(pattern)
        assert matches.all(), "Some transaction IDs don't match expected format"

    @pytest.mark.unit
    def test_amounts_positive(self, sample_transactions_df):
        """All transaction amounts should be positive."""
        assert (sample_transactions_df["transaction_amount"] > 0).all()

    @pytest.mark.unit
    def test_amounts_reasonable_range(self, sample_transactions_df):
        """Amounts should be within a reasonable range (R50 — R500,000)."""
        amounts = sample_transactions_df["transaction_amount"]
        assert amounts.min() >= 50, f"Min amount {amounts.min()} below R50"
        assert amounts.max() <= 500_000, f"Max amount {amounts.max()} above R500,000"

    @pytest.mark.unit
    def test_currencies_valid(self, sample_transactions_df):
        """All currencies should be in the valid set."""
        invalid = set(sample_transactions_df["currency"].unique()) - VALID_CURRENCIES
        assert len(invalid) == 0, f"Invalid currencies found: {invalid}"

    @pytest.mark.unit
    def test_zar_is_dominant_currency(self, sample_transactions_df):
        """ZAR should be the most common currency (>50%)."""
        zar_pct = (sample_transactions_df["currency"] == "ZAR").mean()
        assert zar_pct > 0.50, f"ZAR only {zar_pct:.1%}, expected >50%"

    @pytest.mark.unit
    def test_statuses_valid(self, sample_transactions_df):
        """All statuses should be in the valid set."""
        invalid = set(sample_transactions_df["status"].unique()) - VALID_STATUSES
        assert len(invalid) == 0, f"Invalid statuses: {invalid}"

    @pytest.mark.unit
    def test_completed_is_dominant_status(self, sample_transactions_df):
        """Completed should be the most common status (>60%)."""
        completed_pct = (sample_transactions_df["status"] == "completed").mean()
        assert completed_pct > 0.60, f"Completed only {completed_pct:.1%}"

    @pytest.mark.unit
    def test_transaction_types_valid(self, sample_transactions_df):
        """All transaction types should be valid."""
        invalid = set(sample_transactions_df["transaction_type"].unique()) - VALID_TX_TYPES
        assert len(invalid) == 0, f"Invalid types: {invalid}"

    @pytest.mark.unit
    def test_channels_valid(self, sample_transactions_df):
        """All channels should be valid."""
        invalid = set(sample_transactions_df["channel"].unique()) - VALID_CHANNELS
        assert len(invalid) == 0, f"Invalid channels: {invalid}"

    @pytest.mark.unit
    def test_product_categories_valid(self, sample_transactions_df):
        """All product categories should be valid."""
        invalid = set(sample_transactions_df["product_category"].unique()) - VALID_CATEGORIES
        assert len(invalid) == 0, f"Invalid categories: {invalid}"

    @pytest.mark.unit
    def test_regions_valid(self, sample_transactions_df):
        """All region codes should be valid SA provinces."""
        invalid = set(sample_transactions_df["region_code"].unique()) - VALID_REGIONS
        assert len(invalid) == 0, f"Invalid regions: {invalid}"

    @pytest.mark.unit
    def test_region_code_name_consistency(self, sample_transactions_df):
        """Region codes and names should be consistent pairs."""
        expected = {
            "GP": "Gauteng",
            "WC": "Western Cape",
            "KZN": "KwaZulu-Natal",
            "EC": "Eastern Cape",
            "FS": "Free State",
            "LP": "Limpopo",
            "MP": "Mpumalanga",
            "NW": "North West",
            "NC": "Northern Cape",
        }
        for _, row in sample_transactions_df.iterrows():
            code = row["region_code"]
            name = row["region_name"]
            assert expected[code] == name, f"Mismatch: {code} → {name}"

    @pytest.mark.unit
    def test_dates_are_parseable(self, sample_transactions_df):
        """All transaction dates should be valid datetime strings."""
        dates = pd.to_datetime(sample_transactions_df["transaction_date"], errors="coerce")
        assert dates.notna().all(), "Some dates could not be parsed"

    @pytest.mark.unit
    def test_dates_not_in_future(self, sample_transactions_df):
        """No transaction dates should be in the future."""
        dates = pd.to_datetime(sample_transactions_df["transaction_date"])
        now = pd.Timestamp.now()
        future_count = (dates > now).sum()
        assert future_count == 0, f"{future_count} transactions have future dates"

    @pytest.mark.unit
    def test_no_null_values_in_clean_data(self, sample_transactions_df):
        """Clean generated data should have no nulls in critical fields."""
        critical_fields = [
            "transaction_id",
            "customer_id",
            "transaction_amount",
            "currency",
            "transaction_date",
        ]
        for field in critical_fields:
            null_count = sample_transactions_df[field].isna().sum()
            assert null_count == 0, f"{field} has {null_count} nulls"


# ═══════════════════════════════════════════════════════════════════════════
# REPRODUCIBILITY TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestGeneratorReproducibility:
    """Verify that the same seed produces identical output."""

    @pytest.mark.unit
    def test_same_seed_same_output(self):
        """Two generators with the same seed should produce identical data."""
        np.random.seed(42)
        data1 = np.random.uniform(50, 500_000, size=10)

        np.random.seed(42)
        data2 = np.random.uniform(50, 500_000, size=10)

        np.testing.assert_array_equal(data1, data2)

    @pytest.mark.unit
    def test_different_seed_different_output(self):
        """Different seeds should produce different data."""
        np.random.seed(42)
        data1 = np.random.uniform(50, 500_000, size=10)

        np.random.seed(99)
        data2 = np.random.uniform(50, 500_000, size=10)

        assert not np.array_equal(data1, data2)


# ═══════════════════════════════════════════════════════════════════════════
# FILE OUTPUT TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestGeneratorFileOutput:
    """Verify CSV file output."""

    @pytest.mark.unit
    def test_csv_file_created(self, sample_csv_file):
        """CSV file should be created at the expected path."""
        assert sample_csv_file.exists()

    @pytest.mark.unit
    def test_csv_file_not_empty(self, sample_csv_file):
        """CSV file should not be empty."""
        assert sample_csv_file.stat().st_size > 0

    @pytest.mark.unit
    def test_csv_readable(self, sample_csv_file):
        """CSV file should be readable back into a DataFrame."""
        df = pd.read_csv(sample_csv_file)
        assert len(df) == 100

    @pytest.mark.unit
    def test_csv_roundtrip_preserves_data(self, sample_transactions_df, tmp_data_dir):
        """Writing and reading CSV should preserve all data."""
        path = tmp_data_dir / "roundtrip_test.csv"
        sample_transactions_df.to_csv(path, index=False)
        df_read = pd.read_csv(path)

        assert list(df_read.columns) == list(sample_transactions_df.columns)
        assert len(df_read) == len(sample_transactions_df)


# ═══════════════════════════════════════════════════════════════════════════
# EDGE CASE TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestGeneratorEdgeCases:
    """Test boundary conditions and edge cases."""

    @pytest.mark.unit
    def test_single_record_generation(self, single_row_df):
        """Should handle generating a single record."""
        assert len(single_row_df) == 1
        assert single_row_df["transaction_id"].iloc[0] == "TXN-SINGLE"

    @pytest.mark.unit
    def test_empty_dataframe_has_correct_schema(self, empty_df):
        """Empty DataFrame should still have all required columns."""
        for col in ["transaction_id", "customer_id", "transaction_amount", "currency"]:
            assert col in empty_df.columns

    @pytest.mark.slow
    def test_large_dataset_generation(self):
        """Should handle generating 50,000 records without errors."""
        np.random.seed(42)
        large_data = {
            "transaction_id": [f"TXN-{i:06d}" for i in range(50_000)],
            "transaction_amount": np.random.uniform(50, 500_000, size=50_000),
        }
        df = pd.DataFrame(large_data)
        assert len(df) == 50_000
        assert df["transaction_id"].is_unique

