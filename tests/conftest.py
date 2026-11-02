
# ═══════════════════════════════════════════════════════════════════════════
# Shared Test Fixtures — Financial Data Platform
# ═══════════════════════════════════════════════════════════════════════════
# Provides reusable fixtures for all test modules:
#   - Sample DataFrames (clean, corrupted, edge cases)
#   - Mocked AWS services (S3 via moto)
#   - Temporary directories and files
#   - Configuration overrides
# ═══════════════════════════════════════════════════════════════════════════

import json
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import pytest
from moto import mock_aws


# ─── Constants ───────────────────────────────────────────────────────────

TEST_BUCKET = "financial-data-platform-test-landing"
TEST_REGION = "af-south-1"
SAMPLE_SIZE = 100
SEED = 42


# ─── Sample Data Fixtures ───────────────────────────────────────────────


@pytest.fixture
def sample_transactions_df():
    """Generate a clean sample transactions DataFrame (100 rows)."""
    np.random.seed(SEED)

    regions = {
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

    products = {
        "savings": ["Easy Access Saver", "Fixed Deposit", "Tax-Free Savings"],
        "investments": ["Unit Trust Fund", "Equity Portfolio", "Money Market"],
        "lending": ["Personal Loan", "Home Loan", "Vehicle Finance"],
        "insurance": ["Life Cover", "Short-Term Insurance", "Funeral Plan"],
        "payments": ["EFT Transfer", "Debit Order", "Card Payment"],
        "forex": ["USD Exchange", "EUR Exchange", "GBP Exchange"],
    }

    categories = list(products.keys())
    region_codes = list(regions.keys())
    channels = ["mobile", "online", "branch", "atm"]
    currencies = ["ZAR", "USD", "EUR", "GBP"]
    statuses = ["completed", "pending", "failed", "flagged"]
    tx_types = ["debit", "credit", "fee"]

    base_date = datetime(2026, 10, 1)
    records = []

    for i in range(SAMPLE_SIZE):
        cat = np.random.choice(categories)
        prod = np.random.choice(products[cat])
        rc = np.random.choice(region_codes)
        tx_date = base_date + timedelta(days=int(np.random.randint(0, 30)))

        records.append(
            {
                "transaction_id": f"TXN-{i:06d}",
                "customer_id": f"CUST-{np.random.randint(1, 30):04d}",
                "customer_name": f"Customer {np.random.randint(1, 30)}",
                "account_id": f"ACC-{np.random.randint(1, 50):05d}",
                "transaction_date": tx_date.strftime("%Y-%m-%d %H:%M:%S"),
                "transaction_amount": round(np.random.uniform(50, 500_000), 2),
                "currency": np.random.choice(currencies, p=[0.7, 0.1, 0.1, 0.1]),
                "transaction_type": np.random.choice(tx_types, p=[0.5, 0.35, 0.15]),
                "status": np.random.choice(statuses, p=[0.75, 0.10, 0.10, 0.05]),
                "product_category": cat,
                "product_name": prod,
                "channel": np.random.choice(channels),
                "region_code": rc,
                "region_name": regions[rc],
                "merchant_name": f"Merchant-{np.random.randint(1, 20)}",
                "is_weekend": tx_date.weekday() >= 5,
                "is_month_end": tx_date.day >= 28,
                "is_business_hours": 8 <= tx_date.hour <= 16,
            }
        )

    return pd.DataFrame(records)


@pytest.fixture
def corrupted_transactions_df(sample_transactions_df):
    """Generate a DataFrame with known defects injected."""
    df = sample_transactions_df.copy()

    # Inject specific known defects
    # 1. Null transaction IDs (rows 0-2)
    df.loc[0:2, "transaction_id"] = None

    # 2. Null amounts (rows 3-5)
    df.loc[3:5, "transaction_amount"] = None

    # 3. Negative amounts (rows 6-8)
    df.loc[6:8, "transaction_amount"] = -abs(df.loc[6:8, "transaction_amount"])

    # 4. Invalid currency (rows 9-11)
    df.loc[9:11, "currency"] = "INVALID"

    # 5. Future dates (rows 12-14)
    future = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
    df.loc[12:14, "transaction_date"] = future

    # 6. Duplicate records (rows 15-17 = copies of rows 0-2)
    df.loc[15:17, "transaction_id"] = df.loc[0:2, "transaction_id"].values

    # 7. Null customer (rows 18-19)
    df.loc[18:19, "customer_id"] = None
    df.loc[18:19, "customer_name"] = None

    # 8. Extreme amounts (row 20)
    df.loc[20, "transaction_amount"] = 999_999_999.99

    return df


@pytest.fixture
def empty_df():
    """Empty DataFrame with correct schema."""
    return pd.DataFrame(
        columns=[
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
        ]
    )


@pytest.fixture
def single_row_df():
    """DataFrame with exactly one row."""
    return pd.DataFrame(
        [
            {
                "transaction_id": "TXN-SINGLE",
                "customer_id": "CUST-0001",
                "customer_name": "Test Customer",
                "account_id": "ACC-00001",
                "transaction_date": "2026-10-15 10:30:00",
                "transaction_amount": 1500.00,
                "currency": "ZAR",
                "transaction_type": "debit",
                "status": "completed",
                "product_category": "savings",
                "product_name": "Easy Access Saver",
                "channel": "mobile",
                "region_code": "GP",
                "region_name": "Gauteng",
            }
        ]
    )


# ─── AWS Fixtures (Mocked with moto) ────────────────────────────────────


@pytest.fixture
def aws_credentials():
    """Set mock AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = TEST_REGION
    yield
    # Cleanup
    for key in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
    ]:
        os.environ.pop(key, None)


@pytest.fixture
def mock_s3(aws_credentials):
    """Create a mocked S3 service with test bucket."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=TEST_REGION)
        s3.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": TEST_REGION},
        )
        yield s3


@pytest.fixture
def mock_s3_with_data(mock_s3, sample_transactions_df, tmp_path):
    """Mocked S3 with a CSV file already uploaded."""
    csv_path = tmp_path / "transactions_20261001.csv"
    sample_transactions_df.to_csv(csv_path, index=False)

    mock_s3.upload_file(
        str(csv_path),
        TEST_BUCKET,
        "transactions/2026/10/transactions_20261001.csv",
    )
    return mock_s3


# ─── File System Fixtures ────────────────────────────────────────────────


@pytest.fixture
def tmp_data_dir(tmp_path):
    """Temporary directory for generated data files."""
    data_dir = tmp_path / "data" / "generated"
    data_dir.mkdir(parents=True)
    return data_dir


@pytest.fixture
def sample_csv_file(sample_transactions_df, tmp_data_dir):
    """Write sample data to a CSV file and return the path."""
    csv_path = tmp_data_dir / "transactions_20261001.csv"
    sample_transactions_df.to_csv(csv_path, index=False)
    return csv_path


# ─── Configuration Fixtures ─────────────────────────────────────────────


@pytest.fixture
def test_config():
    """Override configuration for testing."""
    return {
        "s3_bucket_landing": TEST_BUCKET,
        "s3_region": TEST_REGION,
        "num_records": SAMPLE_SIZE,
        "defect_rate": 0.15,
        "seed": SEED,
        "valid_currencies": ["ZAR", "USD", "EUR", "GBP"],
        "exchange_rates": {
            "ZAR": 1.0,
            "USD": 18.50,
            "EUR": 20.10,
            "GBP": 23.40,
        },
        "sa_provinces": {
            "GP": "Gauteng",
            "WC": "Western Cape",
            "KZN": "KwaZulu-Natal",
            "EC": "Eastern Cape",
            "FS": "Free State",
            "LP": "Limpopo",
            "MP": "Mpumalanga",
            "NW": "North West",
            "NC": "Northern Cape",
        },
    }


# ─── Utility Fixtures ───────────────────────────────────────────────────


@pytest.fixture
def freeze_time():
    """Provide a fixed datetime for deterministic tests."""
    return datetime(2026, 11, 2, 8, 0, 0)

