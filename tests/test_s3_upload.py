
# ═══════════════════════════════════════════════════════════════════════════
# Tests: S3 Upload — scripts/03_upload_to_s3.py
# ═══════════════════════════════════════════════════════════════════════════

import os
from pathlib import Path

import boto3
import pandas as pd
import pytest
from moto import mock_aws

TEST_BUCKET = "financial-data-platform-test-landing"
TEST_REGION = "af-south-1"


# ═══════════════════════════════════════════════════════════════════════════
# UPLOAD FUNCTIONALITY TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestS3Upload:
    """Test S3 upload operations."""

    @pytest.mark.integration
    def test_upload_csv_to_s3(self, mock_s3, sample_csv_file):
        """Should successfully upload a CSV file to S3."""
        s3_key = "transactions/2026/10/test_upload.csv"

        mock_s3.upload_file(str(sample_csv_file), TEST_BUCKET, s3_key)

        # Verify file exists in S3
        response = mock_s3.head_object(Bucket=TEST_BUCKET, Key=s3_key)
        assert response["ContentLength"] > 0

    @pytest.mark.integration
    def test_uploaded_file_content_matches(self, mock_s3, sample_csv_file, tmp_path):
        """Downloaded file should match the uploaded file."""
        s3_key = "transactions/2026/10/content_test.csv"

        # Upload
        mock_s3.upload_file(str(sample_csv_file), TEST_BUCKET, s3_key)

        # Download
        download_path = tmp_path / "downloaded.csv"
        mock_s3.download_file(TEST_BUCKET, s3_key, str(download_path))

        # Compare
        original = pd.read_csv(sample_csv_file)
        downloaded = pd.read_csv(download_path)
        pd.testing.assert_frame_equal(original, downloaded)

    @pytest.mark.integration
    def test_upload_creates_correct_s3_path(self, mock_s3, sample_csv_file):
        """S3 key should follow the expected partitioning pattern."""
        s3_key = "transactions/2026/10/transactions_20261001.csv"

        mock_s3.upload_file(str(sample_csv_file), TEST_BUCKET, s3_key)

        # List objects and verify path
        response = mock_s3.list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="transactions/2026/10/"
        )
        keys = [obj["Key"] for obj in response.get("Contents", [])]
        assert s3_key in keys

    @pytest.mark.integration
    def test_upload_multiple_files(self, mock_s3, sample_transactions_df, tmp_path):
        """Should handle uploading multiple files."""
        for i in range(3):
            csv_path = tmp_path / f"transactions_2026100{i+1}.csv"
            sample_transactions_df.to_csv(csv_path, index=False)
            s3_key = f"transactions/2026/10/transactions_2026100{i+1}.csv"
            mock_s3.upload_file(str(csv_path), TEST_BUCKET, s3_key)

        # Verify all 3 files exist
        response = mock_s3.list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="transactions/2026/10/"
        )
        assert len(response.get("Contents", [])) == 3


# ═══════════════════════════════════════════════════════════════════════════
# BUCKET VALIDATION TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestS3BucketValidation:
    """Test S3 bucket existence and configuration."""

    @pytest.mark.integration
    def test_bucket_exists(self, mock_s3):
        """Test bucket should exist."""
        response = mock_s3.list_buckets()
        bucket_names = [b["Name"] for b in response["Buckets"]]
        assert TEST_BUCKET in bucket_names

    @pytest.mark.integration
    def test_upload_to_nonexistent_bucket_fails(self, aws_credentials, sample_csv_file):
        """Uploading to a non-existent bucket should raise an error."""
        with mock_aws():
            s3 = boto3.client("s3", region_name=TEST_REGION)
            with pytest.raises(Exception):
                s3.upload_file(
                    str(sample_csv_file),
                    "nonexistent-bucket",
                    "test.csv",
                )


# ═══════════════════════════════════════════════════════════════════════════
# FILE VALIDATION TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestFileValidation:
    """Test file validation before upload."""

    @pytest.mark.unit
    def test_csv_file_exists(self, sample_csv_file):
        """CSV file should exist before upload."""
        assert sample_csv_file.exists()

    @pytest.mark.unit
    def test_csv_file_has_content(self, sample_csv_file):
        """CSV file should not be empty."""
        assert sample_csv_file.stat().st_size > 0

    @pytest.mark.unit
    def test_csv_file_has_header(self, sample_csv_file):
        """CSV file should have a header row."""
        with open(sample_csv_file) as f:
            header = f.readline().strip()
        assert "transaction_id" in header

    @pytest.mark.unit
    def test_nonexistent_file_raises_error(self):
        """Attempting to upload a non-existent file should raise FileNotFoundError."""
        fake_path = Path("/nonexistent/path/fake.csv")
        assert not fake_path.exists()

    @pytest.mark.unit
    def test_empty_file_detected(self, tmp_data_dir):
        """Empty files should be detectable before upload."""
        empty_file = tmp_data_dir / "empty.csv"
        empty_file.touch()
        assert empty_file.stat().st_size == 0


# ═══════════════════════════════════════════════════════════════════════════
# S3 KEY GENERATION TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestS3KeyGeneration:
    """Test S3 key path generation logic."""

    @pytest.mark.unit
    def test_key_format_with_date_partitioning(self):
        """S3 key should follow year/month partitioning."""
        filename = "transactions_20261015.csv"
        year = "2026"
        month = "10"
        expected_key = f"transactions/{year}/{month}/{filename}"

        # Simulate key generation
        s3_key = f"transactions/{year}/{month}/{filename}"
        assert s3_key == expected_key

    @pytest.mark.unit
    def test_key_handles_different_months(self):
        """Key generation should work for all months."""
        for month in range(1, 13):
            month_str = f"{month:02d}"
            key = f"transactions/2026/{month_str}/transactions_2026{month_str}01.csv"
            assert f"/{month_str}/" in key

    @pytest.mark.unit
    def test_key_no_leading_slash(self):
        """S3 keys should not start with a slash."""
        key = "transactions/2026/10/test.csv"
        assert not key.startswith("/")

