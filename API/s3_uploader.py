
"""
S3 Landing Zone Uploader (v2 — Production Refined)
=====================================================
Uploads generated transaction files and API data to S3 with:
  - Retry logic with exponential backoff
  - Multipart upload for large files (>50MB)
  - File deduplication (skip already-uploaded via checksum)
  - Dry-run mode for testing
  - Progress tracking
  - Content-Type detection
  - S3 tagging for cost allocation
  - Bucket validation on startup

Usage:
    python s3_uploader.py --mode file --source data/raw/transactions/ --bucket financial-platform-raw
    python s3_uploader.py --mode api --api-url http://localhost:8000 --bucket financial-platform-raw
    python s3_uploader.py --mode both --bucket financial-platform-raw --dry-run
"""

import os
import io
import json
import glob
import hashlib
import argparse
import time
from datetime import datetime
from typing import Optional

import boto3
import requests
import pandas as pd
from botocore.exceptions import ClientError

from config import settings, get_logger

# ─── Setup ───────────────────────────────────────────────────────────────────

logger = get_logger("s3_uploader")

CONTENT_TYPES = {
    ".csv": "text/csv",
    ".json": "application/json",
    ".parquet": "application/octet-stream",
    ".txt": "text/plain",
}

MULTIPART_THRESHOLD = 50 * 1024 * 1024  # 50 MB
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2  # seconds


# ─── Retry Decorator ────────────────────────────────────────────────────────

def retry_with_backoff(func):
    """Decorator that retries S3 operations with exponential backoff."""
    def wrapper(*args, **kwargs):
        last_exception = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except ClientError as e:
                last_exception = e
                error_code = e.response["Error"]["Code"]

                # Don't retry client errors (4xx) except throttling
                if error_code in ("NoSuchBucket", "AccessDenied", "InvalidBucketName"):
                    raise

                delay = RETRY_BASE_DELAY ** attempt
                logger.warning(f"  ⚠ Attempt {attempt}/{MAX_RETRIES} failed: {error_code}. "
                               f"Retrying in {delay}s...")
                time.sleep(delay)

        raise last_exception
    return wrapper


# ─── S3 Uploader ─────────────────────────────────────────────────────────────

class S3Uploader:
    """Production-grade S3 uploader with validation, retries, and deduplication."""

    def __init__(
        self,
        bucket_name: str,
        region: str = "af-south-1",
        dry_run: bool = False,
        tags: Optional[dict[str, str]] = None,
    ):
        self.bucket_name = bucket_name
        self.dry_run = dry_run
        self.s3_client = boto3.client("s3", region_name=region)
        self.tags = tags or {"project": "financial-data-platform", "environment": "dev"}
        self.uploaded_files: list[dict] = []
        self.skipped_files: list[dict] = []
        self._uploaded_checksums: set[str] = set()

    def validate_bucket(self) -> bool:
        """Verify the target bucket exists and is accessible."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"  ✓ Bucket validated: {self.bucket_name}")
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                logger.error(f"  ✗ Bucket does not exist: {self.bucket_name}")
            elif error_code == "403":
                logger.error(f"  ✗ Access denied to bucket: {self.bucket_name}")
            else:
                logger.error(f"  ✗ Bucket validation failed: {e}")
            return False

    @staticmethod
    def _calculate_checksum(data: bytes) -> str:
        """Calculate SHA-256 checksum for data integrity and deduplication."""
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def _detect_content_type(filepath: str) -> str:
        """Detect content type from file extension."""
        ext = os.path.splitext(filepath)[1].lower()
        return CONTENT_TYPES.get(ext, "application/octet-stream")

    def _build_tags(self, extra_tags: Optional[dict] = None) -> str:
        """Build S3 tag query string."""
        all_tags = {**self.tags, **(extra_tags or {})}
        return "&".join(f"{k}={v}" for k, v in all_tags.items())

    def _is_duplicate(self, checksum: str) -> bool:
        """Check if a file with this checksum was already uploaded in this session."""
        return checksum in self._uploaded_checksums

    @retry_with_backoff
    def _put_object(self, key: str, body: bytes, content_type: str, metadata: dict, tags: str):
        """Low-level S3 put with retry wrapper."""
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body,
            ContentType=content_type,
            Metadata=metadata,
            ServerSideEncryption="AES256",
            Tagging=tags,
        )

    @retry_with_backoff
    def _multipart_upload(self, key: str, filepath: str, content_type: str, metadata: dict, tags: str):
        """Multipart upload for large files."""
        from boto3.s3.transfer import TransferConfig

        config = TransferConfig(
            multipart_threshold=MULTIPART_THRESHOLD,
            multipart_chunksize=10 * 1024 * 1024,  # 10 MB chunks
        )

        extra_args = {
            "ContentType": content_type,
            "Metadata": metadata,
            "ServerSideEncryption": "AES256",
            "Tagging": tags,
        }

        self.s3_client.upload_file(
            filepath, self.bucket_name, key,
            ExtraArgs=extra_args,
            Config=config,
        )

    def upload_file(self, local_path: str, s3_key: str, metadata: Optional[dict] = None) -> dict:
        """Upload a single file to S3 with deduplication and integrity checks."""
        file_size = os.path.getsize(local_path)

        with open(local_path, "rb") as f:
            file_data = f.read()

        checksum = self._calculate_checksum(file_data)

        # Deduplication check
        if self._is_duplicate(checksum):
            skip_record = {
                "local_path": local_path,
                "s3_key": s3_key,
                "status": "skipped",
                "reason": "duplicate_checksum",
                "checksum": checksum,
            }
            self.skipped_files.append(skip_record)
            logger.info(f"  ⊘ Skipped (duplicate): {os.path.basename(local_path)}")
            return skip_record

        content_type = self._detect_content_type(local_path)
        s3_metadata = {
            "source": "file-upload",
            "upload_timestamp": datetime.now().isoformat(),
            "sha256_checksum": checksum,
            "original_filename": os.path.basename(local_path),
            "file_size_bytes": str(file_size),
            **(metadata or {}),
        }
        tags = self._build_tags({"source_type": "file"})

        if self.dry_run:
            logger.info(f"  [DRY RUN] Would upload: {local_path} → s3://{self.bucket_name}/{s3_key}")
            return {"local_path": local_path, "s3_key": s3_
