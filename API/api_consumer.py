
"""
API Transaction Consumer
=========================
Consumes transactions from the API Simulator via batch pulls or SSE streaming.
Saves to local files for subsequent S3 upload, or directly to S3.

Usage:
    # Batch pull mode
    python api_consumer.py --mode batch --api-url http://localhost:8000 --records 5000

    # Stream mode (runs continuously, saves micro-batches)
    python api_consumer.py --mode stream --api-url http://localhost:8000 --batch-size 500

    # Direct to S3
    python api_consumer.py --mode batch --api-url http://localhost:8000 --records 5000 --to-s3 --bucket financial-platform-raw
"""

import os
import json
import time
import argparse
import logging
from datetime import datetime

import requests
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def consume_batch(api_url: str, num_records: int, output_dir: str) -> str:
    """Pull a batch of transactions from the API and save locally."""
    logger.info(f"Pulling {num_records} transactions from {api_url}...")

    all_transactions = []
    remaining = num_records

    while remaining > 0:
        batch_size = min(remaining, 1000)
        try:
            response = requests.get(
                f"{api_url}/api/v1/transactions",
                params={"limit": batch_size},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            batch = data.get("transactions", [])
            all_transactions.extend(batch)
            remaining -= len(batch)
            logger.info(f"  → Fetched {len(batch)} (total: {len(all_transactions)})")

            if len(batch) < batch_size:
                logger.info("  → No more records available from API")
                break

        except requests.RequestException as e:
            logger.error(f"  ✗ API request failed: {e}")
            time.sleep(5)

    if not all_transactions:
        logger.warning("No transactions received")
        return ""

    df = pd.DataFrame(all_transactions)

    # Save with date partition
    today = datetime.now()
    partition_dir = os.path.join(
        output_dir,
        f"source=api/year={today.strftime('%Y')}/month={today.strftime('%m')}/day={today.strftime('%d')}"
    )
    os.makedirs(partition_dir, exist_ok=True)

    filename = f"api_batch_{today.strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(partition_dir, filename)
    df.to_csv(filepath, index=False)

    logger.info(f"  ✓ Saved {len(df)} records to {filepath}")
    return filepath


def consume_stream(api_url: str, batch_size: int, output_dir: str):
    """
    Consume the SSE stream and save micro-batches.
    Runs continuously until interrupted.
    """
    logger.info(f"Connecting to SSE stream at {api_url}/api/v1/transactions/stream...")

    buffer = []
    batch_count = 0

    try:
        with requests.get(
            f"{api_url}/api/v1/transactions/stream",
            stream=True,
            timeout=None,
        ) as response:
            response.raise_for_status()

            for line in response.iter_lines(decode_unicode=True):
                if not line or not line.startswith("data: "):
                    continue

                try:
                    transaction = json.loads(line[6:])  # Strip "data: " prefix
                    buffer.append(transaction)

                    if len(buffer) % 50 == 0:
                        logger.info(f"  → Buffer: {len(buffer)}/{batch_size} transactions")

                    # Flush when batch is full
                    if len(buffer) >= batch_size:
                        batch_count += 1
                        df = pd.DataFrame(buffer)

                        today = datetime.now()
                        partition_dir = os.path.join(
                            output_dir,
                            f"source=stream/year={today.strftime('%Y')}/month={today.strftime('%m')}/day={today.strftime('%d')}"
                        )
                        os.makedirs(partition_dir, exist_ok=True)

                        filename = f"stream_batch_{batch_count:04d}_{today.strftime('%H%M%S')}.csv"
                        filepath = os.path.join(partition_dir, filename)
                        df.to_csv(filepath, index=False)

                        logger.info(f"  ✓ Flushed batch {batch_count}: {len(df)} records → {filepath}")
                        buffer = []

                except json.JSONDecodeError as e:
                    logger.warning(f"  ⚠ Skipping malformed event: {e}")

    except KeyboardInterrupt:
        logger.info("Stream interrupted by user")
        # Flush remaining buffer
        if buffer:
            df = pd.DataFrame(buffer)
            filepath = os.path.join(output_dir, f"stream_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            df.to_csv(filepath, index=False)
            logger.info(f"  ✓ Flushed final {len(df)} records → {filepath}")

    except requests.RequestException as e:
        logger.error(f"Stream connection error: {e}")


def main():
    parser = argparse.ArgumentParser(description="Consume transactions from API Simulator")
    parser.add_argument("--mode", choices=["batch", "stream"], default="batch", help="Consumption mode")
    parser.add_argument("--api-url", type=str, default="http://localhost:8000", help="API base URL")
    parser.add_argument("--records", type=int, default=5000, help="Records to pull (batch mode)")
    parser.add_argument("--batch-size", type=int, default=500, help="Micro-batch size (stream mode)")
    parser.add_argument("--output-dir", type=str, default="data/raw/transactions", help="Output directory")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info(f"API Consumer — Mode: {args.mode}")
    logger.info("=" * 60)

    if args.mode == "batch":
        consume_batch(args.api_url, args.records, args.output_dir)
    elif args.mode == "stream":
        consume_stream(args.api_url, args.batch_size, args.output_dir)


if __name__ == "__main__":
    main()

