
"""
Financial Transaction Generator (v2 — Production Refined)
===========================================================
Generates realistic financial transaction data with:
  - Persistent customer pool (same customers across batches)
  - Product-channel correlation (mortgages don't happen at ATMs)
  - Weekday/weekend volume variation
  - Month-end spikes and seasonal patterns
  - Multi-currency with exchange rate conversion
  - Account-level tracking (one customer, multiple accounts)
  - Configurable, trackable data quality defects
  - Batch lineage via batch_id
  - Summary sidecar files for observability

Usage:
    python generate_transactions.py --num-records 5000 --output-dir data/raw/
    python generate_transactions.py --num-records 10000 --days-back 30 --seed 123
    python generate_transactions.py --num-records 5000 --defect-rate 0.08 --format parquet
"""

import os
import uuid
import json
import random
import argparse
import calendar
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

import pandas as pd
from faker import Faker

from config import (
    settings, get_logger, Product, Region, Customer, TransactionRecord,
    TransactionType, Channel, Status, Currency, DefectType,
    PRODUCTS, REGIONS, EXCHANGE_RATES, CURRENCY_WEIGHTS, STATUS_WEIGHTS,
    HOURLY_WEIGHTS, DAY_OF_WEEK_MULTIPLIERS, MONTH_END_MULTIPLIER,
)

# ─── Setup ───────────────────────────────────────────────────────────────────

logger = get_logger("generator")


# ─── Customer Pool ───────────────────────────────────────────────────────────

class CustomerPool:
    """
    Maintains a fixed pool of customers so the same customer_id appears
    across multiple transactions — just like a real financial institution.
    """

    def __init__(self, size: int, seed: int = 42):
        self.fake = Faker(["en_ZA", "en_US", "en_GB"])
        Faker.seed(seed)
        random.seed(seed)

        self.customers: list[Customer] = []
        region_codes = [r.code for r in REGIONS]
        region_weights = [r.weight for r in REGIONS]
        channels = [c.value for c in Channel]

        logger.info(f"Building customer pool of {size} customers...")

        for i in range(size):
            customer_id = f"CUST-{100000 + i}"
            region = random.choices(region_codes, weights=region_weights)[0]

            # Each customer has 1–3 accounts
            num_accounts = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
            account_ids = [f"ACC-{customer_id[-6:]}-{j:02d}" for j in range(num_accounts)]

            self.customers.append(Customer(
                customer_id=customer_id,
                customer_name=self.fake.name(),
                primary_region=region,
                preferred_channel=random.choice(channels),
                account_ids=account_ids,
                risk_score=round(random.betavariate(2, 8), 3),  # Skewed low (most are low risk)
            ))

        logger.info(f"  → Pool ready: {len(self.customers)} customers, "
                     f"{sum(len(c.account_ids) for c in self.customers)} accounts")

    def get_random_customer(self) -> Customer:
        """Select a random customer, weighted slightly toward active customers."""
        return random.choice(self.customers)


# ─── Defect Injector ────────────────────────────────────────────────────────

class DefectInjector:
    """
    Injects realistic data quality issues into a controlled percentage of records.
    Tracks defect distribution for observability.
    """

    def __init__(self, defect_rate: float = 0.05):
        self.defect_rate = defect_rate
        self.defect_counts: dict[str, int] = {d.value: 0 for d in DefectType}
        self.total_injected: int = 0

    def maybe_inject(self, record: dict) -> dict:
        """Inject a defect with probability = defect_rate."""
        if random.random() >= self.defect_rate:
            return record

        defect = random.choice(list(DefectType))
        self.total_injected += 1
        self.defect_counts[defect.value] += 1

        if defect == DefectType.MISSING_AMOUNT:
            record["transaction_amount"] = None
            record["amount_in_zar"] = None

        elif defect == DefectType.NEGATIVE_AMOUNT:
            record["transaction_amount"] = -abs(record["transaction_amount"])
            record["amount_in_zar"] = -abs(record["amount_in_zar"]) if record["amount_in_zar"] else None

        elif defect == DefectType.DUPLICATE_ID:
            record["transaction_id"] = "DUP-" + record["transaction_id"][:8]

        elif defect == DefectType.FUTURE_DATE:
            future = datetime.now() + timedelta(days=random.randint(30, 365))
            record["transaction_date"] = future.strftime("%Y-%m-%d %H:%M:%S")

        elif defect == DefectType.INVALID_CURRENCY:
            record["currency"] = random.choice(["XXX", "ABC", "", None])
            record["exchange_rate_to_zar"] = None
            record["amount_in_zar"] = None

        elif defect == DefectType.MISSING_CUSTOMER:
            record["customer_id"] = None
            record["customer_name"] = None
            record["account_id"] = None

        elif defect == DefectType.TRUNCATED_RECORD:
            record["product_name"] = record["product_name"][:3] if record["product_name"] else None
            record["region_name"] = None

        elif defect == DefectType.AMOUNT_OVERFLOW:
            record["transaction_amount"] = 99_999_999_999.99
            record["amount_in_zar"] = 99_999_999_999.99

        record["_defect_injected"] = defect.value
        return record

    def get_summary(self) -> dict:
        return {
            "total_injected": self.total_injected,
            "defect_rate_configured": self.defect_rate,
            "distribution": {k: v for k, v in self.defect_counts.items() if v > 0},
        }


# ─── Transaction Generator ──────────────────────────────────────────────────

class TransactionGenerator:
    """
    Core generator that produces realistic financial transactions
    with proper business logic correlations.
    """

    def __init__(
        self,
        customer_pool: CustomerPool,
        defect_rate: float = 0.05,
        seed: int = 42,
    ):
        self.customer_pool = customer_pool
        self.defect_injector = DefectInjector(defect_rate)
        self.fake = Faker(["en_ZA", "en_US", "en_GB"])
        Faker.seed(seed)
        random.seed(seed)

        self.product_weights = [p.weight for p in PRODUCTS]
        self.region_lookup = {r.code: r for r in REGIONS}

    def _select_product(self) -> Product:
        return random.choices(PRODUCTS, weights=self.product_weights)[0]

    def _select_channel(self, product: Product, customer: Customer) -> str:
        """Select a channel that is valid for this product, biased toward customer preference."""
        allowed = list(product.allowed_channels)
        weights = [2.0 if ch == customer.preferred_channel else 1.0 for ch in allowed]
        return random.choices(allowed, weights=weights)[0]

    def _select_transaction_type(self, product: Product) -> str:
        """Select a transaction type valid for this product."""
        return random.choice(list(product.allowed_types))

    def _generate_amount(self, product: Product) -> float:
        """Generate a realistic transaction amount with log-normal skew."""
        base = random.uniform(product.min_amount, product.max_amount)
        # Log-normal skew: most transactions cluster toward the lower end
        skewed = product.min_amount + (base - product.min_amount) * random.lognormvariate(0, 0.8) / 5
        amount = max(product.min_amount, min(skewed, product.max_amount))
        return float(Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    def _select_currency(self, region_code: str) -> tuple[str, float]:
        """Select currency with exchange rate. International regions use foreign currencies more."""
        if region_code == "INT":
            weights = [0.20, 0.40, 0.25, 0.15]  # More foreign currency for international
        else:
            weights = CURRENCY_WEIGHTS

        currencies = list(Currency)
        currency = random.choices(currencies, weights=weights)[0]
        rate = EXCHANGE_RATES[currency.value]
        return currency.value, rate

    def _generate_timestamp(self, target_date: datetime) -> datetime:
        """Generate a realistic timestamp with hourly distribution."""
        hour = random.choices(range(24), weights=HOURLY_WEIGHTS)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return target_date.replace(hour=hour, minute=minute, second=second, microsecond=0)

    def _is_month_end(self, date: datetime) -> bool:
        """Check if date is in the last 3 days of the month."""
        last_day = calendar.monthrange(date.year, date.month)[1]
        return date.day >= last_day - 2

    def _get_daily_volume(self, base_volume: int, target_date: datetime) -> int:
        """
        Adjust daily volume based on day-of-week and month-end patterns.
        Returns the adjusted number of records to generate.
        """
        multiplier = DAY_OF_WEEK_MULTIPLIERS[target_date.weekday()]

        if self._is_month_end(target_date):
            multiplier *= MONTH_END_MULTIPLIER

        # Add ±10% random noise
        noise = random.uniform(0.90, 1.10)
        adjusted = int(base_volume * multiplier * noise)

        return max(1, adjusted)

    def generate_single(self, target_date: datetime, batch_id: str) -> dict:
        """Generate a single realistic financial transaction."""
        customer = self.customer_pool.get_random_customer()
        product = self._select_product()
        region = self.region_lookup.get(customer.primary_region, REGIONS[0])
        channel = self._select_channel(product, customer)
        tx_type = self._select_transaction_type(product)
        amount = self._generate_amount(product)
        currency, exchange_rate = self._select_currency(region.code)
        amount_in_zar = float(Decimal(str(amount * exchange_rate)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        ))
        tx_datetime = self._generate_timestamp(target_date)
        is_month_end = self._is_month_end(target_date)
        is_weekend = target_date.weekday() >= 5

        # High-risk customers are more likely to be flagged
        status_weights = list(STATUS_WEIGHTS)
        if customer.risk_score > 0.7:
            status_weights[4] *= 3  # Triple the "flagged" weight

        record = TransactionRecord(
            transaction_id=str(uuid.uuid4()),
            batch_id=batch_id,
            customer_id=customer.customer_id,
            customer_name=customer.customer_name,
            account_id=random.choice(customer.account_ids),
            transaction_date=tx_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            transaction_type=tx_type,
            transaction_amount=amount,
            currency=currency,
            exchange_rate_to_zar=exchange_rate,
            amount_in_zar=amount_in_zar,
            product_name=product.name,
            product_category=product.category,
            region_code=region.code,
            region_name=region.name,
            channel=channel,
            status=random.choices([s.value for s in Status], weights=status_weights)[0],
            merchant_name=self.fake.company() if product.category in ("Cards", "Trading") else None,
            reference_number=f"REF-{random.randint(1_000_000, 9_999_999)}",
            description=self.fake.sentence(nb_words=6),
            is_weekend=is_weekend,
            is_month_end=is_month_end,
            created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ).to_dict()

        # Possibly inject a data quality defect
        record = self.defect_injector.maybe_inject(record)

        return record

    def generate_batch(self, num_records: int, target_date: datetime) -> tuple[pd.DataFrame, dict]:
        """
        Generate a full batch of transactions for a target date.
        Returns (DataFrame, batch_metadata).
        """
        batch_id = f"BATCH-{target_date.strftime('%Y%m%d')}-{uuid.uuid4().hex[:8]}"
        adjusted_volume = self._get_daily_volume(num_records, target_date)

        logger.info(
            f"Generating batch {batch_id}: {adjusted_volume} records for "
            f"{target_date.strftime('%Y-%m-%d')} "
            f"({'weekend' if target_date.weekday() >= 5 else 'weekday'}"
            f"{', month-end' if self._is_month_end(target_date) else ''})"
        )

        records = [self.generate_single(target_date, batch_id) for _ in range(adjusted_volume)]
        df = pd.DataFrame(records)

        # Batch metadata for sidecar file
        amounts = df["transaction_amount"].dropna()
        metadata = {
            "batch_id": batch_id,
            "target_date": target_date.strftime("%Y-%m-%d"),
            "generated_at": datetime.now().isoformat(),
            "record_count": len(df),
            "base_volume_requested": num_records,
            "adjusted_volume": adjusted_volume,
            "day_of_week": target_date.strftime("%A"),
            "is_weekend": target_date.weekday() >= 5,
            "is_month_end": self._is_month_end(target_date),
            "amount_stats": {
                "min": float(amounts.min()) if len(amounts) > 0 else None,
                "max": float(amounts.max()) if len(amounts) > 0 else None,
                "mean": round(float(amounts.mean()), 2) if len(amounts) > 0 else None,
                "median": round(float(amounts.median()), 2) if len(amounts) > 0 else None,
                "total": round(float(amounts.sum()), 2) if len(amounts) > 0 else None,
            },
            "status_distribution": df["status"].value_counts().to_dict(),
            "product_distribution": df["product_category"].value_counts().to_dict(),
            "region_distribution": df["region_code"].dropna().value_counts().to_dict(),
            "currency_distribution": df["currency"].dropna().value_counts().to_dict(),
            "channel_distribution": df["channel"].value_counts().to_dict(),
            "defect_summary": self.defect_injector.get_summary(),
        }

        logger.info(f"  → {len(df)} records | Amount range: {metadata['amount_stats']['min']}"
                     f" – {metadata['amount_stats']['max']} | "
                     f"Defects: {metadata['defect_summary']['total_injected']}")

        return df, metadata


# ─── File Output ─────────────────────────────────────────────────────────────

def save_batch(
    df: pd.DataFrame,
    metadata: dict,
    output_dir: str,
    target_date: datetime,
    file_format: str = "csv",
) -> tuple[str, str]:
    """
    Save batch data and metadata sidecar to disk with date-partitioned structure.
    Returns (data_filepath, metadata_filepath).
    """
    date_partition = target_date.strftime("year=%Y/month=%m/day=%d")
    partition_dir = os.path.join(output_dir, date_partition)
    os.makedirs(partition_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_id_short = metadata["batch_id"].split("-")[-1]

    # Save data file
    data_filename = f"transactions_{target_date.strftime('%Y%m%d')}_{batch_id_short}.{file_format}"
    data_filepath = os.path.join(partition_dir, data_filename)

    if file_format == "csv":
        df.to_csv(data_filepath, index=False)
    elif file_format == "json":
        df.to_json(data_filepath, orient="records", lines=True)
    elif file_format == "parquet":
        df.to_parquet(data_filepath, index=False)
    else:
        raise ValueError(f"Unsupported format: {file_format}")

    # Save metadata sidecar (always JSON)
    meta_filename = f"transactions_{target_date.strftime('%Y%m%d')}_{batch_id_short}_metadata.json"
    meta_filepath = os.path.join(partition_dir, meta_filename)
    with open(meta_filepath, "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    file_size_mb = os.path.getsize(data_filepath) / (1024 * 1024)
    logger.info(f"  → Data:     {data_filepath} ({file_size_mb:.2f} MB)")
    logger.info(f"  → Metadata: {meta_filepath}")

    return data_filepath, meta_filepath


# ─── Validation ──────────────────────────────────────────────────────────────

def validate_batch(df: pd.DataFrame) -> dict:
    """Run basic validation checks on generated data before output."""
    checks = {
        "total_records": len(df),
        "has_transaction_ids": df["transaction_id"].notna().all(),
        "has_batch_ids": df["batch_id"].notna().all(),
        "has_dates": df["transaction_date"].notna().all(),
        "has_channels": df["channel"].notna().all(),
        "has_statuses": df["status"].notna().all(),
        "unique_transaction_ids": df["transaction_id"].nunique(),
        "duplicate_ids": len(df) - df["transaction_id"].nunique(),
    }

    all_passed = all(v for k, v in checks.items() if isinstance(v, bool))
    checks["all_core_checks_passed"] = all_passed

    if not all_passed:
        logger.warning(f"  ⚠ Validation issues detected: {checks}")
    else:
        logger.info(f"  ✓ Validation passed ({checks['unique_transaction_ids']} unique IDs)")

    return checks


# ─── Main Entry Point ───────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate realistic financial transaction data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--num-records", type=int, default=5000, help="Base records per day (adjusted by patterns)")
    parser.add_argument("--days-back", type=int, default=1, help="Generate data for N days back")
    parser.add_argument("--output-dir", type=str, default=settings.OUTPUT_DIR, help="Output directory")
    parser.add_argument("--format", type=str, default="csv", choices=["csv", "json", "parquet"], help="Output format")
    parser.add_argument("--seed", type=int, default=settings.SEED, help="Random seed for reproducibility")
    parser.add_argument("--defect-rate", type=float, default=settings.DEFECT_RATE, help="Fraction of records with defects")
    parser.add_argument("--customer-pool-size", type=int, default=settings.CUSTOMER_POOL_SIZE, help="Number of unique customers")
    args = parser.parse_args()

    logger.info("=" * 65)
    logger.info("  Financial Transaction Generator v2")
    logger.info("=" * 65)
    logger.info(f"  Base records/day : {args.num_records}")
    logger.info(f"  Days to generate : {args.days_back}")
    logger.info(f"  Output directory : {args.output_dir}")
    logger.info(f"  Output format    : {args.format}")
    logger.info(f"  Defect rate      : {args.defect_rate:.1%}")
    logger.info(f"  Customer pool    : {args.customer_pool_size}")
    logger.info(f"  Seed             : {args.seed}")
    logger.info("=" * 65)

    # Initialize
    pool = CustomerPool(size=args.customer_pool_size, seed=args.seed)
    generator = TransactionGenerator(
        customer_pool=pool,
        defect_rate=args.defect_rate,
        seed=args.seed,
    )

    total_records = 0
    all_files: list[str] = []
    run_metadata: list[dict] = []

    for day_offset in range(args.days_back, 0, -1):
        target_date = datetime.now() - timedelta(days=day_offset)

        # Generate
        df, batch_meta = generator.generate_batch(args.num_records, target_date)

        # Validate
        validation = validate_batch(df)
        batch_meta["validation"] = validation

        # Save
        data_path, meta_path = save_batch(df, batch_meta, args.output_dir, target_date, args.format)

        total_records += len(df)
        all_files.append(data_path)
        run_metadata.append(batch_meta)

    # Save run-level summary
    run_summary = {
        "run_timestamp": datetime.now().isoformat(),
        "total_records": total_records,
        "total_files": len(all_files),
        "days_generated": args.days_back,
        "files": all_files,
        "defect_summary": generator.defect_injector.get_summary(),
    }
    summary_path = os.path.join(args.output_dir, "run_summary.json")
    with open(summary_path, "w") as f:
        json.dump(run_summary, f, indent=2, default=str)

    logger.info("=" * 65)
    logger.info(f"  COMPLETE: {total_records} records across {len(all_files)} files")
    logger.info(f"  Run summary: {summary_path}")
    logger.info("=" * 65)


if __name__ == "__main__":
    main()

