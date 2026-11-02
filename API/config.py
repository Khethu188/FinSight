
"""
Shared Configuration Module
=============================
Single source of truth for all constants, models, and utilities
used across Phase 1 ingestion scripts.
"""

import os
import json
import logging
import random
from enum import Enum
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional


# ─── Environment-Aware Settings ─────────────────────────────────────────────

@dataclass(frozen=True)
class Settings:
    """Central settings loaded from environment variables with sensible defaults."""
    S3_BUCKET: str = os.getenv("FDP_S3_BUCKET", "financial-platform-raw")
    S3_REGION: str = os.getenv("FDP_AWS_REGION", "af-south-1")
    S3_LANDING_PREFIX: str = os.getenv("FDP_S3_LANDING_PREFIX", "landing/transactions")
    S3_MANIFEST_PREFIX: str = os.getenv("FDP_S3_MANIFEST_PREFIX", "landing/manifests")
    API_HOST: str = os.getenv("FDP_API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("FDP_API_PORT", "8000"))
    API_KEY: str = os.getenv("FDP_API_KEY", "dev-api-key-2026")
    LOG_LEVEL: str = os.getenv("FDP_LOG_LEVEL", "INFO")
    LOG_FORMAT: str = os.getenv("FDP_LOG_FORMAT", "text")  # "text" or "json"
    DEFECT_RATE: float = float(os.getenv("FDP_DEFECT_RATE", "0.05"))
    CUSTOMER_POOL_SIZE: int = int(os.getenv("FDP_CUSTOMER_POOL_SIZE", "2000"))
    SEED: int = int(os.getenv("FDP_SEED", "42"))
    OUTPUT_DIR: str = os.getenv("FDP_OUTPUT_DIR", "data/raw/transactions")


settings = Settings()


# ─── Structured Logging ─────────────────────────────────────────────────────

class JsonFormatter(logging.Formatter):
    """JSON log formatter for production environments (ELK/CloudWatch compatible)."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "extra_data"):
            log_entry["data"] = record.extra_data
        return json.dumps(log_entry)


def get_logger(name: str) -> logging.Logger:
    """Create a configured logger instance."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()

        if settings.LOG_FORMAT == "json":
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))

        logger.addHandler(handler)
        logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))

    return logger


# ─── Enums ───────────────────────────────────────────────────────────────────

class TransactionType(str, Enum):
    DEBIT = "debit"
    CREDIT = "credit"
    TRANSFER = "transfer"
    REVERSAL = "reversal"
    FEE = "fee"
    INTEREST = "interest"


class Channel(str, Enum):
    BRANCH = "branch"
    ONLINE = "online"
    MOBILE_APP = "mobile_app"
    ATM = "atm"
    POS_TERMINAL = "pos_terminal"
    API = "api"


class Status(str, Enum):
    COMPLETED = "completed"
    PENDING = "pending"
    FAILED = "failed"
    REVERSED = "reversed"
    FLAGGED = "flagged"


class Currency(str, Enum):
    ZAR = "ZAR"
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"


class DefectType(str, Enum):
    MISSING_AMOUNT = "missing_amount"
    NEGATIVE_AMOUNT = "negative_amount"
    DUPLICATE_ID = "duplicate_id"
    FUTURE_DATE = "future_date"
    INVALID_CURRENCY = "invalid_currency"
    MISSING_CUSTOMER = "missing_customer"
    TRUNCATED_RECORD = "truncated_record"
    AMOUNT_OVERFLOW = "amount_overflow"


# ─── Data Models ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Product:
    """Financial product definition with realistic constraints."""
    name: str
    category: str
    min_amount: float
    max_amount: float
    weight: float  # Selection probability weight
    allowed_channels: tuple[str, ...]  # Realistic channel restrictions
    allowed_types: tuple[str, ...]     # Realistic transaction type restrictions


@dataclass(frozen=True)
class Region:
    """Geographic region with transaction volume weight."""
    code: str
    name: str
    weight: float  # Proportion of total transaction volume


@dataclass
class Customer:
    """Persistent customer in the customer pool."""
    customer_id: str
    customer_name: str
    primary_region: str
    preferred_channel: str
    account_ids: list[str] = field(default_factory=list)
    risk_score: float = 0.0  # 0.0 (low risk) to 1.0 (high risk)


@dataclass
class TransactionRecord:
    """A single financial transaction with full metadata."""
    transaction_id: str
    batch_id: str
    customer_id: Optional[str]
    customer_name: Optional[str]
    account_id: Optional[str]
    transaction_date: str
    transaction_type: str
    transaction_amount: Optional[float]
    currency: Optional[str]
    exchange_rate_to_zar: Optional[float]
    amount_in_zar: Optional[float]
    product_name: Optional[str]
    product_category: Optional[str]
    region_code: Optional[str]
    region_name: Optional[str]
    channel: str
    status: str
    merchant_name: Optional[str]
    reference_number: str
    description: str
    is_weekend: bool
    is_month_end: bool
    created_at: str
    _defect_injected: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


# ─── Business Constants ─────────────────────────────────────────────────────

PRODUCTS: list[Product] = [
    Product("Personal Loan", "Lending", 1_000, 250_000, 3.0,
            ("branch", "online", "mobile_app"),
            ("debit", "credit", "fee", "interest")),
    Product("Home Mortgage", "Lending", 100_000, 5_000_000, 1.0,
            ("branch", "online"),
            ("debit", "credit", "fee", "interest")),
    Product("Vehicle Finance", "Lending", 50_000, 800_000, 2.0,
            ("branch", "online", "mobile_app"),
            ("debit", "credit", "fee", "interest")),
    Product("Credit Card", "Cards", 50, 50_000, 5.0,
            ("online", "mobile_app", "atm", "pos_terminal"),
            ("debit", "credit", "reversal", "fee")),
    Product("Savings Account", "Deposits", 100, 500_000, 4.0,
            ("branch", "online", "mobile_app", "atm"),
            ("debit", "credit", "transfer", "interest")),
    Product("Fixed Deposit", "Deposits", 10_000, 2_000_000, 2.0,
            ("branch", "online"),
            ("credit", "interest")),
    Product("Forex Trading", "Trading", 500, 1_000_000, 1.0,
            ("online", "mobile_app", "api"),
            ("debit", "credit", "transfer", "fee")),
    Product("Insurance Premium", "Insurance", 200, 15_000, 3.0,
            ("branch", "online", "mobile_app"),
            ("debit", "fee")),
    Product("Investment Fund", "Investments", 5_000, 3_000_000, 2.0,
            ("branch", "online", "mobile_app"),
            ("debit", "credit", "transfer", "fee")),
    Product("Business Loan", "Lending", 50_000, 10_000_000, 1.0,
            ("branch", "online", "api"),
            ("debit", "credit", "fee", "interest")),
]

REGIONS: list[Region] = [
    Region("GP", "Gauteng", 0.35),
    Region("WC", "Western Cape", 0.20),
    Region("KZN", "KwaZulu-Natal", 0.15),
    Region("EC", "Eastern Cape", 0.08),
    Region("FS", "Free State", 0.05),
    Region("LP", "Limpopo", 0.05),
    Region("MP", "Mpumalanga", 0.04),
    Region("NW", "North West", 0.04),
    Region("NC", "Northern Cape", 0.02),
    Region("INT", "International", 0.02),
]

# Exchange rates to ZAR (approximate, for realism)
EXCHANGE_RATES: dict[str, float] = {
    "ZAR": 1.00,
    "USD": 18.45,
    "EUR": 19.82,
    "GBP": 23.15,
}

CURRENCY_WEIGHTS: list[float] = [0.85, 0.08, 0.04, 0.03]
STATUS_WEIGHTS: list[float] = [0.82, 0.08, 0.04, 0.03, 0.03]

# Hourly transaction volume distribution (index = hour, value = relative weight)
HOURLY_WEIGHTS: list[int] = [
    1, 1, 1, 1, 1, 2, 3, 5, 8, 9, 9, 8,   # 00:00–11:00
    7, 7, 8, 8, 7, 6, 5, 4, 3, 2, 2, 1,   # 12:00–23:00
]

# Day-of-week volume multipliers (Mon=0 through Sun=6)
DAY_OF_WEEK_MULTIPLIERS: list[float] = [
    1.0,   # Monday
    1.05,  # Tuesday
    1.10,  # Wednesday (mid-week peak)
    1.05,  # Thursday
    1.15,  # Friday (payday activity)
    0.45,  # Saturday
    0.20,  # Sunday
]

# Month-end spike: last 3 days of month get a volume boost
MONTH_END_MULTIPLIER: float = 1.8

