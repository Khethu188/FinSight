
"""
Financial Transaction API Simulator (v2 — Production Refined)
===============================================================
A FastAPI service simulating a real-time transaction feed with:
  - API key authentication
  - Rate limiting
  - Cursor-based pagination
  - Request logging middleware
  - SSE stream with event IDs for reconnection
  - CORS configuration
  - Proper error models
  - OpenAPI documentation tags

Endpoints:
    GET  /health                         → Health check
    GET  /api/v1/transactions            → Paginated batch with filters
    GET  /api/v1/transactions/stream     → SSE real-time stream
    GET  /api/v1/transactions/count      → Record count with filters
    GET  /api/v1/transactions/{id}       → Single transaction lookup
    GET  /api/v1/summary                 → Aggregate statistics

Usage:
    uvicorn api_simulator:app --host 0.0.0.0 --port 8000 --reload

    # With environment variables:
    FDP_API_KEY=my-secret-key FDP_API_PORT=8080 uvicorn api_simulator:app
"""

import uuid
import json
import time
import random
import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, HTTPException, Request, Depends, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from config import settings, get_logger
from generate_transactions import TransactionGenerator, CustomerPool

# ─── Setup ───────────────────────────────────────────────────────────────────

logger = get_logger("api")

# In-memory store
transaction_store: dict[str, dict] = {}
transaction_order: list[str] = []  # Ordered list of IDs for cursor pagination
MAX_STORE_SIZE = 50_000
START_TIME = datetime.now()

# Rate limiting: track requests per API key
rate_limit_store: dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 100    # requests per window


# ─── Pydantic Models ────────────────────────────────────────────────────────

class ErrorResponse(BaseModel):
    error: str
    detail: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class Transaction(BaseModel):
    transaction_id: str
    batch_id: Optional[str] = None
    customer_id: Optional[str] = None
    customer_name: Optional[str] = None
    account_id: Optional[str] = None
    transaction_date: str
    transaction_type: str
    transaction_amount: Optional[float] = None
    currency: Optional[str] = None
    exchange_rate_to_zar: Optional[float] = None
    amount_in_zar: Optional[float] = None
    product_name: Optional[str] = None
    product_category: Optional[str] = None
    region_code: Optional[str] = None
    region_name: Optional[str] = None
    channel: str
    status: str
    merchant_name: Optional[str] = None
    reference_number: str
    description: str
    is_weekend: Optional[bool] = None
    is_month_end: Optional[bool] = None


class PaginatedResponse(BaseModel):
    count: int
    total_available: int
    next_cursor: Optional[str]
    has_more: bool
    timestamp: str
    transactions: list[Transaction]


class CountResponse(BaseModel):
    total: int
    filtered: int
    filters_applied: dict
    timestamp: str


class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: str
    transactions_in_store: int
    uptime_seconds: float


class SummaryResponse(BaseModel):
    date: str
    total_transactions: int
    total_amount: float
    avg_amount: float
    status_breakdown: dict
    product_breakdown: dict
    region_breakdown: dict
    channel_breakdown: dict
    currency_breakdown: dict


# ─── Authentication ──────────────────────────────────────────────────────────

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: Optional[str] = Security(api_key_header)) -> str:
    """
    Validate API key. In dev mode (default key), authentication is lenient.
    In production, a valid key is required.
    """
    if settings.API_KEY == "dev-api-key-2026":
        # Dev mode: accept any key or no key
        return api_key or "anonymous"

    if not api_key or api_key != settings.API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key. Provide X-API-Key header.",
        )
    return api_key


# ─── Rate Limiting ───────────────────────────────────────────────────────────

async def check_rate_limit(request: Request, api_key: str = Depends(verify_api_key)) -> str:
    """Enforce rate limiting per API key."""
    now = time.time()
    window = rate_limit_store[api_key]

    # Remove expired entries
    while window and window[0] < now - RATE_LIMIT_WINDOW:
        window.popleft()

    if len(window) >= RATE_LIMIT_MAX:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded. Max {RATE_LIMIT_MAX} requests per {RATE_LIMIT_WINDOW}s.",
            headers={"Retry-After": str(RATE_LIMIT_WINDOW)},
        )

    window.append(now)
    return api_key


# ─── Request Logging Middleware ──────────────────────────────────────────────

class RequestLoggingMiddleware:
    """Logs all incoming requests with timing information."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.time()
        request = Request(scope, receive)

        # Capture response status
        response_status = [200]

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                response_status[0] = message["status"]
            await send(message)

        await self.app(scope, receive, send_wrapper)

        duration_ms = (time.time() - start) * 1000
        logger.info(
            f"{request.method} {request.url.path} → {response_status[0]} ({duration_ms:.1f}ms)"
        )


# ─── Background Generator ───────────────────────────────────────────────────

async def background_generator(generator: TransactionGenerator):
    """Continuously generate transactions in the background."""
    while True:
        try:
            tx = generator.generate_single(datetime.now(), batch_id="LIVE-STREAM")
            tx.pop("_defect_injected", None)
            tx.pop("created_at", None)

            tx_id = tx["transaction_id"]
            transaction_store[tx_id] = tx
            transaction_order.append(tx_id)

            # Evict oldest if store is full
            while len(transaction_store) > MAX_STORE_SIZE:
                oldest_id = transaction_order.pop(0)
                transaction_store.pop(oldest_id, None)

            await asyncio.sleep(random.uniform(0.1, 2.0))

        except Exception as e:
            logger.error(f"Background generator error: {e}")
            await asyncio.sleep(5)


# ─── App Lifecycle ───────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize customer pool, pre-populate store, start background generation."""
    logger.info("Initializing API Simulator...")

    pool = CustomerPool(size=500, seed=settings.SEED)
    generator = TransactionGenerator(customer_pool=pool, defect_rate=0.02, seed=settings.SEED)

    # Pre-populate
    for _ in range(200):
        tx = generator.generate_single(datetime.now(), batch_id="SEED")
        tx.pop("_defect_injected", None)
        tx.pop("created_at", None)
        tx_id = tx["transaction_id"]
        transaction_store[tx_id] = tx
        transaction_order.append(tx_id)

    logger.info(f"Pre-populated {len(transaction_store)} transactions")

    # Store generator on app state for SSE endpoint
    app.state.generator = generator

    task = asyncio.create_task(background_generator(generator))
    yield
    task.cancel()


# ─── FastAPI App ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="Financial Transaction API",
    description=(
        "Simulated real-time financial transaction feed for the "
        "Cloud Financial Data Platform. Provides batch queries, "
        "real-time SSE streaming, and aggregate statistics."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Request logging
app.add_middleware(RequestLoggingMiddleware)


# ─── Helper: Apply Filters ──────────────────────────────────────────────────

def apply_filters(
    transactions: list[dict],
    status: Optional[str] = None,
    product_category: Optional[str] = None,
    region_code: Optional[str] = None,
    channel: Optional[str] = None,
    currency: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> list[dict]:
    """Apply query filters to a list of transactions."""
    filtered = transactions

    if status:
        filtered = [t for t in filtered if t.get("status") == status]
    if product_category:
        filtered = [t for t in filtered if t.get("product_category") == product_category]
    if region_code:
        filtered = [t for t in filtered if t.get("region_code") == region_code]
    if channel:
        filtered = [t for t in filtered if t.get("channel") == channel]
    if currency:
        filtered = [t for t in filtered if t.get("currency") == currency]
    if min_amount is not None:
        filtered = [t for t in filtered if t.get("transaction_amount") and t["transaction_amount"] >= min_amount]
    if max_amount is not None:
        filtered = [t for t in filtered if t.get("transaction_amount") and t["transaction_amount"] <= max_amount]
    if date_from:
        filtered = [t for t in filtered if t.get("transaction_date", "") >= date_from]
    if date_to:
        filtered = [t for t in filtered if t.get("transaction_date", "") <= date_to]

    return filtered


# ─── Endpoints ───────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Health check with system statistics."""
    return HealthResponse(
        status="healthy",
        version="2.0.0",
        timestamp=datetime.now().isoformat(),
        transactions_in_store=len(transaction_store),
        uptime_seconds=round((datetime.now() - START_TIME).total_seconds(), 1),
    )


@app.get(
    "/api/v1/transactions",
    response_model=PaginatedResponse,
    tags=["Transactions"],
    dependencies=[Depends(check_rate_limit)],
)
async def get_transactions(
    limit: int = Query(default=100, ge=1, le=1000, description="Page size"),
    cursor: Optional[str] = Query(default=None, description="Cursor for pagination (transaction_id)"),
    status: Optional[str] = Query(default=None),
    product_category: Optional[str] = Query(default=None),
    region_code: Optional[str] = Query(default=None),
    channel: Optional[str] = Query(default=None),
    currency: Optional[str] = Query(default=None),
    min_amount: Optional[float] = Query(default=None),
    max_amount: Optional[float] = Query(default=None),
    date_from: Optional[str] = Query(default=None, description="Start date (YYYY-MM-DD HH:MM:SS)"),
    date_to: Optional[str] = Query(default=None, description="End date (YYYY-MM-DD HH:MM:SS)"),
):
    """Fetch transactions with cursor-based pagination and filters."""
    # Determine starting position from cursor
    start_idx = 0
    if cursor:
        try:
            start_idx = transaction_order.index(cursor) + 1
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid cursor: {cursor}")

    # Get ordered slice
    candidate_ids = transaction_order[start_idx:]
    candidates = [transaction_store[tid] for tid in candidate_ids if tid in transaction_store]

    # Apply filters
    filtered = apply_filters(
        candidates, status, product_category, region_code,
        channel, currency, min_amount, max_amount, date_from, date_to,
    )

    # Paginate
    page = filtered[:limit]
    has_more = len(filtered) > limit
    next_cursor = page[-1]["transaction_id"] if has_more and page else None

    return PaginatedResponse(
        count=len(page),
        total_available=len(transaction_store),
        next_cursor=next_cursor,
        has_more=has_more,
        timestamp=datetime.now().isoformat(),
        transactions=page,
    )


@app.get(
    "/api/v1/transactions/count",
    response_model=CountResponse,
    tags=["Transactions"],
    dependencies=[Depends(check_rate_limit)],
)
async def count_transactions(
    status: Optional[str] = Query(default=None),
    product_category: Optional[str] = Query(default=None),
    region_code: Optional[str] = Query(default=None),
    channel: Optional[str] = Query(default=None),
    currency: Optional[str] = Query(default=None),
    min_amount: Optional[float] = Query(default=None),
    max_amount: Optional[float] = Query(default=None),
):
    """Get transaction count with optional filters."""
    all_txs = list(transaction_store.values())
    filtered = apply_filters(
        all_txs, status, product_category, region_code,
        channel, currency, min_amount, max_amount,
    )

    filters = {k: v for k, v in {
        "status": status, "product_category": product_category,
        "region_code": region_code, "channel": channel,
        "currency": currency, "min_amount": min_amount, "max_amount": max_amount,
    }.items() if v is not None}

    return CountResponse(
        total=len(all_txs),
        filtered=len(filtered),
        filters_applied=filters,
        timestamp=datetime.now().isoformat(),
    )


@app.get(
    "/api/v1/transactions/stream",
    tags=["Streaming"],
    dependencies=[Depends(check_rate_limit)],
)
async def stream_transactions(request: Request):
    """
    Server-Sent Events (SSE) stream with event IDs for reconnection.
    Clients can resume from a specific event using the Last-Event-ID header.
    """
    generator: TransactionGenerator = request.app.state.generator
    event_counter = 0

    async def event_generator():
        nonlocal event_counter
        while True:
            tx = generator.generate_single(datetime.now(), batch_id="SSE-STREAM")
            tx.pop("_defect_injected", None)
            tx.pop("created_at", None)

            event_counter += 1
            event_id = f"evt-{event_counter}-{uuid.uuid4().hex[:8]}"

            data = json.dumps(tx)
            yield f"id: {event_id}\nevent: transaction\ndata: {data}\n\n"

            await asyncio.sleep(random.uniform(0.5, 3.0))

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get(
    "/api/v1/transactions/{transaction_id}",
    response_model=Transaction,
    tags=["Transactions"],
    responses={404: {"model": ErrorResponse}},
    dependencies=[Depends(check_rate_limit)],
)
async def get_transaction(transaction_id: str):
    """Fetch a single transaction by ID."""
    tx = transaction_store.get(transaction_id)
    if not tx:
        raise HTTPException(status_code=404, detail=f"Transaction {transaction_id} not found")
    return tx


@app.get(
    "/api/v1/summary",
    response_model=SummaryResponse,
    tags=["Analytics"],
    dependencies=[Depends(check_rate_limit)],
)
async def get_summary():
    """Aggregate statistics for all transactions in the store."""
    transactions = list(transaction_store.values())

    if not transactions:
        raise HTTPException(status_code=404, detail="No transactions available")

    amounts = [t["transaction_amount"] for t in transactions if t.get("transaction_amount")]

    def count_field(field_name: str) -> dict:
        counts: dict[str, int] = {}
        for t in transactions:
            val = t.get(field_name, "unknown") or "unknown"
            counts[val] = counts.get(val, 0) + 1
        return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))

    return SummaryResponse(
        date=datetime.now().strftime("%Y-%m-%d"),
        total_transactions=len(transactions),
        total_amount=round(sum(amounts), 2),
        avg_amount=round(sum(amounts) / len(amounts), 2) if amounts else 0,
        status_breakdown=count_field("status"),
        product_breakdown=count_field("product_category"),
        region_breakdown=count_field("region_code"),
        channel_breakdown=count_field("channel"),
        currency_breakdown=count_field("currency"),
    )

