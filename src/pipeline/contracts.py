"""Source contracts: the columns each feed must deliver.

A missing or renamed column stops the load (`SchemaDriftError`) before anything is written, because silently
loading NULLs into a required column is the most expensive kind of data bug: every report downstream looks fine.
An extra column is not an error: it is recorded in `audit.schema_changes` for a human to review.
"""

from __future__ import annotations

REQUIRED_COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_id", "email", "full_name", "country", "segment", "updated_at"],
    "orders": ["order_id", "customer_id", "order_ts", "status", "amount", "currency", "updated_at"],
    "payments": ["payment_id", "order_id", "amount", "currency", "method", "paid_at"],
    "fx_rates": ["rate_date", "currency", "rate_to_eur"],
}

ACCEPTED_STATUSES = ("created", "paid", "cancelled", "refunded")
ACCEPTED_CURRENCIES = ("EUR", "USD", "GBP", "PLN")
TRACKED_CUSTOMER_ATTRIBUTES = ("email", "country", "segment")  # a change opens a new SCD2 version


class SchemaDriftError(Exception):
    """A source file does not match its contract; the batch is not loaded."""


def check_columns(entity: str, actual: list[str]) -> tuple[list[str], list[str]]:
    """Return (missing, extra) columns of a source file against its contract."""
    required = REQUIRED_COLUMNS[entity]
    missing = [c for c in required if c not in actual]
    extra = [c for c in actual if c not in required]
    return missing, extra
