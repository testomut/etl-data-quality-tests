"""Typed shapes of the expected data, so a typo in a field name fails at construction, not in an assertion."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class CustomerVersion(BaseModel):
    customer_id: str
    email: str
    country: str
    segment: str
    valid_from: datetime
    valid_to: datetime | None = None


class ExpectedOrder(BaseModel):
    order_id: str
    customer_id: str
    status: str
    order_ts: datetime
    amount: Decimal
    currency: str
    amount_eur: Decimal
    country: str | None
    updated_at: datetime
