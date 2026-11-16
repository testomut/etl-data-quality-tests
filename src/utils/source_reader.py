"""Expected results, computed from the source files in plain Python.

This is the test oracle. It never reads the warehouse and shares no code with the pipeline: the rules below are
written from the requirements in docs/TEST_STRATEGY.md. A test then compares the warehouse (actual) with this
(expected). If both sides used the same SQL, a wrong rule would be wrong in both and every test would pass.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

from src.types.models import CustomerVersion, ExpectedOrder

VALID_STATUSES = {"created", "paid", "cancelled", "refunded"}
VALID_CURRENCIES = {"EUR", "USD", "GBP", "PLN"}
OPEN_START = datetime(1900, 1, 1)
CENT = Decimal("0.01")


def _ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


def _decimal(value: object) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


class SourceReader:
    def __init__(self, data_dir: Path, batch_ids: list[str]) -> None:
        self.data_dir = data_dir
        self.batch_ids = batch_ids

    # ------------------------------------------------------------------ files
    def get_manifest(self, batch_id: str) -> dict:
        return json.loads((self.data_dir / batch_id / "manifest.json").read_text(encoding="utf-8"))

    def get_rows(self, batch_id: str, entity: str) -> list[dict]:
        folder = self.data_dir / batch_id
        if entity == "orders":
            lines = (folder / "orders.jsonl").read_text(encoding="utf-8").splitlines()
            return [json.loads(line) for line in lines if line.strip()]
        with (folder / f"{entity}.csv").open(encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    def get_row_count(self, batch_id: str, entity: str) -> int:
        return len(self.get_rows(batch_id, entity))

    # ------------------------------------------------------------------ customers
    @staticmethod
    def _customer_is_valid(row: dict) -> bool:
        return bool(row.get("customer_id")) and "@" in (row.get("email") or "")

    def get_customer_versions(self) -> dict[str, list[CustomerVersion]]:
        """SCD2 history per customer: a new version whenever email, country or segment changes."""
        rows = [r for b in self.batch_ids for r in self.get_rows(b, "customers") if self._customer_is_valid(r)]
        rows.sort(key=lambda r: (r["customer_id"], _ts(r["updated_at"])))
        history: dict[str, list[CustomerVersion]] = defaultdict(list)
        for r in rows:
            versions = history[r["customer_id"]]
            tracked = (r["email"], r["country"], r["segment"])
            if versions and (versions[-1].email, versions[-1].country, versions[-1].segment) == tracked:
                continue
            if versions:
                versions[-1].valid_to = _ts(r["updated_at"])
            versions.append(CustomerVersion(
                customer_id=r["customer_id"], email=r["email"], country=r["country"], segment=r["segment"],
                valid_from=OPEN_START if not versions else _ts(r["updated_at"]), valid_to=None))
        return dict(history)

    def get_moved_customers(self) -> list[str]:
        return sorted(cid for cid, versions in self.get_customer_versions().items() if len(versions) > 1)

    def get_country_at(self, customer_id: str, moment: datetime) -> str | None:
        for v in self.get_customer_versions().get(customer_id, []):
            if v.valid_from <= moment and (v.valid_to is None or moment < v.valid_to):
                return v.country
        return None

    # ------------------------------------------------------------------ orders
    def get_reject_reason(self, row: dict, known_customers: set[str]) -> str | None:
        amount = _decimal(row.get("amount"))
        if not row.get("customer_id"):
            return "missing_customer_id"
        if amount is None:
            return "amount_not_numeric"
        if amount <= 0:
            return "non_positive_amount"
        if row.get("currency") not in VALID_CURRENCIES:
            return "unknown_currency"
        if row.get("status") not in VALID_STATUSES:
            return "invalid_status"
        if row["customer_id"] not in known_customers:
            return "unknown_customer"
        return None

    def get_expected_rejects(self) -> dict[str, list[str]]:
        """reason -> rejected order IDs (customers with an invalid email are reported under 'invalid_email')."""
        rejects: dict[str, list[str]] = defaultdict(list)
        known: set[str] = set()
        for b in self.batch_ids:
            for c in self.get_rows(b, "customers"):
                if self._customer_is_valid(c):
                    known.add(c["customer_id"])
                else:
                    rejects["invalid_email"].append(c["customer_id"])
            for o in self.get_rows(b, "orders"):
                reason = self.get_reject_reason(o, known)
                if reason:
                    rejects[reason].append(o["order_id"])
        return dict(rejects)

    def get_expected_orders(self) -> dict[str, ExpectedOrder]:
        """Final state per order: the valid version with the latest updated_at wins."""
        known: set[str] = set()
        latest: dict[str, dict] = {}
        for b in self.batch_ids:
            known |= {c["customer_id"] for c in self.get_rows(b, "customers") if self._customer_is_valid(c)}
            for o in self.get_rows(b, "orders"):
                if self.get_reject_reason(o, known):
                    continue
                current = latest.get(o["order_id"])
                if current is None or _ts(o["updated_at"]) >= _ts(current["updated_at"]):
                    latest[o["order_id"]] = o
        fx = self.get_fx_rates()
        expected = {}
        for oid, o in latest.items():
            order_ts = _ts(o["order_ts"])
            amount = Decimal(str(o["amount"]))
            rate = fx[(order_ts.date().isoformat(), o["currency"])]
            expected[oid] = ExpectedOrder(
                order_id=oid, customer_id=o["customer_id"], status=o["status"], order_ts=order_ts,
                amount=amount, currency=o["currency"], amount_eur=(amount * rate).quantize(CENT, ROUND_HALF_UP),
                country=self.get_country_at(o["customer_id"], order_ts), updated_at=_ts(o["updated_at"]))
        return expected

    def get_fx_rates(self) -> dict[tuple[str, str], Decimal]:
        return {(r["rate_date"], r["currency"]): Decimal(r["rate_to_eur"])
                for b in self.batch_ids for r in self.get_rows(b, "fx_rates")}

    def get_expected_daily_revenue(self) -> dict[tuple[str, str], tuple[int, Decimal]]:
        """(order_date, country) -> (paid orders, revenue in EUR). Only 'paid' counts as revenue."""
        totals: dict[tuple[str, str], list] = defaultdict(lambda: [0, Decimal("0")])
        for o in self.get_expected_orders().values():
            if o.status == "paid":
                key = (o.order_ts.date().isoformat(), o.country or "")
                totals[key][0] += 1
                totals[key][1] += o.amount_eur
        return {k: (v[0], v[1]) for k, v in totals.items()}

    def get_expected_watermark(self, entity: str) -> datetime:
        if entity == "customers":
            return max(_ts(c["updated_at"]) for b in self.batch_ids for c in self.get_rows(b, "customers")
                       if self._customer_is_valid(c))
        return max(o.updated_at for o in self.get_expected_orders().values())

    def get_late_orders(self) -> list[str]:
        """Orders delivered in a later batch for a day that an earlier batch already covered."""
        late = []
        for i, b in enumerate(self.batch_ids[1:], start=1):
            earlier_days = {_ts(o["order_ts"]).date() for prev in self.batch_ids[:i]
                            for o in self.get_rows(prev, "orders")}
            seen_before = {o["order_id"] for prev in self.batch_ids[:i] for o in self.get_rows(prev, "orders")}
            late += [o["order_id"] for o in self.get_rows(b, "orders")
                     if o["order_id"] not in seen_before and _ts(o["order_ts"]).date() in earlier_days
                     and not self.get_reject_reason(o, {o["customer_id"]})]
        return sorted(set(late))

    def get_redelivered_orders(self) -> list[str]:
        """Orders that appear more than once in a single batch file."""
        result = []
        for b in self.batch_ids:
            ids = [o["order_id"] for o in self.get_rows(b, "orders")]
            result += [oid for oid in set(ids) if ids.count(oid) > 1]
        return sorted(result)
