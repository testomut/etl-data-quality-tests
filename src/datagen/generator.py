"""Deterministic source data for the pipeline.

Two daily batches of an online shop: customers (CSV), orders (JSON lines), payments (CSV) and FX rates (CSV).
Batch 2 deliberately contains what real feeds contain: updates of existing rows, a customer moving country
(drives the SCD2 history), orders that arrive late for an earlier day, duplicates and invalid rows.

The generator is seeded, so every run and every xdist worker sees exactly the same data. Expected values in the
tests are computed from these files by `src.utils.source_reader`, never from the warehouse itself.
"""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

COUNTRIES = ["FR", "DE", "PL", "GB", "US", "ES"]
CURRENCIES = ["EUR", "USD", "GBP", "PLN"]
RATE_TO_EUR = {"EUR": 1.0, "USD": 0.92, "GBP": 1.16, "PLN": 0.23}
METHODS = ["card", "sepa", "paypal"]

CUSTOMER_COLUMNS = ["customer_id", "email", "full_name", "country", "segment", "updated_at"]
PAYMENT_COLUMNS = ["payment_id", "order_id", "amount", "currency", "method", "paid_at"]
FX_COLUMNS = ["rate_date", "currency", "rate_to_eur"]

BATCH_1 = "batch_001"
BATCH_2 = "batch_002"
AS_OF = {
    BATCH_1: datetime(2026, 11, 1, 6, 0, tzinfo=UTC),
    BATCH_2: datetime(2026, 11, 2, 6, 0, tzinfo=UTC),
}
FIRST_ORDER_DAY = date(2026, 10, 25)


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class GeneratorOptions:
    """Switches for scenario tests. Defaults produce the normal two-batch feed."""

    seed: int = 20261101
    drift: str | None = None  # renamed_column | extra_column | amount_as_text
    extra_invalid_orders: int = 0  # pushes the reject rate up, used by the quality-gate tests
    stale_hours: int = 0  # shifts every timestamp of batch 2 back, used by the freshness tests


@dataclass
class _State:
    customers: dict[str, dict] = field(default_factory=dict)
    orders: dict[str, dict] = field(default_factory=dict)


def _amount(rng: random.Random) -> float:
    return round(rng.uniform(5, 400), 2)


def _new_customer(rng: random.Random, n: int, ts: datetime) -> dict:
    return {
        "customer_id": f"C{n:04d}",
        "email": f"customer{n}@example.com",
        "full_name": f"Customer {n}",
        "country": rng.choice(COUNTRIES),
        "segment": rng.choice(["retail", "business"]),
        "updated_at": iso(ts),
    }


def _new_order(rng: random.Random, n: int, customer_id: str, order_ts: datetime, updated_at: datetime) -> dict:
    status = rng.choices(["paid", "created", "cancelled", "refunded"], weights=[70, 12, 10, 8])[0]
    return {
        "order_id": f"O{n:05d}",
        "customer_id": customer_id,
        "order_ts": iso(order_ts),
        "status": status,
        "amount": _amount(rng),
        "currency": rng.choice(CURRENCIES),
        "updated_at": iso(updated_at),
    }


def _payment_for(order: dict) -> dict:
    paid_at = datetime.strptime(order["order_ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC) + timedelta(minutes=3)
    return {
        "payment_id": "P" + order["order_id"][1:],
        "order_id": order["order_id"],
        "amount": order["amount"],
        "currency": order["currency"],
        "method": METHODS[int(order["order_id"][1:]) % len(METHODS)],
        "paid_at": iso(paid_at),
    }


def _fx_rows(days: list[date]) -> list[dict]:
    # A small daily drift keeps the conversion honest: a join on the wrong day gives a different amount.
    rows = []
    for i, d in enumerate(days):
        for cur, base in RATE_TO_EUR.items():
            rate = base if cur == "EUR" else round(base * (1 + 0.001 * i), 6)
            rows.append({"rate_date": d.isoformat(), "currency": cur, "rate_to_eur": rate})
    return rows


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def _write_batch(out: Path, batch_id: str, customers: list[dict], orders: list[dict], payments: list[dict],
                 fx: list[dict], opts: GeneratorOptions, as_of: datetime) -> None:
    folder = out / batch_id
    folder.mkdir(parents=True, exist_ok=True)
    customer_columns = list(CUSTOMER_COLUMNS)
    if opts.drift == "renamed_column" and batch_id == BATCH_2:
        customer_columns[customer_columns.index("country")] = "country_code"
        customers = [{**c, "country_code": c["country"]} for c in customers]
    if opts.drift == "extra_column" and batch_id == BATCH_2:
        customer_columns.append("loyalty_tier")
        customers = [{**c, "loyalty_tier": "gold"} for c in customers]
    if opts.drift == "amount_as_text" and batch_id == BATCH_2:
        # A European-formatted amount ("12,50") is what an upstream locale change typically produces.
        orders = [{**o, "amount": str(o["amount"]).replace(".", ",")} if i % 10 == 0 else o
                  for i, o in enumerate(orders)]
    _write_csv(folder / "customers.csv", customer_columns, customers)
    _write_jsonl(folder / "orders.jsonl", orders)
    _write_csv(folder / "payments.csv", PAYMENT_COLUMNS, payments)
    _write_csv(folder / "fx_rates.csv", FX_COLUMNS, fx)
    manifest = {
        "batch_id": batch_id,
        "as_of": iso(as_of),
        "row_counts": {"customers": len(customers), "orders": len(orders), "payments": len(payments),
                       "fx_rates": len(fx)},
    }
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def generate(out: Path, opts: GeneratorOptions | None = None) -> list[str]:
    """Write both batches under `out` and return their IDs in load order."""
    opts = opts or GeneratorOptions()
    rng = random.Random(opts.seed)
    state = _State()

    # Batch 1: 50 customers, 300 orders over 7 days.
    as_of_1 = AS_OF[BATCH_1]
    days_1 = [FIRST_ORDER_DAY + timedelta(days=i) for i in range(7)]
    for n in range(1, 51):
        c = _new_customer(rng, n, datetime(2026, 10, 20, 9, 0, tzinfo=UTC))
        state.customers[c["customer_id"]] = c
    customer_ids = list(state.customers)
    for n in range(1, 301):
        day = days_1[(n - 1) % len(days_1)]
        ts = datetime(day.year, day.month, day.day, rng.randint(0, 23), rng.randint(0, 59), tzinfo=UTC)
        o = _new_order(rng, n, rng.choice(customer_ids), ts, ts + timedelta(minutes=10))
        state.orders[o["order_id"]] = o
    orders_1 = list(state.orders.values())
    payments_1 = [_payment_for(o) for o in orders_1 if o["status"] in ("paid", "refunded")]
    _write_batch(out, BATCH_1, list(state.customers.values()), orders_1, payments_1, _fx_rows(days_1), opts,
                 as_of_1)

    # Batch 2: one more day of business, plus corrections of the past.
    shift = timedelta(hours=opts.stale_hours)
    as_of_2 = AS_OF[BATCH_2]
    day_2 = date(2026, 11, 1)
    upd = datetime(2026, 11, 1, 20, 0, tzinfo=UTC) - shift

    customers_2 = []
    for n in range(51, 61):  # 10 new customers
        c = _new_customer(rng, n, upd)
        state.customers[c["customer_id"]] = c
        customers_2.append(c)
    for cid in ["C0003", "C0011", "C0019", "C0027", "C0035"]:  # 5 customers move country -> SCD2 versions
        old = state.customers[cid]
        new_country = COUNTRIES[(COUNTRIES.index(old["country"]) + 1) % len(COUNTRIES)]
        changed = {**old, "country": new_country, "updated_at": iso(upd)}
        state.customers[cid] = changed
        customers_2.append(changed)
    customers_2.append({**_new_customer(rng, 61, upd), "email": "not-an-email"})  # invalid -> rejects

    orders_2: list[dict] = []
    all_ids = list(state.customers)
    for n in range(301, 381):  # 80 new orders of day 2
        ts = datetime(2026, 11, 1, rng.randint(0, 19), rng.randint(0, 59), tzinfo=UTC) - shift
        o = _new_order(rng, n, rng.choice(all_ids), ts, ts + timedelta(minutes=10))
        state.orders[o["order_id"]] = o
        orders_2.append(o)
    paid_1 = [o for o in orders_1 if o["status"] == "paid"]
    for o in paid_1[:20]:  # 20 refunds of earlier orders
        refunded = {**o, "status": "refunded", "updated_at": iso(upd)}
        state.orders[o["order_id"]] = refunded
        orders_2.append(refunded)
    for n in range(381, 384):  # 3 late-arriving orders that happened on 30.10 but reach us only now
        ts = datetime(2026, 10, 30, 22, 30 + n - 381, tzinfo=UTC)
        o = {**_new_order(rng, n, rng.choice(customer_ids), ts, upd), "status": "paid"}
        state.orders[o["order_id"]] = o
        orders_2.append(o)
    # The same refund delivered twice with an older updated_at: the newer version must win.
    stale_copy = {**orders_2[80], "status": "paid", "updated_at": iso(upd - timedelta(hours=5))}
    orders_2.append(stale_copy)
    orders_2.append(dict(orders_2[0]))  # exact duplicate of a new order
    invalid = [
        {**_new_order(rng, 901, "C0001", upd, upd), "customer_id": None},
        {**_new_order(rng, 902, "C0001", upd, upd), "amount": -15.0},
        {**_new_order(rng, 903, "C0001", upd, upd), "currency": "XXX"},
        _new_order(rng, 904, "C9999", upd, upd),  # orphan: the customer does not exist anywhere
    ]
    invalid += [{**_new_order(rng, 905 + i, "C0001", upd, upd), "amount": -1.0}
                for i in range(opts.extra_invalid_orders)]
    orders_2.extend(invalid)

    payments_2 = [_payment_for(o) for o in orders_2
                  if o.get("status") in ("paid", "refunded") and o["order_id"] in state.orders
                  and o["order_id"] >= "O00301" and o["order_id"] < "O00900"]
    payments_2 = list({p["payment_id"]: p for p in payments_2}.values())
    _write_batch(out, BATCH_2, customers_2, orders_2, payments_2, _fx_rows([day_2]), opts, as_of_2)
    return [BATCH_1, BATCH_2]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Write the sample source batches")
    parser.add_argument("--out", default="data/sample")
    print(generate(Path(parser.parse_args().out)))
