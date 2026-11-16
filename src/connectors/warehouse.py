"""Read access to the warehouse for tests: get_* return data, verify_* assert and explain the failure.

Every verify_* reports *which* rows differ, capped to a readable number, because "lists differ" on a table of
hundreds of rows sends the reader back to the database to find out what happened.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import duckdb

from src.types.models import ExpectedOrder

SHOW = 10  # how many differing rows a failure message lists


def _head(items: list) -> str:
    extra = f" ... and {len(items) - SHOW} more" if len(items) > SHOW else ""
    return f"{items[:SHOW]}{extra}"


class WarehouseConnector:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def _rows(self, sql: str, params: list | None = None) -> list[tuple]:
        return self.con.execute(sql, params or []).fetchall()

    def _scalar(self, sql: str, params: list | None = None):
        row = self.con.execute(sql, params or []).fetchone()
        assert row is not None, f"No row returned by: {sql}"
        return row[0]

    # ------------------------------------------------------------------ generic
    def get_row_count(self, table: str, where: str = "TRUE", params: list | None = None) -> int:
        return self._scalar(f"SELECT COUNT(*) FROM {table} WHERE {where}", params)

    def verify_row_count(self, table: str, expected: int, where: str = "TRUE", params: list | None = None) -> None:
        actual = self.get_row_count(table, where, params)
        assert actual == expected, (
            f"Failed to compare row count of {table} where {where} {params or ''}! "
            f"Expected: {expected}, Actual: {actual}")

    def get_columns(self, table: str) -> list[str]:
        schema, name = table.split(".")
        return [r[0] for r in self._rows(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position", [schema, name])]

    def verify_columns(self, table: str, expected: list[str]) -> None:
        actual = self.get_columns(table)
        assert actual == expected, f"Failed to compare columns of {table}! Expected: {expected}, Actual: {actual}"

    def get_duplicates(self, table: str, key: str) -> list[tuple]:
        return self._rows(f"SELECT {key}, COUNT(*) FROM {table} GROUP BY {key} HAVING COUNT(*) > 1 ORDER BY 1")

    def verify_no_duplicates(self, table: str, key: str) -> None:
        dups = self.get_duplicates(table, key)
        assert not dups, f"Duplicate {key} in {table}! Expected: none, Actual ({len(dups)}): {_head(dups)}"

    def get_null_rows(self, table: str, column: str, key: str) -> list:
        return [r[0] for r in self._rows(f"SELECT {key} FROM {table} WHERE {column} IS NULL ORDER BY 1")]

    def verify_no_nulls(self, table: str, columns: list[str], key: str) -> None:
        nulls = {c: self.get_null_rows(table, c, key) for c in columns}
        problems = {c: _head(v) for c, v in nulls.items() if v}
        assert not problems, f"NULLs in required columns of {table}! Expected: none, Actual: {problems}"

    def get_unexpected_values(self, table: str, column: str, accepted: tuple[str, ...]) -> list[tuple]:
        marks = ", ".join("?" * len(accepted))
        return self._rows(
            f"SELECT {column}, COUNT(*) FROM {table} WHERE {column} NOT IN ({marks}) OR {column} IS NULL "
            f"GROUP BY {column} ORDER BY 1", list(accepted))

    def verify_accepted_values(self, table: str, column: str, accepted: tuple[str, ...]) -> None:
        bad = self.get_unexpected_values(table, column, accepted)
        assert not bad, f"Unexpected {table}.{column} values! Accepted: {accepted}, Actual extra: {bad}"

    def get_orphans(self, child: str, child_key: str, parent: str, parent_key: str) -> list:
        return [r[0] for r in self._rows(
            f"SELECT c.{child_key} FROM {child} c LEFT JOIN {parent} p ON p.{parent_key} = c.{child_key} "
            f"WHERE p.{parent_key} IS NULL ORDER BY 1")]

    def verify_no_orphans(self, child: str, child_key: str, parent: str, parent_key: str) -> None:
        orphans = self.get_orphans(child, child_key, parent, parent_key)
        assert not orphans, (f"{child}.{child_key} without a match in {parent}.{parent_key}! "
                             f"Expected: none, Actual ({len(orphans)}): {_head(orphans)}")

    # ------------------------------------------------------------------ rejects
    def get_rejects(self, entity: str) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {}
        for key, reason in self._rows(
                "SELECT record_key, reason FROM audit.rejects WHERE entity = ? ORDER BY reason, record_key",
                [entity]):
            result.setdefault(reason, []).append(key)
        return result

    def verify_rejected(self, entity: str, reason: str, expected_keys: list[str]) -> None:
        actual = sorted(self.get_rejects(entity).get(reason, []))
        expected = sorted(expected_keys)
        assert actual == expected, (f"Failed to compare {entity} rejected as '{reason}'! "
                                    f"Expected: {expected}, Actual: {actual}")

    # ------------------------------------------------------------------ orders
    def get_staged_orders(self) -> dict[str, tuple]:
        return {r[0]: r[1:] for r in self._rows(
            "SELECT order_id, customer_id, status, amount, currency, order_ts FROM staging.orders")}

    def verify_staged_orders(self, expected: dict[str, ExpectedOrder]) -> None:
        actual = self.get_staged_orders()
        missing = sorted(set(expected) - set(actual))
        unexpected = sorted(set(actual) - set(expected))
        differ = sorted(oid for oid in set(expected) & set(actual)
                        if actual[oid][:4] != (expected[oid].customer_id, expected[oid].status,
                                               expected[oid].amount, expected[oid].currency))
        assert not (missing or unexpected or differ), (
            "Failed to compare staging.orders with the source! "
            f"Missing: {_head(missing)}; Unexpected: {_head(unexpected)}; "
            f"Different: {_head([(o, expected[o].status, actual[o][1]) for o in differ])}")

    def get_order_status(self, order_id: str) -> str | None:
        row = self._rows("SELECT status FROM staging.orders WHERE order_id = ?", [order_id])
        return row[0][0] if row else None

    def verify_order_status(self, order_id: str, expected: str) -> None:
        actual = self.get_order_status(order_id)
        assert actual == expected, (
            f"Failed to compare status of {order_id}! Expected: {expected}, Actual: {actual}")

    # ------------------------------------------------------------------ marts
    def get_fct_amounts(self) -> dict[str, tuple[Decimal | None, str | None]]:
        rows = self._rows("SELECT order_id, amount_eur, country FROM marts.fct_orders")
        return {r[0]: (r[1], r[2]) for r in rows}

    def verify_fct_amounts(self, expected: dict[str, ExpectedOrder]) -> None:
        actual = self.get_fct_amounts()
        differ = [(oid, str(e.amount_eur), str(actual.get(oid, (None,))[0])) for oid, e in sorted(expected.items())
                  if actual.get(oid, (None,))[0] != e.amount_eur]
        assert not differ, ("Failed to compare amount_eur (order, expected, actual)! "
                            f"{len(differ)} differ: {_head(differ)}")

    def verify_fct_countries(self, expected: dict[str, ExpectedOrder]) -> None:
        actual = self.get_fct_amounts()
        differ = [(oid, e.country, actual.get(oid, (None, None))[1]) for oid, e in sorted(expected.items())
                  if actual.get(oid, (None, None))[1] != e.country]
        assert not differ, ("Failed to compare country at order time (order, expected, actual)! "
                            f"{len(differ)} differ: {_head(differ)}")

    def get_daily_revenue(self) -> dict[tuple[str, str], tuple[int, Decimal]]:
        return {(str(d), c): (n, r) for d, c, n, r in self._rows(
            "SELECT order_date, country, paid_orders, revenue_eur FROM marts.daily_revenue")}

    def verify_daily_revenue(self, expected: dict[tuple[str, str], tuple[int, Decimal]]) -> None:
        actual = self.get_daily_revenue()
        keys = sorted(set(expected) | set(actual))
        differ = [(k, expected.get(k), actual.get(k)) for k in keys if expected.get(k) != actual.get(k)]
        assert not differ, ("Failed to compare daily revenue ((day, country), expected, actual)! "
                            f"{len(differ)} differ: {_head(differ)}")

    def get_revenue_for_day(self, day: str) -> Decimal:
        return self._scalar("SELECT COALESCE(SUM(revenue_eur), 0) FROM marts.daily_revenue WHERE order_date = ?",
                            [day])

    def get_payment_mismatches(self) -> list[tuple]:
        """Paid or refunded orders whose payment is missing or differs in amount or currency."""
        return self._rows(
            """
            WITH paid AS (SELECT order_id, amount, currency FROM staging.orders
                          WHERE status IN ('paid', 'refunded')),
            pay AS (SELECT order_id, COUNT(*) AS n, MIN(amount) AS amount, MIN(currency) AS currency
                    FROM staging.payments GROUP BY order_id)
            SELECT p.order_id, p.amount, p.currency, pay.n, pay.amount, pay.currency
            FROM paid p LEFT JOIN pay ON pay.order_id = p.order_id
            WHERE pay.n IS DISTINCT FROM 1 OR pay.amount <> p.amount OR pay.currency <> p.currency
            ORDER BY 1
            """)

    def verify_payments_match_orders(self) -> None:
        bad = self.get_payment_mismatches()
        assert not bad, ("Paid/refunded orders without exactly one matching payment "
                         f"(order, amount, currency, payments, paid amount, paid currency)! {_head(bad)}")

    def get_payments_for_unpaid_orders(self) -> list:
        return [r[0] for r in self._rows(
            """SELECT p.payment_id FROM staging.payments p JOIN staging.orders o ON o.order_id = p.order_id
               WHERE o.status IN ('created', 'cancelled') ORDER BY 1""")]

    # ------------------------------------------------------------------ SCD2
    def get_customer_versions(self, customer_id: str) -> list[tuple]:
        return self._rows(
            "SELECT country, valid_from, valid_to, is_current FROM marts.dim_customer WHERE customer_id = ? "
            "ORDER BY valid_from", [customer_id])

    def verify_customer_versions(self, customer_id: str,
                                 expected: list[tuple[str, datetime, datetime | None]]) -> None:
        actual = [(c, f, t) for c, f, t, _ in self.get_customer_versions(customer_id)]
        assert actual == expected, (f"Failed to compare SCD2 history of {customer_id} (country, from, to)! "
                                    f"Expected: {expected}, Actual: {actual}")

    def get_current_version_counts(self) -> list[tuple]:
        return self._rows(
            "SELECT customer_id, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS n FROM marts.dim_customer "
            "GROUP BY customer_id HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1 ORDER BY 1")

    def get_broken_intervals(self) -> list[tuple]:
        """Versions whose valid_to does not equal the next version's valid_from (gap or overlap)."""
        return self._rows(
            """
            WITH v AS (SELECT customer_id, customer_key, valid_from, valid_to,
                              LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) AS next_from
                       FROM marts.dim_customer)
            SELECT customer_key, valid_to, next_from FROM v
            WHERE (next_from IS NULL AND valid_to IS NOT NULL)
               OR (next_from IS NOT NULL AND (valid_to IS NULL OR valid_to <> next_from))
            ORDER BY 1
            """)

    # ------------------------------------------------------------------ audit
    def get_watermark(self, entity: str) -> datetime | None:
        row = self._rows("SELECT high_watermark FROM audit.watermarks WHERE entity = ?", [entity])
        return row[0][0] if row else None

    def verify_watermark(self, entity: str, expected: datetime) -> None:
        actual = self.get_watermark(entity)
        assert actual == expected, f"Failed to compare {entity} watermark! Expected: {expected}, Actual: {actual}"

    def get_last_run(self, batch_id: str) -> tuple | None:
        row = self._rows("SELECT status, message FROM audit.batch_runs WHERE batch_id = ? "
                         "ORDER BY finished_at DESC "
                         "LIMIT 1", [batch_id])
        return row[0] if row else None

    def get_freshness_lag_hours(self, batch_id: str) -> float:
        """Hours between the batch's as_of and the newest change it delivered (raw, so dropped rows count too)."""
        as_of = self._scalar("SELECT MAX(as_of) FROM audit.batch_runs WHERE batch_id = ?", [batch_id])
        newest = self._scalar(
            "SELECT MAX(CAST(REPLACE(updated_at, 'Z', '') AS TIMESTAMP)) FROM raw.orders WHERE _batch_id = ?",
            [batch_id])
        return (as_of - newest).total_seconds() / 3600

    def get_schema_changes(self, batch_id: str) -> list[tuple]:
        return self._rows("SELECT entity, column_name, change FROM audit.schema_changes WHERE batch_id = ? "
                          "ORDER BY 1, 2", [batch_id])

    def get_snapshot(self) -> dict[str, object]:
        """Counts and sums that must not move when an already loaded batch is processed again."""
        return {
            "raw.orders": self.get_row_count("raw.orders"),
            "staging.orders": self.get_row_count("staging.orders"),
            "staging.payments": self.get_row_count("staging.payments"),
            "marts.dim_customer": self.get_row_count("marts.dim_customer"),
            "audit.rejects": self.get_row_count("audit.rejects"),
            "revenue": self._scalar("SELECT SUM(revenue_eur) FROM marts.daily_revenue"),
        }
