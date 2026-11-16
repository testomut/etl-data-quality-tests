"""The ELT pipeline under test: raw -> staging -> marts on DuckDB, with an audit schema.

    raw.*        every source row exactly as delivered, plus batch metadata (append-only)
    staging.*    typed, validated, deduplicated current state; invalid rows go to audit.rejects
    marts.*      dim_customer (SCD2 history), fct_orders (EUR amounts, customer version at order time),
                 daily_revenue (paid orders per day and country)
    audit.*      batch_runs, watermarks, rejects, schema_changes

Loads are incremental: each entity has a high watermark on `updated_at`, and a batch only processes rows newer
than it. That is what makes a re-run of the same batch a no-op (idempotency) and what lets a late-arriving order
(old `order_ts`, new `updated_at`) still reach the right day.

`faults` switches on known defects. They exist only so the test suite can prove that each check fails when the
defect it guards against is present (tests/faults). Production code would not carry them.

SQL is kept to CTEs, window functions and standard joins so it ports to Redshift / Snowflake / BigQuery with
small changes. DuckDB-only functions are limited to TRY_CAST and read helpers and are marked where used.
"""

from __future__ import annotations

import csv
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from src.pipeline.contracts import (
    ACCEPTED_CURRENCIES,
    ACCEPTED_STATUSES,
    REQUIRED_COLUMNS,
    TRACKED_CUSTOMER_ATTRIBUTES,
    SchemaDriftError,
    check_columns,
)

log = logging.getLogger(__name__)

KNOWN_FAULTS = {
    "skip_dedup",  # duplicates and stale versions reach staging
    "no_rejects",  # invalid rows are not quarantined
    "wrong_fx",  # amount divided by the rate instead of multiplied
    "refund_as_revenue",  # refunded orders counted as revenue
    "drop_late",  # incremental filter on order_ts instead of updated_at: late rows and corrections are lost
    "scd2_overwrite",  # customer changes overwrite history instead of opening a new version
    "non_idempotent",  # a re-run appends the same batch again
}

OPEN_START = "1900-01-01 00:00:00"

DDL = """
CREATE SCHEMA IF NOT EXISTS raw; CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts; CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS raw.customers (customer_id VARCHAR, email VARCHAR, full_name VARCHAR, country VARCHAR,
    segment VARCHAR, updated_at VARCHAR, _batch_id VARCHAR, _source_file VARCHAR, _loaded_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS raw.orders (order_id VARCHAR, customer_id VARCHAR, order_ts VARCHAR, status VARCHAR,
    amount VARCHAR, currency VARCHAR, updated_at VARCHAR, _batch_id VARCHAR, _source_file VARCHAR,
    _loaded_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS raw.payments (payment_id VARCHAR, order_id VARCHAR, amount VARCHAR, currency VARCHAR,
    method VARCHAR, paid_at VARCHAR, _batch_id VARCHAR, _source_file VARCHAR, _loaded_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS raw.fx_rates (rate_date VARCHAR, currency VARCHAR, rate_to_eur VARCHAR,
    _batch_id VARCHAR, _source_file VARCHAR, _loaded_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS staging.customers (customer_id VARCHAR, email VARCHAR, full_name VARCHAR,
    country VARCHAR, segment VARCHAR, updated_at TIMESTAMP, _batch_id VARCHAR);
CREATE TABLE IF NOT EXISTS staging.orders (order_id VARCHAR, customer_id VARCHAR, order_ts TIMESTAMP,
    status VARCHAR, amount DECIMAL(12, 2), currency VARCHAR, updated_at TIMESTAMP, _batch_id VARCHAR);
CREATE TABLE IF NOT EXISTS staging.payments (payment_id VARCHAR, order_id VARCHAR, amount DECIMAL(12, 2),
    currency VARCHAR, method VARCHAR, paid_at TIMESTAMP, _batch_id VARCHAR);
CREATE TABLE IF NOT EXISTS staging.fx_rates (rate_date DATE, currency VARCHAR, rate_to_eur DECIMAL(12, 6));

CREATE TABLE IF NOT EXISTS marts.dim_customer (customer_key VARCHAR, customer_id VARCHAR, email VARCHAR,
    full_name VARCHAR, country VARCHAR, segment VARCHAR, valid_from TIMESTAMP, valid_to TIMESTAMP,
    is_current BOOLEAN);
CREATE TABLE IF NOT EXISTS marts.fct_orders (order_id VARCHAR, customer_key VARCHAR, customer_id VARCHAR,
    country VARCHAR, order_ts TIMESTAMP, order_date DATE, status VARCHAR, amount DECIMAL(12, 2),
    currency VARCHAR, rate_to_eur DECIMAL(12, 6), amount_eur DECIMAL(14, 2));
CREATE TABLE IF NOT EXISTS marts.daily_revenue (order_date DATE, country VARCHAR, paid_orders INTEGER,
    revenue_eur DECIMAL(14, 2));

CREATE TABLE IF NOT EXISTS audit.batch_runs (batch_id VARCHAR, as_of TIMESTAMP, started_at TIMESTAMP,
    finished_at TIMESTAMP, status VARCHAR, raw_orders INTEGER, staged_orders INTEGER, rejected INTEGER,
    message VARCHAR);
CREATE TABLE IF NOT EXISTS audit.watermarks (entity VARCHAR, high_watermark TIMESTAMP);
CREATE TABLE IF NOT EXISTS audit.rejects (batch_id VARCHAR, entity VARCHAR, record_key VARCHAR, reason VARCHAR,
    payload VARCHAR);
CREATE TABLE IF NOT EXISTS audit.schema_changes (batch_id VARCHAR, entity VARCHAR, column_name VARCHAR,
    change VARCHAR);
"""


def _ts(col: str) -> str:
    """ISO-8601 text with a trailing Z -> TIMESTAMP (UTC)."""
    return f"CAST(REPLACE({col}, 'Z', '') AS TIMESTAMP)"


def _quote_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


class Pipeline:
    def __init__(self, db_path: Path | str, faults: set[str] | None = None) -> None:
        self.faults = set(faults or ())
        unknown = self.faults - KNOWN_FAULTS
        if unknown:
            raise ValueError(f"Unknown faults: {sorted(unknown)}")
        self.con = duckdb.connect(str(db_path))
        self.con.execute(DDL)

    def close(self) -> None:
        self.con.close()

    def _count(self, sql: str, params: list) -> int:
        row = self.con.execute(sql, params).fetchone()
        return int(row[0]) if row else 0

    # ------------------------------------------------------------------ batch
    def run_batch(self, folder: Path) -> str:
        """Load one source batch end to end. Returns the batch status ('success' or 'skipped')."""
        manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
        batch_id, as_of = manifest["batch_id"], manifest["as_of"].replace("Z", "")
        started = datetime.now(UTC).replace(tzinfo=None)

        already = self._count(
            "SELECT COUNT(*) FROM audit.batch_runs WHERE batch_id = ? AND status = 'success'", [batch_id])

        sources = {entity: self._read_source(folder, entity) for entity in REQUIRED_COLUMNS}
        drift = {e: check_columns(e, cols) for e, (cols, _) in sources.items()}
        missing = {e: m for e, (m, _) in drift.items() if m}
        if missing:
            self._record_run(batch_id, as_of, started, "failed", 0, 0, 0, f"schema drift: missing {missing}")
            raise SchemaDriftError(f"{batch_id}: required columns missing {missing}; nothing was loaded")

        self.con.execute("BEGIN TRANSACTION")
        try:
            for entity, (_, extra) in drift.items():
                for col in extra:
                    self.con.execute("INSERT INTO audit.schema_changes VALUES (?, ?, ?, 'added')",
                                     [batch_id, entity, col])
            if not already or "non_idempotent" in self.faults:
                for entity, (_, rows) in sources.items():
                    self._load_raw(entity, rows, batch_id, folder / self._file_name(entity))
            self._stage_customers(batch_id)
            self._stage_fx(batch_id)
            raw_orders, staged, rejected = self._stage_orders(batch_id)
            self._stage_payments(batch_id)
            self._build_marts()
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        status = "skipped" if already and "non_idempotent" not in self.faults else "success"
        self._record_run(batch_id, as_of, started, "success", raw_orders, staged, rejected, status)
        return status

    def run_all(self, data_dir: Path, batch_ids: list[str]) -> None:
        for batch_id in batch_ids:
            self.run_batch(data_dir / batch_id)

    # ------------------------------------------------------------------ raw
    @staticmethod
    def _file_name(entity: str) -> str:
        return f"{entity}.jsonl" if entity == "orders" else f"{entity}.csv"

    def _read_source(self, folder: Path, entity: str) -> tuple[list[str], list[dict]]:
        path = folder / self._file_name(entity)
        if entity == "orders":
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
            columns = list(rows[0]) if rows else list(REQUIRED_COLUMNS[entity])
            return columns, rows
        with path.open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader.fieldnames or []), list(reader)

    def _load_raw(self, entity: str, rows: list[dict], batch_id: str, source: Path) -> None:
        columns = REQUIRED_COLUMNS[entity]
        loaded_at = datetime.now(UTC).replace(tzinfo=None)

        def text(v: object) -> str | None:
            return None if v is None or v == "" else str(v)

        values = [[text(r.get(c)) for c in columns] + [batch_id, source.name, loaded_at] for r in rows]
        if values:
            placeholders = ", ".join(["?"] * (len(columns) + 3))
            self.con.executemany(f"INSERT INTO raw.{entity} VALUES ({placeholders})", values)

    # ------------------------------------------------------------------ watermarks
    def _watermark(self, entity: str) -> str:
        row = self.con.execute("SELECT high_watermark FROM audit.watermarks WHERE entity = ?", [entity]).fetchone()
        return str(row[0]) if row else OPEN_START

    def _advance_watermark(self, entity: str, staging_table: str, column: str = "updated_at") -> None:
        self.con.execute("DELETE FROM audit.watermarks WHERE entity = ?", [entity])
        self.con.execute(f"INSERT INTO audit.watermarks SELECT ?, MAX({column}) FROM staging.{staging_table}",
                         [entity])

    def _reject(self, batch_id: str, entity: str, key_col: str, select_sql: str, params: list) -> int:
        rows = self.con.execute(select_sql, params).fetchall()
        cols = [d[0] for d in self.con.description]
        for row in rows:
            rec = dict(zip(cols, row, strict=True))
            self.con.execute("INSERT INTO audit.rejects VALUES (?, ?, ?, ?, ?)",
                             [batch_id, entity, rec.get(key_col), rec["reason"], json.dumps(rec, default=str)])
        return len(rows)

    # ------------------------------------------------------------------ staging
    def _stage_customers(self, batch_id: str) -> None:
        wm = self._watermark("customers")
        candidates = f"""
            SELECT customer_id, email, full_name, country, segment, {_ts('updated_at')} AS updated_at,
                   CASE WHEN customer_id IS NULL THEN 'missing_customer_id'
                        WHEN email IS NULL OR POSITION('@' IN email) = 0 THEN 'invalid_email' END AS reason
            FROM raw.customers WHERE _batch_id = ? AND {_ts('updated_at')} > CAST(? AS TIMESTAMP)
        """
        if "no_rejects" not in self.faults:
            self._reject(batch_id, "customers", "customer_id",
                         f"SELECT * FROM ({candidates}) WHERE reason IS NOT NULL", [batch_id, wm])
        valid = self.con.execute(
            f"""
            WITH c AS ({candidates}),
            ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
                       FROM c {"" if "no_rejects" in self.faults else "WHERE reason IS NULL"})
            SELECT customer_id, email, full_name, country, segment, updated_at FROM ranked WHERE rn = 1
            ORDER BY customer_id
            """,
            [batch_id, wm],
        ).fetchall()
        for cid, email, name, country, segment, updated_at in valid:
            self.con.execute("DELETE FROM staging.customers WHERE customer_id = ?", [cid])
            self.con.execute("INSERT INTO staging.customers VALUES (?, ?, ?, ?, ?, ?, ?)",
                             [cid, email, name, country, segment, updated_at, batch_id])
            self._merge_dim_customer(cid, email, name, country, segment, updated_at)
        if valid:
            self._advance_watermark("customers", "customers")

    def _merge_dim_customer(self, cid: str, email: str, name: str, country: str, segment: str,
                            updated_at: datetime) -> None:
        current = self.con.execute(
            "SELECT customer_key, email, country, segment FROM marts.dim_customer "
            "WHERE customer_id = ? AND is_current", [cid]).fetchone()
        new = {"email": email, "country": country, "segment": segment}
        if current is None:
            self.con.execute(
                "INSERT INTO marts.dim_customer VALUES (?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP), NULL, TRUE)",
                [f"{cid}#1", cid, email, name, country, segment, OPEN_START])
            return
        key, *old_values = current
        old = dict(zip(TRACKED_CUSTOMER_ATTRIBUTES, old_values, strict=True))
        if old == new or "scd2_overwrite" in self.faults:
            self.con.execute(
                "UPDATE marts.dim_customer SET email = ?, full_name = ?, country = ?, segment = ? "
                "WHERE customer_key = ?", [email, name, country, segment, key])
            return
        version = int(key.split("#")[1]) + 1
        self.con.execute("UPDATE marts.dim_customer SET valid_to = ?, is_current = FALSE WHERE customer_key = ?",
                         [updated_at, key])
        self.con.execute("INSERT INTO marts.dim_customer VALUES (?, ?, ?, ?, ?, ?, ?, NULL, TRUE)",
                         [f"{cid}#{version}", cid, email, name, country, segment, updated_at])

    def _stage_fx(self, batch_id: str) -> None:
        self.con.execute(
            """DELETE FROM staging.fx_rates WHERE (rate_date, currency) IN (
                   SELECT CAST(rate_date AS DATE), currency FROM raw.fx_rates WHERE _batch_id = ?)""",
            [batch_id])
        self.con.execute(
            """INSERT INTO staging.fx_rates
               SELECT DISTINCT CAST(rate_date AS DATE), currency, CAST(rate_to_eur AS DECIMAL(12, 6))
               FROM raw.fx_rates WHERE _batch_id = ?""",
            [batch_id])

    def _stage_orders(self, batch_id: str) -> tuple[int, int, int]:
        # The non_idempotent fault forgets the watermark, so a re-run re-inserts rows that are already staged.
        wm = OPEN_START if "non_idempotent" in self.faults else self._watermark("orders")
        filter_col = "order_ts" if "drop_late" in self.faults else "updated_at"
        # TRY_CAST is DuckDB/Snowflake syntax; on Redshift the same check is a regex on the text value.
        candidates = f"""
            SELECT order_id, customer_id, {_ts('order_ts')} AS order_ts, status,
                   TRY_CAST(amount AS DECIMAL(12, 2)) AS amount, amount AS amount_text, currency,
                   {_ts('updated_at')} AS updated_at,
                   CASE WHEN customer_id IS NULL THEN 'missing_customer_id'
                        WHEN TRY_CAST(amount AS DECIMAL(12, 2)) IS NULL THEN 'amount_not_numeric'
                        WHEN TRY_CAST(amount AS DECIMAL(12, 2)) <= 0 THEN 'non_positive_amount'
                        WHEN currency NOT IN ({_quote_list(ACCEPTED_CURRENCIES)}) THEN 'unknown_currency'
                        WHEN status NOT IN ({_quote_list(ACCEPTED_STATUSES)}) THEN 'invalid_status'
                        WHEN customer_id NOT IN (SELECT customer_id FROM staging.customers)
                             THEN 'unknown_customer' END AS reason
            FROM raw.orders WHERE _batch_id = ? AND {_ts(filter_col)} > CAST(? AS TIMESTAMP)
        """
        raw_count = self._count(f"SELECT COUNT(*) FROM ({candidates})", [batch_id, wm])
        rejected = 0
        if "no_rejects" not in self.faults:
            rejected = self._reject(batch_id, "orders", "order_id",
                                    f"SELECT * FROM ({candidates}) WHERE reason IS NOT NULL", [batch_id, wm])
        valid_filter = "" if "no_rejects" in self.faults else "WHERE reason IS NULL"
        dedup = "" if "skip_dedup" in self.faults else "WHERE rn = 1"
        self.con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE incoming_orders AS
            WITH c AS ({candidates}),
            ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
                       FROM c {valid_filter})
            SELECT order_id, customer_id, order_ts, status, amount, currency, updated_at FROM ranked {dedup}
            """,
            [batch_id, wm],
        )
        if "skip_dedup" not in self.faults and "non_idempotent" not in self.faults:
            self.con.execute("DELETE FROM staging.orders WHERE order_id IN (SELECT order_id FROM incoming_orders)")
        self.con.execute("INSERT INTO staging.orders SELECT *, ? FROM incoming_orders", [batch_id])
        staged = self._count("SELECT COUNT(*) FROM incoming_orders", [])
        if staged:
            self._advance_watermark("orders", "orders")
        return raw_count, staged, rejected

    def _stage_payments(self, batch_id: str) -> None:
        self._reject(
            batch_id, "payments", "payment_id",
            """SELECT *, 'non_positive_amount' AS reason FROM raw.payments
               WHERE _batch_id = ? AND TRY_CAST(amount AS DECIMAL(12, 2)) <= 0""", [batch_id])
        self.con.execute(
            "DELETE FROM staging.payments WHERE payment_id IN "
            "(SELECT payment_id FROM raw.payments WHERE _batch_id = ?)",
            [batch_id])
        self.con.execute(
            f"""INSERT INTO staging.payments
                SELECT payment_id, order_id, CAST(amount AS DECIMAL(12, 2)), currency, method, {_ts('paid_at')}, ?
                FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _loaded_at DESC) AS rn
                      FROM raw.payments WHERE _batch_id = ? AND TRY_CAST(amount AS DECIMAL(12, 2)) > 0)
                WHERE rn = 1""",
            [batch_id, batch_id])

    # ------------------------------------------------------------------ marts
    def _build_marts(self) -> None:
        amount_eur = "o.amount / fx.rate_to_eur" if "wrong_fx" in self.faults else "o.amount * fx.rate_to_eur"
        revenue_statuses = "('paid', 'refunded')" if "refund_as_revenue" in self.faults else "('paid')"
        self.con.execute(
            f"""
            DELETE FROM marts.fct_orders;
            INSERT INTO marts.fct_orders
            SELECT o.order_id, d.customer_key, o.customer_id, d.country, o.order_ts, CAST(o.order_ts AS DATE),
                   o.status, o.amount, o.currency, fx.rate_to_eur, ROUND({amount_eur}, 2)
            FROM staging.orders o
            LEFT JOIN marts.dim_customer d
                   ON d.customer_id = o.customer_id
                  AND o.order_ts >= d.valid_from
                  AND (d.valid_to IS NULL OR o.order_ts < d.valid_to)
            LEFT JOIN staging.fx_rates fx
                   ON fx.rate_date = CAST(o.order_ts AS DATE) AND fx.currency = o.currency;

            DELETE FROM marts.daily_revenue;
            INSERT INTO marts.daily_revenue
            SELECT order_date, country, COUNT(*), SUM(amount_eur)
            FROM marts.fct_orders WHERE status IN {revenue_statuses}
            GROUP BY order_date, country;
            """
        )

    # ------------------------------------------------------------------ audit
    def _record_run(self, batch_id: str, as_of: str, started: datetime, status: str, raw_orders: int,
                    staged: int, rejected: int, message: str) -> None:
        self.con.execute(
            "INSERT INTO audit.batch_runs VALUES (?, CAST(? AS TIMESTAMP), ?, ?, ?, ?, ?, ?, ?)",
            [batch_id, as_of, started, datetime.now(UTC).replace(tzinfo=None), status, raw_orders, staged,
             rejected, message],
        )
