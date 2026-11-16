"""Release-readiness gate: turns warehouse metrics into a GO / NO-GO verdict with reasons.

Run after a batch, before the marts are published to reporting. Thresholds are the agreed acceptance criteria
(docs/TEST_STRATEGY.md, "Quality gate"); a check that fails names the metric, its value and the limit, so the
recommendation can be discussed with numbers rather than a red build.

    python -m src.quality.gate --warehouse warehouse.duckdb --batch batch_002 --report reports
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from src.connectors.warehouse import WarehouseConnector


@dataclass(frozen=True)
class Thresholds:
    max_reject_rate: float = 0.05  # rejected / delivered order rows of the batch
    max_freshness_hours: float = 24.0  # as_of minus the newest change in the batch
    max_duplicate_orders: int = 0
    max_orders_without_customer: int = 0
    max_unconverted_orders: int = 0  # orders without an FX rate, i.e. no EUR amount
    max_revenue_difference: float = 0.0  # daily_revenue total vs paid orders in fct_orders


@dataclass
class Check:
    name: str
    value: float
    limit: float
    passed: bool


@dataclass
class GateResult:
    batch_id: str
    verdict: str
    checks: list[Check]

    @property
    def failed(self) -> list[str]:
        return [c.name for c in self.checks if not c.passed]


def evaluate(con: duckdb.DuckDBPyConnection, batch_id: str, limits: Thresholds | None = None) -> GateResult:
    limits = limits or Thresholds()
    wh = WarehouseConnector(con)
    delivered = wh.get_row_count("raw.orders", "_batch_id = ?", [batch_id])
    rejected = wh.get_row_count("audit.rejects", "batch_id = ? AND entity = 'orders'", [batch_id])
    revenue_mart = wh._scalar("SELECT COALESCE(SUM(revenue_eur), 0) FROM marts.daily_revenue")
    revenue_fct = wh._scalar("SELECT COALESCE(SUM(amount_eur), 0) FROM marts.fct_orders WHERE status = 'paid'")
    values = [
        ("reject_rate", rejected / delivered if delivered else 0.0, limits.max_reject_rate),
        ("freshness_hours", wh.get_freshness_lag_hours(batch_id), limits.max_freshness_hours),
        ("duplicate_orders", len(wh.get_duplicates("staging.orders", "order_id")), limits.max_duplicate_orders),
        ("orders_without_customer", wh.get_row_count("marts.fct_orders", "customer_key IS NULL"),
         limits.max_orders_without_customer),
        ("unconverted_orders", wh.get_row_count("marts.fct_orders", "amount_eur IS NULL"),
         limits.max_unconverted_orders),
        ("revenue_difference", abs(float(revenue_mart) - float(revenue_fct)), limits.max_revenue_difference),
    ]
    checks = [Check(name, round(float(v), 4), float(limit), float(v) <= limit) for name, v, limit in values]
    verdict = "GO" if all(c.passed for c in checks) else "NO-GO"
    return GateResult(batch_id, verdict, checks)


def write_report(result: GateResult, folder: Path) -> tuple[Path, Path]:
    folder.mkdir(parents=True, exist_ok=True)
    json_path = folder / "quality_gate.json"
    json_path.write_text(json.dumps({"batch_id": result.batch_id, "verdict": result.verdict,
                                     "checks": [asdict(c) for c in result.checks]}, indent=2), encoding="utf-8")
    lines = [f"# Quality gate: {result.batch_id} - **{result.verdict}**", "",
             "| Check | Value | Limit | Result |", "|---|---|---|---|"]
    lines += [f"| {c.name} | {c.value} | {c.limit} | {'pass' if c.passed else 'FAIL'} |" for c in result.checks]
    if result.failed:
        lines += ["", f"Recommendation: do not publish; fix {', '.join(result.failed)} first."]
    md_path = folder / "quality_gate.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate the release-readiness gate for a batch")
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--batch", required=True)
    parser.add_argument("--report", default="reports")
    args = parser.parse_args()
    con = duckdb.connect(args.warehouse, read_only=True)
    result = evaluate(con, args.batch)
    write_report(result, Path(args.report))
    reasons = f" (failed: {', '.join(result.failed)})" if result.failed else ""
    print(f"{result.batch_id}: {result.verdict}{reasons}")
    return 0 if result.verdict == "GO" else 1


if __name__ == "__main__":
    raise SystemExit(main())
