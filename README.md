# ETL data quality tests

A small ELT pipeline for an online shop (orders, customers, payments, FX rates) on DuckDB, and a pytest suite that
checks the data at every layer: completeness, uniqueness, validity, reconciliation with the source, referential
integrity, SCD2 history, incremental loads, late-arriving data, schema drift, freshness, and a GO / NO-GO
quality gate.

> Learning project, November 2026. The data is synthetic and generated deterministically. DuckDB stands in for a
> cloud warehouse (Redshift, Snowflake, BigQuery); the SQL sticks to CTEs, window functions and standard joins.

## Architecture

```text
 source batches (CSV / JSON lines)          tests
 ─────────────────────────────────          ─────────────────────────────────────────────
 batch_001/  customers.csv                   SourceReader (oracle): expected values from the
             orders.jsonl                    source files, in plain Python, no shared code
             payments.csv                                  │
             fx_rates.csv                                  ▼ expected
 batch_002/  ... updates, late orders,       WarehouseConnector: get_* / verify_*  ◄── actual
             duplicates, invalid rows                      │
        │                                                  ▼
        ▼                                    66 tests: raw · staging · marts · scenarios ·
 raw.*        as delivered + lineage                       quality gate · fault injection
        │
        ▼  validate, dedupe (ROW_NUMBER), watermark on updated_at
 staging.*    typed current state ──► audit.rejects (reason + payload)
        │
        ▼  FX conversion, SCD2 merge, point-in-time join
 marts.*      dim_customer (SCD2) · fct_orders · daily_revenue
        │
        ▼
 quality gate: GO / NO-GO with reasons ──► reports/quality_gate.{md,json}
```

## Coverage

| Area | Tests | What is checked |
|---|---|---|
| Raw completeness | DQ-RAW-001..010 | every file fully loaded per batch, lineage columns, invalid rows kept in raw |
| Staging validity | DQ-STG-001..006 | not null, accepted values, ranges, formats, final state equals the source |
| Uniqueness and dedup | DQ-STG-007..010 | one row per key, newest version wins, every delivered row accounted for |
| Quarantine | DQ-STG-011..016 | each broken rule sends exactly the offending rows to `audit.rejects` |
| Reconciliation | DQ-MRT-001..007 | EUR conversion, daily revenue per country, mart vs fact, orders vs payments |
| Referential integrity | DQ-MRT-008..011 | customer versions, FX rates, payments, published mart columns |
| SCD2 history | DQ-MRT-012..016 | one current version, no gaps or overlaps, full history, country at order time |
| Incremental loads | DQ-SCN-001..005 | idempotent re-runs, no roll-back, watermarks, late orders, later refunds |
| Schema drift | DQ-SCN-006..008 | renamed column stops the batch, new column logged, "12,50" amounts quarantined |
| Freshness | DQ-SCN-009..010 | fresh batch passes, 30-hour-old batch detected |
| Quality gate | DQ-GATE-001..004 | GO on clean data, NO-GO on rejects or stale data, report content |
| Fault injection | DQ-FLT-001..007 | each check fails when the defect it guards against is switched on |
| Framework | DQ-UNIT-001..003 | deterministic data, contract check, fault names validated |

Strategy, risks and limits: [docs/TEST_STRATEGY.md](docs/TEST_STRATEGY.md). Case registry: [tests/cases.json](tests/cases.json).

## Run

```sh
python -m venv .venv && .venv/Scripts/pip install -e ".[dev]"     # macOS/Linux: .venv/bin/pip

pytest -n auto                       # all 66 tests, about 15 s
pytest -m negative                   # only checks that must reject broken data
pytest -m fault                      # only the fault-injection proofs

python -m src.pipeline.run --data data/sample --warehouse warehouse.duckdb            # build a warehouse
python -m src.quality.gate --warehouse warehouse.duckdb --batch batch_002 --report reports
python -m src.pipeline.run --data data/sample --warehouse broken.duckdb --fault wrong_fx  # see a defect
```

## Layout

```text
src/datagen/        deterministic source batches (and drift / stale / invalid-row options)
src/pipeline/       contracts.py (source columns), pipeline.py (raw -> staging -> marts), run.py
src/connectors/     warehouse.py: get_* / verify_* used by tests and the gate
src/utils/          source_reader.py (oracle), custom_commands.py (numbered steps)
src/quality/        gate.py: GO / NO-GO and its report
fixtures/           data.py, warehouse.py (shared read-only warehouse, fresh one per scenario)
tests/              raw/ staging/ marts/ scenarios/ quality_gate/ faults/ unit/ + cases.json
docs/               TEST_STRATEGY.md
```

## How a failure reads

```text
Failed to compare SCD2 history of C0003 (country, from, to)!
Expected: [('FR', 1900-01-01, 2026-11-01 20:00), ('DE', 2026-11-01 20:00, None)]
Actual:   [('DE', 1900-01-01, None)]
```
