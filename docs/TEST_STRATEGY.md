# Test strategy: order and revenue pipeline

Learning project, November 2026. The pipeline and the data are synthetic; the strategy is written the way I would
write it for a real data team.

## 1. What we protect and why

The marts feed a daily revenue report. The expensive failures are not crashes but numbers that look plausible
and are wrong: revenue counted twice, refunds counted as income, a customer's history rewritten, a late order
silently dropped. The strategy is ordered by that risk.

| Risk | Impact | Where it is caught |
|---|---|---|
| Rows lost between source and warehouse | Revenue under-reported, nobody notices | Raw completeness, staging row accounting, source reconciliation |
| Duplicates (redelivery, re-runs) | Revenue over-reported | Uniqueness, idempotency scenario |
| Wrong transformation (FX, rounding) | Every EUR figure wrong | Transformation check against an independent oracle |
| Wrong business rule (refunds, cancellations) | Revenue over-reported | Aggregate reconciliation, business-rule test |
| History overwritten (SCD2) | Published reports change retroactively | SCD2 interval and history checks, point-in-time join |
| Late or corrected data ignored | Past days never corrected | Late-arriving and correction scenarios, watermark checks |
| Source format change | NULLs or wrong numbers loaded silently | Schema drift and type drift scenarios |
| Stale feed | Report shows yesterday as today | Freshness check, quality gate |

## 2. Layers and what each one guarantees

| Layer | Guarantee | Test families |
|---|---|---|
| `raw` | Everything delivered is stored as delivered, with lineage | completeness per file and batch, lineage columns, unfiltered |
| `staging` | Typed, valid, one row per key, newest version wins; invalid rows quarantined with a reason | not null, accepted values, ranges, formats, uniqueness, dedup, row accounting, quarantine |
| `marts` | Report numbers equal the source, references resolve, history is kept | transformation, aggregate and cross-source reconciliation, referential integrity, SCD2, output contract |
| pipeline behaviour | Re-runs, late data, corrections and format changes are handled | incremental scenarios, schema drift, freshness |

## 3. How expected values are produced

Expected values come from `src/utils/source_reader.py`, which reads the source files in plain Python and applies
the rules from this document. It shares no code with the pipeline. Comparing warehouse SQL with the same SQL
would pass even when the rule itself is wrong.

## 4. Proving the tests work

`tests/faults` switches on one known defect at a time (no dedup, no quarantine, wrong FX, refunds as revenue,
lost late data, overwritten history, appending re-runs) and requires the regular check to fail. A check that
stays green against its own defect is removed or fixed.

## 5. Quality gate (release readiness)

After each batch, before marts are published, `src/quality/gate.py` produces a GO / NO-GO recommendation:

| Check | Limit | Reasoning |
|---|---|---|
| reject_rate | <= 5% of delivered order rows | a few bad rows are normal; more means a broken upstream |
| freshness_hours | <= 24 h between batch as_of and its newest change | a daily report must describe the previous day |
| duplicate_orders | 0 | any duplicate inflates revenue |
| orders_without_customer | 0 | such orders vanish from customer reports |
| unconverted_orders | 0 | no EUR amount means a missing FX rate |
| revenue_difference | 0 | the mart must equal the fact table to the cent |

The gate recommends; the team decides. A NO-GO names the failing check with its value and limit.

## 6. What is not covered

- Performance and volume: the data set is small on purpose.
- Real warehouse specifics (Redshift distribution and sort keys, Snowflake clustering): DuckDB stands in, and the
  SQL avoids engine-specific syntax except where marked.
- Orchestration (scheduling, retries, alerting).
- Security and access control of the warehouse.

## 7. Conventions

Tests follow my usual rules: numbered steps, `get_*` / `verify_*` connectors with messages that name the
differing rows, fixtures that clean up before creating, stable case IDs in `tests/cases.json`, polarity markers,
and no assertion edited just to make a test pass.
