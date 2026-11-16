"""Proof that the checks catch what they claim to catch.

Each case switches on one known defect in the pipeline, runs the standard feed, and calls the same verify_*
method the regular test uses. The check must fail. A check that stays green against its own defect is useless,
and this file is where that would show.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from src.connectors.warehouse import WarehouseConnector
from src.utils.custom_commands import CustomCommands
from src.utils.source_reader import SourceReader


def _duplicates(wh: WarehouseConnector, source: SourceReader) -> None:
    wh.verify_no_duplicates("staging.orders", "order_id")


def _invalid_rows(wh: WarehouseConnector, source: SourceReader) -> None:
    wh.verify_no_nulls("staging.orders", ["customer_id", "amount"], key="order_id")


def _eur_amounts(wh: WarehouseConnector, source: SourceReader) -> None:
    wh.verify_fct_amounts(source.get_expected_orders())


def _revenue(wh: WarehouseConnector, source: SourceReader) -> None:
    wh.verify_daily_revenue(source.get_expected_daily_revenue())


def _staged_orders(wh: WarehouseConnector, source: SourceReader) -> None:
    wh.verify_staged_orders(source.get_expected_orders())


def _scd2_history(wh: WarehouseConnector, source: SourceReader) -> None:
    history = source.get_customer_versions()
    for customer_id in source.get_moved_customers():
        expected = [(v.country, v.valid_from, v.valid_to) for v in history[customer_id]]
        wh.verify_customer_versions(customer_id, expected)


FAULTS = [
    pytest.param("skip_dedup", _duplicates, "Duplicate order_id", marks=pytest.mark.case_id("DQ-FLT-001"),
                 id="no-dedup-caught-by-uniqueness"),
    pytest.param("no_rejects", _invalid_rows, "NULLs in required columns", marks=pytest.mark.case_id("DQ-FLT-002"),
                 id="no-quarantine-caught-by-not-null"),
    pytest.param("wrong_fx", _eur_amounts, "amount_eur", marks=pytest.mark.case_id("DQ-FLT-003"),
                 id="wrong-fx-caught-by-transformation-check"),
    pytest.param("refund_as_revenue", _revenue, "daily revenue", marks=pytest.mark.case_id("DQ-FLT-004"),
                 id="refunds-as-revenue-caught-by-reconciliation"),
    pytest.param("drop_late", _staged_orders, "staging.orders with the source",
                 marks=pytest.mark.case_id("DQ-FLT-005"),
                 id="lost-late-data-caught-by-source-reconciliation"),
    pytest.param("scd2_overwrite", _scd2_history, "SCD2 history", marks=pytest.mark.case_id("DQ-FLT-006"),
                 id="overwritten-history-caught-by-scd2-check"),
]


@pytest.mark.fault
@pytest.mark.negative
@pytest.mark.parametrize(("fault", "check", "message"), FAULTS)
def test_check_fails_when_its_defect_is_injected(make_pipeline: Callable, source_dir: Path, batches: list[str],
                                                 source: SourceReader, custom: CustomCommands, fault: str,
                                                 check: Callable, message: str) -> None:
    """DQ-FLT-001..006 Fault injection: the check fails on the defect it guards against"""
    pipeline, wh = make_pipeline(faults={fault})

    with custom.wrapped_test_step(f"1. Load the standard feed with the defect '{fault}' switched on"):
        pipeline.run_all(source_dir, batches)

    with custom.wrapped_test_step("2. The regular check fails and its message names the problem"):
        with pytest.raises(AssertionError, match=message):
            check(wh, source)


@pytest.mark.fault
@pytest.mark.negative
@pytest.mark.case_id("DQ-FLT-007")
def test_idempotency_check_fails_when_reruns_append(make_pipeline: Callable, source_dir: Path, batches: list[str],
                                                    custom: CustomCommands) -> None:
    """DQ-FLT-007 Fault injection: a pipeline that appends on re-run is caught by the snapshot comparison"""
    pipeline, wh = make_pipeline(faults={"non_idempotent"})

    with custom.wrapped_test_step("1. Load both batches, snapshot, and run batch 2 again"):
        pipeline.run_all(source_dir, batches)
        before = wh.get_snapshot()
        pipeline.run_batch(source_dir / batches[-1])

    with custom.wrapped_test_step("2. The snapshot differs and duplicates appear"):
        assert wh.get_snapshot() != before, "The re-run changed nothing, so the fault was not exercised"
        with pytest.raises(AssertionError, match="Duplicate order_id"):
            wh.verify_no_duplicates("staging.orders", "order_id")
