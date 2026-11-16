"""Scenarios: what happens when a source changes its format without telling anyone."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from src.pipeline.contracts import SchemaDriftError
from src.utils.custom_commands import CustomCommands
from src.utils.source_reader import SourceReader


@pytest.mark.negative
@pytest.mark.case_id("DQ-SCN-006")
def test_renamed_required_column_stops_the_batch(make_pipeline: Callable, make_source: Callable,
                                                 custom: CustomCommands) -> None:
    """DQ-SCN-006 Schema drift: a renamed required column fails the batch loudly and loads nothing"""
    data_dir, batches = make_source(drift="renamed_column")
    pipeline, wh = make_pipeline()
    pipeline.run_batch(data_dir / batches[0])
    before = wh.get_snapshot()

    with custom.wrapped_test_step("1. Batch 2 renames customers.country to country_code"):
        with pytest.raises(SchemaDriftError, match="country"):
            pipeline.run_batch(data_dir / batches[1])

    with custom.wrapped_test_step("2. Nothing of batch 2 reached the warehouse"):
        wh.verify_row_count("raw.orders", 0, "_batch_id = ?", [batches[1]])
        after = wh.get_snapshot()
        assert after == before, (
            f"Failed to compare warehouse after a failed batch! Expected: {before}, Actual: {after}")

    with custom.wrapped_test_step("3. The failure is recorded for the on-call engineer"):
        status, message = wh.get_last_run(batches[1])
        assert status == "failed" and "country" in message, (
            f"Failed to compare the run record! Expected: failed, mentions country, Actual: {status}, {message}")


@pytest.mark.positive
@pytest.mark.case_id("DQ-SCN-007")
def test_new_column_loads_and_is_recorded(make_pipeline: Callable, make_source: Callable,
                                          custom: CustomCommands) -> None:
    """DQ-SCN-007 Schema drift: an added column does not stop the load and is logged for review"""
    data_dir, batches = make_source(drift="extra_column")
    pipeline, wh = make_pipeline()

    with custom.wrapped_test_step("1. Load both batches; batch 2 adds customers.loyalty_tier"):
        pipeline.run_all(data_dir, batches)

    with custom.wrapped_test_step("2. The new column is listed in audit.schema_changes"):
        changes = wh.get_schema_changes(batches[1])
        assert changes == [("customers", "loyalty_tier", "added")], (
            "Failed to compare schema changes! "
            f"Expected: [('customers', 'loyalty_tier', 'added')], Actual: {changes}")

    with custom.wrapped_test_step("3. The batch is loaded as usual"):
        wh.verify_staged_orders(SourceReader(data_dir, batches).get_expected_orders())


@pytest.mark.negative
@pytest.mark.case_id("DQ-SCN-008")
def test_amounts_in_a_new_number_format_are_rejected(make_pipeline: Callable, make_source: Callable,
                                                     custom: CustomCommands) -> None:
    """DQ-SCN-008 Type drift: amounts like "12,50" are quarantined, not loaded as NULL or 1250"""
    data_dir, batches = make_source(drift="amount_as_text")
    source = SourceReader(data_dir, batches)
    pipeline, wh = make_pipeline()

    with custom.wrapped_test_step("1. Load both batches; every tenth order of batch 2 has a comma amount"):
        pipeline.run_all(data_dir, batches)

    with custom.wrapped_test_step("2. Exactly those orders are rejected as amount_not_numeric"):
        wh.verify_rejected("orders", "amount_not_numeric", source.get_expected_rejects()["amount_not_numeric"])

    with custom.wrapped_test_step("3. The rest of the batch is loaded"):
        wh.verify_staged_orders(source.get_expected_orders())
