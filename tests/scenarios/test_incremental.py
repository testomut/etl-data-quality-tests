"""Scenarios: incremental loading over several batches, re-runs and late data. Each test has its own warehouse."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import pytest

from src.utils.custom_commands import CustomCommands
from src.utils.source_reader import SourceReader


@pytest.mark.positive
@pytest.mark.case_id("DQ-SCN-001")
def test_rerunning_a_loaded_batch_changes_nothing(make_pipeline: Callable, source_dir: Path, batches: list[str],
                                                  custom: CustomCommands) -> None:
    """DQ-SCN-001 Idempotency: processing the same batch again leaves every table as it was"""
    pipeline, wh = make_pipeline()

    with custom.wrapped_test_step("1. Load both batches and take a snapshot"):
        pipeline.run_all(source_dir, batches)
        before = wh.get_snapshot()

    with custom.wrapped_test_step("2. Run batch 2 again: the run is recorded as skipped"):
        status = pipeline.run_batch(source_dir / batches[-1])
        assert status == "skipped", f"Failed to compare re-run status! Expected: skipped, Actual: {status}"

    with custom.wrapped_test_step("3. Counts and revenue are unchanged"):
        after = wh.get_snapshot()
        assert after == before, f"Failed to compare warehouse after a re-run! Expected: {before}, Actual: {after}"


@pytest.mark.positive
@pytest.mark.case_id("DQ-SCN-002")
def test_replaying_an_older_batch_does_not_roll_data_back(make_pipeline: Callable, source_dir: Path,
                                                          batches: list[str], source: SourceReader,
                                                          custom: CustomCommands) -> None:
    """DQ-SCN-002 Watermark: an older batch processed after a newer one cannot overwrite newer versions"""
    pipeline, wh = make_pipeline()
    pipeline.run_all(source_dir, batches)
    refunded = [o.order_id for o in source.get_expected_orders().values() if o.status == "refunded"]

    with custom.wrapped_test_step("1. Forget that batch 1 was loaded and process it again"):
        pipeline.con.execute("DELETE FROM audit.batch_runs WHERE batch_id = ?", [batches[0]])
        pipeline.run_batch(source_dir / batches[0])

    with custom.wrapped_test_step("2. Orders refunded in batch 2 are still refunded"):
        for order_id in refunded:
            wh.verify_order_status(order_id, "refunded")
        wh.verify_no_duplicates("staging.orders", "order_id")


@pytest.mark.positive
@pytest.mark.case_id("DQ-SCN-003")
def test_watermark_follows_each_batch(make_pipeline: Callable, source_dir: Path, batches: list[str],
                                      custom: CustomCommands) -> None:
    """DQ-SCN-003 Watermark: after each batch it equals the newest valid change loaded so far"""
    pipeline, wh = make_pipeline()

    for i, batch_id in enumerate(batches, start=1):
        with custom.wrapped_test_step(f"{i}. Load {batch_id} and compare the watermarks with the source"):
            pipeline.run_batch(source_dir / batch_id)
            expected = SourceReader(source_dir, batches[:i])
            wh.verify_watermark("orders", expected.get_expected_watermark("orders"))
            wh.verify_watermark("customers", expected.get_expected_watermark("customers"))


@pytest.mark.positive
@pytest.mark.case_id("DQ-SCN-004")
def test_late_order_is_attached_to_the_day_it_happened(make_pipeline: Callable, source_dir: Path,
                                                       batches: list[str], source: SourceReader,
                                                       custom: CustomCommands) -> None:
    """DQ-SCN-004 Late-arriving data: an order delivered a day late lands in its own day's revenue"""
    pipeline, wh = make_pipeline()
    late = source.get_late_orders()
    final = source.get_expected_orders()
    after_batch_1 = SourceReader(source_dir, batches[:1]).get_expected_orders()
    day = final[late[0]].order_ts.date().isoformat()

    def on_day(order_id: str) -> bool:
        return final[order_id].order_ts.date().isoformat() == day

    late_paid = sum((final[o].amount_eur for o in late if final[o].status == "paid"), Decimal("0"))
    refunded_later = sum((final[o].amount_eur for o in after_batch_1 if on_day(o)
                          and after_batch_1[o].status == "paid" and final[o].status == "refunded"), Decimal("0"))

    with custom.wrapped_test_step(f"1. Load batch 1 and note the revenue of {day}"):
        pipeline.run_batch(source_dir / batches[0])
        before = wh.get_revenue_for_day(day)

    with custom.wrapped_test_step("2. Load batch 2, which carries late orders for that day"):
        pipeline.run_batch(source_dir / batches[1])

    with custom.wrapped_test_step("3. The day's revenue grew by the late orders, minus refunds of its orders"):
        actual = wh.get_revenue_for_day(day)
        assert late_paid > 0, "The late orders must be paid, otherwise they do not move revenue"
        assert actual == before + late_paid - refunded_later, (
            f"Failed to compare revenue of {day}! Expected: {before} + {late_paid} - {refunded_later}, "
            f"Actual: {actual}")


@pytest.mark.positive
@pytest.mark.case_id("DQ-SCN-005")
def test_refund_in_a_later_batch_reduces_earlier_revenue(make_pipeline: Callable, source_dir: Path,
                                                         batches: list[str], custom: CustomCommands) -> None:
    """DQ-SCN-005 Corrections: refunds delivered later move earlier revenue down, per the source of each stage"""
    pipeline, wh = make_pipeline()

    with custom.wrapped_test_step("1. After batch 1 the mart equals the batch 1 source"):
        pipeline.run_batch(source_dir / batches[0])
        wh.verify_daily_revenue(SourceReader(source_dir, batches[:1]).get_expected_daily_revenue())

    with custom.wrapped_test_step("2. After batch 2 the mart equals the combined source, refunds applied"):
        pipeline.run_batch(source_dir / batches[1])
        wh.verify_daily_revenue(SourceReader(source_dir, batches).get_expected_daily_revenue())
