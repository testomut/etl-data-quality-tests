"""Scenarios: data that arrives on time but describes yesterday is still a problem for a daily report."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from src.utils.custom_commands import CustomCommands

MAX_LAG_HOURS = 24  # agreed in docs/TEST_STRATEGY.md


@pytest.mark.positive
@pytest.mark.case_id("DQ-SCN-009")
def test_regular_batch_is_fresh(make_pipeline: Callable, source_dir: Path, batches: list[str],
                                custom: CustomCommands) -> None:
    """DQ-SCN-009 Freshness: the newest change in a regular batch is less than 24 hours older than its as_of"""
    pipeline, wh = make_pipeline()
    pipeline.run_all(source_dir, batches)

    with custom.wrapped_test_step("1. Compare the lag of batch 2 with the limit"):
        lag = wh.get_freshness_lag_hours(batches[1])
        assert 0 <= lag <= MAX_LAG_HOURS, (
            f"Failed to compare freshness lag! Expected: 0..{MAX_LAG_HOURS} h, Actual: {lag:.1f} h")


@pytest.mark.negative
@pytest.mark.case_id("DQ-SCN-010")
def test_stale_batch_is_detected(make_pipeline: Callable, make_source: Callable, custom: CustomCommands) -> None:
    """DQ-SCN-010 Freshness: a batch whose changes are 30 hours old is reported as stale"""
    data_dir, batches = make_source(stale_hours=30)
    pipeline, wh = make_pipeline()
    pipeline.run_all(data_dir, batches)

    with custom.wrapped_test_step("1. The lag of batch 2 is over the limit"):
        lag = wh.get_freshness_lag_hours(batches[1])
        assert lag > MAX_LAG_HOURS, (
            f"Failed to detect stale data! Expected: > {MAX_LAG_HOURS} h, Actual: {lag:.1f} h")
