"""Quality gate: the GO / NO-GO recommendation before marts are published, and its report."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from src.quality.gate import evaluate, write_report
from src.utils.custom_commands import CustomCommands


@pytest.mark.positive
@pytest.mark.case_id("DQ-GATE-001")
def test_clean_batch_gets_go(make_pipeline: Callable, source_dir: Path, batches: list[str],
                             custom: CustomCommands) -> None:
    """DQ-GATE-001 A regular batch passes every check and gets GO"""
    pipeline, _ = make_pipeline()
    pipeline.run_all(source_dir, batches)

    with custom.wrapped_test_step("1. Evaluate the gate for batch 2"):
        result = evaluate(pipeline.con, batches[1])

    with custom.wrapped_test_step("2. Verdict is GO and no check failed"):
        assert result.verdict == "GO" and not result.failed, (
            f"Failed to compare gate verdict! Expected: GO, Actual: {result.verdict}, failed: {result.failed}")


@pytest.mark.negative
@pytest.mark.case_id("DQ-GATE-002")
def test_too_many_rejects_gets_no_go(make_pipeline: Callable, make_source: Callable,
                                     custom: CustomCommands) -> None:
    """DQ-GATE-002 A batch with more than 5% rejected orders gets NO-GO, naming reject_rate"""
    data_dir, batches = make_source(extra_invalid_orders=10)
    pipeline, _ = make_pipeline()
    pipeline.run_all(data_dir, batches)

    with custom.wrapped_test_step("1. Evaluate the gate for the batch with extra invalid rows"):
        result = evaluate(pipeline.con, batches[1])

    with custom.wrapped_test_step("2. Verdict is NO-GO because of the reject rate only"):
        assert (result.verdict, result.failed) == ("NO-GO", ["reject_rate"]), (
            f"Failed to compare gate verdict! Expected: NO-GO on reject_rate, "
            f"Actual: {result.verdict} on {result.failed}")


@pytest.mark.negative
@pytest.mark.case_id("DQ-GATE-003")
def test_stale_batch_gets_no_go(make_pipeline: Callable, make_source: Callable, custom: CustomCommands) -> None:
    """DQ-GATE-003 A batch whose newest change is 30 hours old gets NO-GO, naming freshness_hours"""
    data_dir, batches = make_source(stale_hours=30)
    pipeline, _ = make_pipeline()
    pipeline.run_all(data_dir, batches)

    with custom.wrapped_test_step("1. Evaluate the gate for the stale batch"):
        result = evaluate(pipeline.con, batches[1])

    with custom.wrapped_test_step("2. Verdict is NO-GO and freshness is among the reasons"):
        assert result.verdict == "NO-GO" and "freshness_hours" in result.failed, (
            f"Failed to compare gate verdict! Expected: NO-GO with freshness_hours, "
            f"Actual: {result.verdict} on {result.failed}")


@pytest.mark.positive
@pytest.mark.case_id("DQ-GATE-004")
def test_gate_report_states_verdict_and_reasons(make_pipeline: Callable, make_source: Callable, tmp_path: Path,
                                                custom: CustomCommands) -> None:
    """DQ-GATE-004 The report (JSON for CI, Markdown for people) carries the verdict, values and limits"""
    data_dir, batches = make_source(extra_invalid_orders=10)
    pipeline, _ = make_pipeline()
    pipeline.run_all(data_dir, batches)
    result = evaluate(pipeline.con, batches[1])

    with custom.wrapped_test_step("1. Write the report"):
        json_path, md_path = write_report(result, tmp_path / "reports")

    with custom.wrapped_test_step("2. JSON has the verdict and every check with value and limit"):
        data = json.loads(json_path.read_text(encoding="utf-8"))
        assert data["verdict"] == "NO-GO", (
            f"Failed to compare JSON verdict! Expected: NO-GO, Actual: {data['verdict']}")
        assert {c["name"] for c in data["checks"]} == {c.name for c in result.checks}, "Checks missing in JSON"

    with custom.wrapped_test_step("3. Markdown states the recommendation and the failing check"):
        text = md_path.read_text(encoding="utf-8")
        assert "NO-GO" in text and "fix reject_rate" in text, f"Recommendation missing in the report:\n{text}"
