"""Checks of the test framework itself: if these break, the data tests above cannot be trusted."""

from __future__ import annotations

import filecmp
from pathlib import Path

import pytest

from src.datagen.generator import generate
from src.pipeline.contracts import check_columns
from src.pipeline.pipeline import Pipeline
from src.utils.custom_commands import CustomCommands


@pytest.mark.positive
@pytest.mark.case_id("DQ-UNIT-001")
def test_generator_is_deterministic(tmp_path: Path, custom: CustomCommands) -> None:
    """DQ-UNIT-001 The same seed gives byte-identical files, so every worker and every run tests the same data"""
    with custom.wrapped_test_step("1. Generate the feed twice"):
        batches = generate(tmp_path / "a")
        generate(tmp_path / "b")

    with custom.wrapped_test_step("2. Every file is identical"):
        for batch_id in batches:
            cmp = filecmp.dircmp(tmp_path / "a" / batch_id, tmp_path / "b" / batch_id)
            assert not (cmp.diff_files or cmp.left_only or cmp.right_only), (
                f"Failed to compare generated files of {batch_id}! Differ: {cmp.diff_files}, "
                f"only once: {cmp.left_only + cmp.right_only}")


@pytest.mark.positive
@pytest.mark.case_id("DQ-UNIT-002")
def test_contract_check_reports_missing_and_extra_columns(custom: CustomCommands) -> None:
    """DQ-UNIT-002 The contract check names the missing and the extra column"""
    with custom.wrapped_test_step("1. Check a customers header with country renamed"):
        columns = ["customer_id", "email", "full_name", "country_code", "segment", "updated_at"]
        missing, extra = check_columns("customers", columns)
        assert (missing, extra) == (["country"], ["country_code"]), (
            "Failed to compare contract check! "
            f"Expected: (['country'], ['country_code']), Actual: {(missing, extra)}")


@pytest.mark.negative
@pytest.mark.case_id("DQ-UNIT-003")
def test_unknown_fault_name_is_refused(tmp_path: Path, custom: CustomCommands) -> None:
    """DQ-UNIT-003 A typo in a fault name fails at once instead of silently testing the healthy pipeline"""
    with custom.wrapped_test_step("1. Create a pipeline with a misspelled fault"):
        with pytest.raises(ValueError, match="Unknown faults"):
            Pipeline(tmp_path / "w.duckdb", faults={"wrong_fxx"})
