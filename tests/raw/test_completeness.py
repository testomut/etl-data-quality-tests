"""Raw layer: everything the sources delivered lands in raw, unfiltered, with batch metadata.

Raw is the evidence a dispute is settled with ("did the feed contain this order?"), so it keeps invalid rows too;
validation happens one layer later.
"""

from __future__ import annotations

import pytest

from src.connectors.warehouse import WarehouseConnector
from src.utils.custom_commands import CustomCommands
from src.utils.source_reader import SourceReader

FILES = [
    pytest.param(batch, entity, marks=pytest.mark.case_id(f"DQ-RAW-{i:03d}"), id=f"{batch}-{entity}")
    for i, (batch, entity) in enumerate(
        [(b, e) for b in ("batch_001", "batch_002") for e in ("customers", "orders", "payments", "fx_rates")],
        start=1)
]


@pytest.mark.positive
@pytest.mark.parametrize(("batch_id", "entity"), FILES)
def test_every_source_row_lands_in_raw(wh: WarehouseConnector, source: SourceReader, custom: CustomCommands,
                                       batch_id: str, entity: str) -> None:
    """DQ-RAW-001..008 Completeness: raw row count per batch and feed equals the source file"""
    with custom.wrapped_test_step(f"1. Count rows of {entity} in the {batch_id} file"):
        expected = source.get_row_count(batch_id, entity)
        manifest_count = source.get_manifest(batch_id)["row_counts"][entity]
        assert expected == manifest_count, (
            f"Failed to compare file rows with the manifest! Expected: {manifest_count}, Actual: {expected}")

    with custom.wrapped_test_step(f"2. Compare with raw.{entity} for {batch_id}"):
        wh.verify_row_count(f"raw.{entity}", expected, "_batch_id = ?", [batch_id])


@pytest.mark.positive
@pytest.mark.case_id("DQ-RAW-009")
def test_raw_rows_carry_batch_metadata(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-RAW-009 Lineage: every raw row has its batch ID, source file name and load time"""
    for entity in ("customers", "orders", "payments", "fx_rates"):
        with custom.wrapped_test_step(f"1. Check lineage columns of raw.{entity}"):
            for column in ("_batch_id", "_source_file", "_loaded_at"):
                wh.verify_row_count(f"raw.{entity}", 0, f"{column} IS NULL")


@pytest.mark.positive
@pytest.mark.case_id("DQ-RAW-010")
def test_raw_keeps_rows_that_staging_rejects(wh: WarehouseConnector, source: SourceReader,
                                             custom: CustomCommands) -> None:
    """DQ-RAW-010 Raw is unfiltered: rejected orders are still present in raw"""
    rejected = [oid for ids in source.get_expected_rejects().values() for oid in ids if oid.startswith("O")]

    with custom.wrapped_test_step("1. Every order the rules reject is still in raw.orders"):
        assert rejected, "The standard feed must contain invalid orders, otherwise this test proves nothing"
        for order_id in rejected:
            wh.verify_row_count("raw.orders", 1, "order_id = ?", [order_id])
