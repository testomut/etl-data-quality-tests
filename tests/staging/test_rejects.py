"""Staging: invalid rows are quarantined in audit.rejects with a reason, and never reach staging."""

from __future__ import annotations

import pytest

from src.connectors.warehouse import WarehouseConnector
from src.utils.custom_commands import CustomCommands
from src.utils.source_reader import SourceReader

REASONS = [
    pytest.param("orders", "missing_customer_id", marks=pytest.mark.case_id("DQ-STG-011"), id="missing-customer"),
    pytest.param("orders", "non_positive_amount", marks=pytest.mark.case_id("DQ-STG-012"), id="negative-amount"),
    pytest.param("orders", "unknown_currency", marks=pytest.mark.case_id("DQ-STG-013"), id="unknown-currency"),
    pytest.param("orders", "unknown_customer", marks=pytest.mark.case_id("DQ-STG-014"), id="orphan-customer"),
    pytest.param("customers", "invalid_email", marks=pytest.mark.case_id("DQ-STG-015"), id="invalid-email"),
]


@pytest.mark.negative
@pytest.mark.parametrize(("entity", "reason"), REASONS)
def test_invalid_rows_are_quarantined_with_a_reason(wh: WarehouseConnector, source: SourceReader,
                                                    custom: CustomCommands, entity: str, reason: str) -> None:
    """DQ-STG-011..015 Quarantine: each broken rule sends exactly the offending rows to audit.rejects"""
    expected = source.get_expected_rejects().get(reason, [])
    key = "order_id" if entity == "orders" else "customer_id"

    with custom.wrapped_test_step(f"1. The source contains {entity} that break the rule {reason}"):
        assert expected, f"The standard feed must contain a {reason} row, otherwise this test proves nothing"

    with custom.wrapped_test_step("2. Exactly those rows are in audit.rejects with that reason"):
        wh.verify_rejected(entity, reason, expected)

    with custom.wrapped_test_step(f"3. None of them reached staging.{entity}"):
        for record_key in expected:
            wh.verify_row_count(f"staging.{entity}", 0, f"{key} = ?", [record_key])


@pytest.mark.negative
@pytest.mark.case_id("DQ-STG-016")
def test_rejected_payload_is_kept_for_investigation(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-STG-016 Quarantine: every reject keeps its batch, entity, reason and original payload"""
    with custom.wrapped_test_step("1. No reject row misses the fields needed to investigate it"):
        for column in ("batch_id", "entity", "reason", "payload"):
            wh.verify_row_count("audit.rejects", 0, f"{column} IS NULL")
