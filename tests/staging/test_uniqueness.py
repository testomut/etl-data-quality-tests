"""Staging: one row per business key, the newest version of a record wins, and no delivered row is lost."""

from __future__ import annotations

import pytest

from src.connectors.warehouse import WarehouseConnector
from src.utils.custom_commands import CustomCommands
from src.utils.source_reader import SourceReader


@pytest.mark.positive
@pytest.mark.case_id("DQ-STG-007")
def test_orders_are_unique(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-STG-007 Uniqueness: one staged row per order_id"""
    with custom.wrapped_test_step("1. Look for repeated order_id in staging.orders"):
        wh.verify_no_duplicates("staging.orders", "order_id")


@pytest.mark.positive
@pytest.mark.case_id("DQ-STG-008")
def test_customers_and_payments_are_unique(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-STG-008 Uniqueness: one staged row per customer_id and per payment_id"""
    with custom.wrapped_test_step("1. Look for repeated customer_id and payment_id"):
        wh.verify_no_duplicates("staging.customers", "customer_id")
        wh.verify_no_duplicates("staging.payments", "payment_id")


@pytest.mark.positive
@pytest.mark.case_id("DQ-STG-009")
def test_newer_version_of_a_redelivered_order_wins(wh: WarehouseConnector, source: SourceReader,
                                                   custom: CustomCommands) -> None:
    """DQ-STG-009 Deduplication: when one batch delivers an order twice, the latest updated_at is kept"""
    redelivered = source.get_redelivered_orders()
    expected = source.get_expected_orders()

    with custom.wrapped_test_step("1. The feed contains orders delivered twice in one batch"):
        assert redelivered, "The standard feed must contain redelivered orders, otherwise this test proves nothing"

    with custom.wrapped_test_step("2. Each of them is staged once, with the status of its newest version"):
        for order_id in redelivered:
            wh.verify_row_count("staging.orders", 1, "order_id = ?", [order_id])
            wh.verify_order_status(order_id, expected[order_id].status)


@pytest.mark.positive
@pytest.mark.case_id("DQ-STG-010")
def test_every_delivered_order_row_is_accounted_for(wh: WarehouseConnector, source: SourceReader,
                                                    custom: CustomCommands) -> None:
    """DQ-STG-010 Row accounting: delivered = staged + rejected + collapsed duplicates, for batch 2"""
    batch_id = "batch_002"
    rows = source.get_rows(batch_id, "orders")
    delivered = len(rows)
    duplicates = delivered - len({r["order_id"] for r in rows})

    with custom.wrapped_test_step("1. Count what staging and the rejects table hold for the batch"):
        staged = wh.get_row_count("staging.orders", "_batch_id = ?", [batch_id])
        rejected = wh.get_row_count("audit.rejects", "batch_id = ? AND entity = 'orders'", [batch_id])

    with custom.wrapped_test_step("2. Nothing is lost and nothing is invented"):
        assert staged + rejected + duplicates == delivered, (
            f"Failed to compare delivered order rows of {batch_id}! Expected: {delivered}, "
            f"Actual: staged {staged} + rejected {rejected} + duplicates {duplicates} = "
            f"{staged + rejected + duplicates}")
