"""Staging: typed, valid current state of each entity. Checked against rules and against the source."""

from __future__ import annotations

import pytest

from src.connectors.warehouse import WarehouseConnector
from src.pipeline.contracts import ACCEPTED_CURRENCIES, ACCEPTED_STATUSES
from src.utils.custom_commands import CustomCommands
from src.utils.source_reader import SourceReader


@pytest.mark.positive
@pytest.mark.case_id("DQ-STG-001")
def test_required_order_fields_are_filled(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-STG-001 Not null: required order fields are filled in staging"""
    with custom.wrapped_test_step("1. Check required columns of staging.orders"):
        wh.verify_no_nulls("staging.orders", ["customer_id", "order_ts", "status", "amount", "currency",
                                              "updated_at"], key="order_id")


@pytest.mark.positive
@pytest.mark.case_id("DQ-STG-002")
def test_order_status_is_an_accepted_value(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-STG-002 Accepted values: order status"""
    with custom.wrapped_test_step("1. Check staging.orders.status"):
        wh.verify_accepted_values("staging.orders", "status", ACCEPTED_STATUSES)


@pytest.mark.positive
@pytest.mark.case_id("DQ-STG-003")
def test_order_currency_is_an_accepted_value(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-STG-003 Accepted values: order currency"""
    with custom.wrapped_test_step("1. Check staging.orders.currency"):
        wh.verify_accepted_values("staging.orders", "currency", ACCEPTED_CURRENCIES)


@pytest.mark.positive
@pytest.mark.case_id("DQ-STG-004")
def test_order_amounts_are_positive(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-STG-004 Range: every staged amount is greater than zero"""
    with custom.wrapped_test_step("1. Count non-positive amounts in staging.orders and staging.payments"):
        wh.verify_row_count("staging.orders", 0, "amount <= 0")
        wh.verify_row_count("staging.payments", 0, "amount <= 0")


@pytest.mark.positive
@pytest.mark.case_id("DQ-STG-005")
def test_customer_emails_are_well_formed(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-STG-005 Format: every staged customer email contains @"""
    with custom.wrapped_test_step("1. Count customers whose email has no @"):
        wh.verify_row_count("staging.customers", 0, "POSITION('@' IN email) = 0")


@pytest.mark.positive
@pytest.mark.case_id("DQ-STG-006")
def test_staged_orders_match_the_source(wh: WarehouseConnector, source: SourceReader,
                                        custom: CustomCommands) -> None:
    """DQ-STG-006 Source reconciliation: staged orders equal the latest valid version of each source order"""
    expected = source.get_expected_orders()

    with custom.wrapped_test_step("1. Same number of orders as the source final state"):
        wh.verify_row_count("staging.orders", len(expected))

    with custom.wrapped_test_step("2. Same orders with the same customer, status, amount and currency"):
        wh.verify_staged_orders(expected)
