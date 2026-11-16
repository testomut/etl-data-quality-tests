"""Marts: the numbers reporting will show, reconciled with the source and with each other."""

from __future__ import annotations

from decimal import Decimal

import pytest

from src.connectors.warehouse import WarehouseConnector
from src.utils.custom_commands import CustomCommands
from src.utils.source_reader import SourceReader


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-001")
def test_fact_table_has_one_row_per_staged_order(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-MRT-001 Completeness: fct_orders holds every staged order exactly once"""
    with custom.wrapped_test_step("1. Same row count as staging.orders"):
        wh.verify_row_count("marts.fct_orders", wh.get_row_count("staging.orders"))

    with custom.wrapped_test_step("2. No order_id repeats and none is missing"):
        wh.verify_no_duplicates("marts.fct_orders", "order_id")
        wh.verify_no_orphans("staging.orders", "order_id", "marts.fct_orders", "order_id")


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-002")
def test_eur_amount_uses_the_rate_of_the_order_day(wh: WarehouseConnector, source: SourceReader,
                                                   custom: CustomCommands) -> None:
    """DQ-MRT-002 Transformation: amount_eur = amount x rate of the order day and currency, rounded to cents"""
    with custom.wrapped_test_step("1. Compare amount_eur of every order with the value computed from the source"):
        wh.verify_fct_amounts(source.get_expected_orders())


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-003")
def test_daily_revenue_matches_the_source(wh: WarehouseConnector, source: SourceReader,
                                          custom: CustomCommands) -> None:
    """DQ-MRT-003 Aggregate reconciliation: paid orders and revenue per day and country equal the source"""
    with custom.wrapped_test_step("1. Compare every (day, country) row of daily_revenue"):
        wh.verify_daily_revenue(source.get_expected_daily_revenue())


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-004")
def test_revenue_mart_agrees_with_the_fact_table(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-MRT-004 Internal consistency: total of daily_revenue = total of paid orders in fct_orders"""
    with custom.wrapped_test_step("1. Sum both tables"):
        mart = sum((r for _, r in wh.get_daily_revenue().values()), Decimal("0"))
        fct = sum((a for oid, (a, _) in wh.get_fct_amounts().items()
                   if wh.get_order_status(oid) == "paid"), Decimal("0"))

    with custom.wrapped_test_step("2. The totals are equal to the cent"):
        assert mart == fct, f"Failed to compare total revenue! Expected (fct_orders): {fct}, Actual (mart): {mart}"


@pytest.mark.negative
@pytest.mark.case_id("DQ-MRT-005")
def test_unpaid_orders_are_not_revenue(wh: WarehouseConnector, source: SourceReader,
                                       custom: CustomCommands) -> None:
    """DQ-MRT-005 Business rule: created, cancelled and refunded orders never count as revenue"""
    expected = source.get_expected_orders()
    unpaid = [o for o in expected.values() if o.status != "paid"]

    with custom.wrapped_test_step("1. The feed contains unpaid orders of every kind"):
        kinds = {o.status for o in unpaid}
        assert kinds == {"created", "cancelled", "refunded"}, (
            "Failed to compare unpaid statuses in the feed! "
            f"Expected: created, cancelled, refunded, Actual: {kinds}")

    with custom.wrapped_test_step("2. Paid order count in the mart equals the paid orders of the source"):
        mart_orders = sum(n for n, _ in wh.get_daily_revenue().values())
        paid = len(expected) - len(unpaid)
        assert mart_orders == paid, f"Failed to compare paid order count! Expected: {paid}, Actual: {mart_orders}"


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-006")
def test_every_paid_order_has_one_matching_payment(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-MRT-006 Cross-source reconciliation: each paid or refunded order has exactly one payment, same amount"""
    with custom.wrapped_test_step("1. Compare orders with payments"):
        wh.verify_payments_match_orders()


@pytest.mark.negative
@pytest.mark.case_id("DQ-MRT-007")
def test_unpaid_orders_have_no_payment(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-MRT-007 Cross-source reconciliation: created and cancelled orders have no payment"""
    with custom.wrapped_test_step("1. Look for payments of created or cancelled orders"):
        found = wh.get_payments_for_unpaid_orders()
        assert not found, f"Payments for unpaid orders! Expected: none, Actual: {found}"
