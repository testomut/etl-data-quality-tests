"""Marts: every reference resolves, so no row silently disappears from a join in a report."""

from __future__ import annotations

import pytest

from src.connectors.warehouse import WarehouseConnector
from src.utils.custom_commands import CustomCommands


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-008")
def test_every_order_points_to_a_customer_version(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-MRT-008 Referential integrity: fct_orders.customer_key exists in dim_customer"""
    with custom.wrapped_test_step("1. No order without a customer version"):
        wh.verify_row_count("marts.fct_orders", 0, "customer_key IS NULL")

    with custom.wrapped_test_step("2. Every key resolves to a row of dim_customer"):
        wh.verify_no_orphans("marts.fct_orders", "customer_key", "marts.dim_customer", "customer_key")


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-009")
def test_every_order_has_an_fx_rate(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-MRT-009 Referential integrity: every order found an FX rate for its day and currency"""
    with custom.wrapped_test_step("1. No order without a rate or an EUR amount"):
        wh.verify_no_nulls("marts.fct_orders", ["rate_to_eur", "amount_eur"], key="order_id")


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-010")
def test_every_payment_belongs_to_a_known_order(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-MRT-010 Referential integrity: staging.payments.order_id exists in staging.orders"""
    with custom.wrapped_test_step("1. Look for payments of unknown orders"):
        wh.verify_no_orphans("staging.payments", "order_id", "staging.orders", "order_id")


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-011")
def test_mart_tables_keep_their_published_columns(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-MRT-011 Output contract: reports depend on these exact columns, in this order"""
    with custom.wrapped_test_step("1. Compare the columns of each mart table"):
        wh.verify_columns("marts.fct_orders", ["order_id", "customer_key", "customer_id", "country", "order_ts",
                                               "order_date", "status", "amount", "currency", "rate_to_eur",
                                               "amount_eur"])
        wh.verify_columns("marts.daily_revenue", ["order_date", "country", "paid_orders", "revenue_eur"])
        wh.verify_columns("marts.dim_customer", ["customer_key", "customer_id", "email", "full_name", "country",
                                                 "segment", "valid_from", "valid_to", "is_current"])
