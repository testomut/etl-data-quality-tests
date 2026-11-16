"""Marts: dim_customer keeps history (SCD type 2), and facts use the version valid when the order happened.

Without history, a customer who moves from France to Germany drags all their past revenue to Germany, and last
month's report changes after it was published.
"""

from __future__ import annotations

import pytest

from src.connectors.warehouse import WarehouseConnector
from src.utils.custom_commands import CustomCommands
from src.utils.source_reader import SourceReader


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-012")
def test_each_customer_has_exactly_one_current_version(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-MRT-012 SCD2: one is_current row per customer"""
    with custom.wrapped_test_step("1. Look for customers with zero or several current versions"):
        bad = wh.get_current_version_counts()
        assert not bad, f"Customers without exactly one current version! Expected: none, Actual: {bad}"


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-013")
def test_versions_have_no_gaps_or_overlaps(wh: WarehouseConnector, custom: CustomCommands) -> None:
    """DQ-MRT-013 SCD2: each version ends exactly where the next one starts; only the last is open"""
    with custom.wrapped_test_step("1. Compare valid_to of each version with valid_from of the next"):
        broken = wh.get_broken_intervals()
        assert not broken, f"Broken validity intervals (key, valid_to, next valid_from)! {broken}"


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-014")
def test_customer_who_moved_country_has_full_history(wh: WarehouseConnector, source: SourceReader,
                                                     custom: CustomCommands) -> None:
    """DQ-MRT-014 SCD2: a change of country opens a new version and closes the old one"""
    moved = source.get_moved_customers()
    history = source.get_customer_versions()

    with custom.wrapped_test_step("1. The feed contains customers who changed country"):
        assert moved, "The standard feed must contain a customer who moved, otherwise this test proves nothing"

    with custom.wrapped_test_step("2. Their history in dim_customer equals the source history"):
        for customer_id in moved:
            wh.verify_customer_versions(
                customer_id, [(v.country, v.valid_from, v.valid_to) for v in history[customer_id]])


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-015")
def test_orders_report_the_country_valid_at_order_time(wh: WarehouseConnector, source: SourceReader,
                                                       custom: CustomCommands) -> None:
    """DQ-MRT-015 Point-in-time join: an order keeps the customer's country from the moment it was placed"""
    expected = source.get_expected_orders()
    moved = set(source.get_moved_customers())

    with custom.wrapped_test_step("1. Customers who moved have orders placed before the move"):
        before_move = [o for o in expected.values() if o.customer_id in moved]
        assert before_move, "No orders of moved customers in the feed, so the join is not exercised"

    with custom.wrapped_test_step("2. Every order has the country of the version valid at order time"):
        wh.verify_fct_countries(expected)


@pytest.mark.positive
@pytest.mark.case_id("DQ-MRT-016")
def test_unchanged_customers_keep_a_single_version(wh: WarehouseConnector, source: SourceReader,
                                                   custom: CustomCommands) -> None:
    """DQ-MRT-016 SCD2: a customer without changes to tracked attributes has one version only"""
    unchanged = [cid for cid, versions in source.get_customer_versions().items() if len(versions) == 1]

    with custom.wrapped_test_step("1. dim_customer has one row for each of them"):
        wh.verify_row_count("marts.dim_customer", len(unchanged),
                            "customer_id IN (SELECT UNNEST(?))", [unchanged])
