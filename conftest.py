"""Root pytest configuration: plugin wiring and the case-ID check. Fixtures live in fixtures/."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest_plugins = ["fixtures.data", "fixtures.warehouse"]

CASES = Path(__file__).parent / "tests" / "cases.json"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Every test carries exactly one registered case ID, and no two tests share one.

    The ID is what a test management tool and a bug report refer to; it must survive renames of the test.
    """
    registry = json.loads(CASES.read_text(encoding="utf-8")) if CASES.exists() else {}
    seen: dict[str, str] = {}
    problems = []
    for item in items:
        marks = list(item.iter_markers("case_id"))
        if len(marks) != 1:
            problems.append(f"{item.nodeid}: expected one case_id marker, found {len(marks)}")
            continue
        case_id = marks[0].args[0]
        if case_id not in registry:
            problems.append(f"{item.nodeid}: case ID {case_id} is not in tests/cases.json")
        if case_id in seen:
            problems.append(f"{item.nodeid}: case ID {case_id} already used by {seen[case_id]}")
        seen[case_id] = item.nodeid
    if problems and not config.getoption("--collect-only"):
        raise pytest.UsageError("Case ID problems:\n  " + "\n  ".join(problems))
