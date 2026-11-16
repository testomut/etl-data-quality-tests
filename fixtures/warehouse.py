"""Warehouse fixtures.

`wh` is a warehouse built once per worker from the standard feed and shared by the read-only tests, which only
SELECT; that keeps the suite fast. Anything that loads, re-runs or injects a fault takes `make_pipeline`, which
gives the test its own database file.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from src.connectors.warehouse import WarehouseConnector
from src.pipeline.pipeline import Pipeline
from src.utils.custom_commands import CustomCommands


@pytest.fixture(scope="session")
def built_warehouse(tmp_path_factory: pytest.TempPathFactory, worker_id: str, source_dir: Path,
                    batches: list[str]) -> Iterator[Pipeline]:
    db = tmp_path_factory.mktemp(f"warehouse_{worker_id}") / "warehouse.duckdb"
    pipeline = Pipeline(db)
    pipeline.run_all(source_dir, batches)
    yield pipeline
    pipeline.close()


@pytest.fixture(scope="session")
def wh(built_warehouse: Pipeline) -> WarehouseConnector:
    return WarehouseConnector(built_warehouse.con)


@pytest.fixture
def make_pipeline(tmp_path: Path) -> Iterator[Callable[..., tuple[Pipeline, WarehouseConnector]]]:
    """Fresh pipeline per call; the database file is removed first so a crashed run never leaks into the next."""
    opened: list[Pipeline] = []

    def _make(faults: set[str] | None = None,
              name: str = "warehouse.duckdb") -> tuple[Pipeline, WarehouseConnector]:
        db = tmp_path / name
        db.unlink(missing_ok=True)
        pipeline = Pipeline(db, faults=faults)
        opened.append(pipeline)
        return pipeline, WarehouseConnector(pipeline.con)

    yield _make
    for p in opened:
        p.close()


@pytest.fixture(scope="session")
def custom() -> CustomCommands:
    return CustomCommands()
