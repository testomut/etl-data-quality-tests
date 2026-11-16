"""Source data fixtures: the generated feed and the oracle that reads it."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from src.datagen.generator import GeneratorOptions, generate
from src.utils.source_reader import SourceReader


@pytest.fixture(scope="session")
def source_dir(tmp_path_factory: pytest.TempPathFactory, worker_id: str) -> Path:
    """The standard two-batch feed, generated once per xdist worker into its own folder."""
    folder = tmp_path_factory.mktemp(f"source_{worker_id}")
    generate(folder)
    return folder


@pytest.fixture(scope="session")
def batches(source_dir: Path) -> list[str]:
    return sorted(p.name for p in source_dir.iterdir() if p.is_dir())


@pytest.fixture(scope="session")
def source(source_dir: Path, batches: list[str]) -> SourceReader:
    return SourceReader(source_dir, batches)


@pytest.fixture
def make_source(tmp_path: Path) -> Callable[..., tuple[Path, list[str]]]:
    """A feed with scenario options (schema drift, stale data, extra invalid rows) in this test's own folder."""

    def _make(**options: object) -> tuple[Path, list[str]]:
        folder = tmp_path / "source"
        return folder, generate(folder, GeneratorOptions(**options))  # type: ignore[arg-type]

    return _make
