"""Test-step reporting shared by every test."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager

log = logging.getLogger("steps")

try:  # Allure is optional: the suite must run without it.
    import allure
except ImportError:  # pragma: no cover - depends on the environment
    allure = None


class CustomCommands:
    @contextmanager
    def wrapped_test_step(self, name: str) -> Iterator[None]:
        """Numbered step: an Allure step when Allure is installed, a timed log line otherwise."""
        started = time.perf_counter()
        if allure is not None:
            with allure.step(name):
                yield
        else:
            log.info("[STEP] %s", name)
            yield
        log.info("[STEP] %s - %.2fs", name, time.perf_counter() - started)
