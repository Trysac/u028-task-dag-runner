"""Behavior tests for the public API of task_dag_runner."""

from __future__ import annotations

import pytest

import task_dag_runner


def test_normalize_trims_and_lowercases() -> None:
    assert task_dag_runner.normalize("  Hello World ") == "hello world"


@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_normalize_rejects_blank_input(value: str) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        task_dag_runner.normalize(value)


def test_version_is_semver() -> None:
    parts = task_dag_runner.__version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)
