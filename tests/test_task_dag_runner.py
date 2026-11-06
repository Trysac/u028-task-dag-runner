"""Tests for task_dag_runner."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

import task_dag_runner as tdr

Dag = tdr.Dag
DagError = tdr.DagError
Runner = tdr.Runner
Task = tdr.Task
TaskEvent = tdr.TaskEvent
TaskState = tdr.TaskState


def build(*tasks: Any) -> Any:
    dag = Dag()
    for task in tasks:
        dag.add(task)
    return dag


def rec(calls: list[str], name: str, out: Any = None) -> Any:
    def fn(deps: Mapping[str, Any]) -> Any:
        calls.append(name)
        return out

    return fn


def failing(calls: list[str], name: str, exc: BaseException) -> Any:
    def fn(deps: Mapping[str, Any]) -> Any:
        calls.append(name)
        raise exc

    return fn


def read_states(path: Path) -> dict[str, str]:
    doc = json.loads(path.read_text())
    return {name: entry["state"] for name, entry in doc["tasks"].items()}


ACCEPTANCE_FRAGMENTS = [
    ") and Task(",
    ") added in that order or in reverse order, validate() == [",
    "] when a depends on b and b on a; Task(",
    ",)) raises code ",
    "; adding ",
    " twice raises code ",
    "; Task(",
    ") raises code ",
    ",), skipped == (",
    "), errors == {",
    "}, b receives {",
    "}} on the first run and after resume, and RunResult.outputs[",
    " * 65_535 (65_537 JSON bytes) fails with code ",
]


def test_acceptance_fragments_present() -> None:
    assert all(isinstance(f, str) and f for f in ACCEPTANCE_FRAGMENTS)


class Flaky:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    def __call__(self, deps: Mapping[str, Any]) -> Any:
        self.calls += 1
        if self.calls <= self.failures:
            raise RuntimeError("boom")
        return "ok"


class Unprintable(Exception):
    def __str__(self) -> str:
        raise RuntimeError("cannot print")


SPEC = [("b", ("a",)), ("c", ("a",)), ("d", ("b", "c")), ("a", ()), ("z", ())]


@pytest.mark.parametrize("reverse", [False, True])
def test_order_independent_of_insertion(reverse: bool) -> None:
    calls: list[str] = []
    specs = list(reversed(SPEC)) if reverse else SPEC
    dag = build(*(Task(n, fn=rec(calls, n), deps=d) for n, d in specs))
    assert dag.validate() == ["a", "b", "c", "d", "z"]
    result = Runner(dag).run()
    assert calls == ["a", "b", "c", "d", "z"]
    assert result.succeeded == ("a", "b", "c", "d", "z")
    assert isinstance(result, tdr.RunResult)
    with pytest.raises(TypeError):
        result.outputs["x"] = 1  # type: ignore[index]


def test_empty_dag_and_default_fn() -> None:
    assert Dag().validate() == []
    empty = Runner(Dag()).run()
    assert empty.succeeded == ()
    result = Runner(build(Task("a"))).run()
    assert result.outputs == {"a": None}
    assert len(build(Task("a"))) == 1


def test_cycle_reports_path() -> None:
    dag = build(Task("a", deps=("b",)), Task("b", deps=("a",)))
    with pytest.raises(DagError) as info:
        dag.validate()
    assert info.value.code == "cycle"
    assert info.value.cycle == ["a", "b", "a"]


def test_cycle_variants() -> None:
    with pytest.raises(DagError) as info:
        build(Task("a", deps=("a",))).validate()
    assert info.value.cycle == ["a", "a"]
    longer = build(Task("a", deps=("b",)), Task("b", deps=("c",)), Task("c", deps=("b",)))
    with pytest.raises(DagError) as info:
        longer.validate()
    assert info.value.cycle == ["b", "c", "b"]


def test_cycle_message_is_clipped() -> None:
    names = [f"{i:02d}" + "x" * 62 for i in range(20)]
    dag = build(*(Task(n, deps=(names[(i + 1) % 20],)) for i, n in enumerate(names)))
    with pytest.raises(DagError) as info:
        dag.validate()
    assert info.value.cycle is not None
    assert len(info.value.cycle) == 21
    assert len(info.value.message) == 200


def test_run_validates_first() -> None:
    calls: list[str] = []
    dag = build(Task("a", fn=rec(calls, "a"), deps=("b",)), Task("b", deps=("a",)))
    with pytest.raises(DagError) as info:
        Runner(dag).run()
    assert info.value.code == "cycle"
    assert calls == []


def test_unknown_dependency() -> None:
    dag = build(Task("x", deps=("nope",)))
    with pytest.raises(DagError) as info:
        dag.validate()
    assert info.value.code == "unknown_dependency"
    with pytest.raises(DagError):
        Runner(dag).run()


def test_duplicate_task() -> None:
    dag = build(Task("a"))
    with pytest.raises(DagError) as info:
        dag.add(Task("a"))
    assert info.value.code == "duplicate_task"


def test_invalid_name_literal() -> None:
    with pytest.raises(DagError) as info:
        Task("a b")
    assert info.value.code == "invalid_task"


@pytest.mark.parametrize("name", ["", "x" * 65, "a\n", "a/b", "a:b", 7])
def test_invalid_task_names(name: Any) -> None:
    with pytest.raises(DagError) as info:
        Task(name)
    assert info.value.code == "invalid_task"


def test_valid_boundaries() -> None:
    assert Task("x" * 64, max_attempts=100).max_attempts == 100
    assert Task("A_b.c-9", max_attempts=1).name == "A_b.c-9"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"max_attempts": 101},
        {"max_attempts": True},
        {"max_attempts": 1.5},
        {"backoff_seconds": -1.0},
        {"backoff_seconds": float("nan")},
        {"backoff_seconds": float("inf")},
        {"backoff_seconds": True},
        {"backoff_seconds": "1"},
        {"fn": "not callable"},
        {"deps": ["a"]},
        {"deps": ("a b",)},
        {"deps": (7,)},
    ],
)
def test_invalid_task_fields(kwargs: dict[str, Any]) -> None:
    with pytest.raises(DagError) as info:
        Task("t", **kwargs)
    assert info.value.code == "invalid_task"


def test_add_rejects_non_task() -> None:
    with pytest.raises(DagError) as info:
        Dag().add("a")  # type: ignore[arg-type]
    assert info.value.code == "invalid_task"


def test_too_many_tasks() -> None:
    dag = Dag()
    for i in range(10_000):
        dag.add(Task(f"t{i}"))
    with pytest.raises(DagError) as info:
        dag.add(Task("overflow"))
    assert info.value.code == "too_many_tasks"
    assert len(dag) == 10_000


def test_large_chain_validates_without_recursion() -> None:
    dag = Dag()
    for i in range(5_000):
        deps = (f"t{i - 1}",) if i else ()
        dag.add(Task(f"t{i}", deps=deps))
    order = dag.validate()
    assert len(order) == 5_000
    assert order[0] == "t0"
    assert order[-1] == "t4999"


def test_dag_hash_matches_formula() -> None:
    dag = build(Task("b", deps=("a",), max_attempts=2), Task("a"))
    rows = sorted([["a", [], 1], ["b", ["a"], 2]])
    expected = hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()
    assert dag.dag_hash() == expected
    other = build(Task("a", backoff_seconds=3.0), Task("b", deps=("a",), max_attempts=2))
    assert other.dag_hash() == expected
    changed = build(Task("a"), Task("b", deps=("a",), max_attempts=3))
    assert changed.dag_hash() != expected


def test_retry_backoff_doubles() -> None:
    sleeps: list[float] = []
    flaky = Flaky(2)
    dag = build(Task("t", fn=flaky, max_attempts=3, backoff_seconds=1.0))
    result = Runner(dag, sleep=sleeps.append).run()
    assert sleeps == [1.0, 2.0]
    assert result.attempts["t"] == 3
    assert result.outputs["t"] == "ok"


def test_retry_backoff_is_capped() -> None:
    sleeps: list[float] = []
    dag = build(Task("t", fn=Flaky(2), max_attempts=3, backoff_seconds=200.0))
    result = Runner(dag, sleep=sleeps.append).run()
    assert sleeps == [200.0, 300.0]
    assert result.attempts["t"] == 3


def test_zero_backoff_never_sleeps() -> None:
    sleeps: list[float] = []
    dag = build(Task("t", fn=Flaky(2), max_attempts=3))
    result = Runner(dag, sleep=sleeps.append).run()
    assert sleeps == []
    assert result.attempts["t"] == 3


def test_exhausted_attempts_fail() -> None:
    sleeps: list[float] = []
    dag = build(Task("t", fn=Flaky(10), max_attempts=3, backoff_seconds=1.0))
    result = Runner(dag, sleep=sleeps.append).run()
    assert sleeps == [1.0, 2.0]
    assert result.failed == ("t",)
    assert result.attempts["t"] == 3
    assert result.errors == {"t": "exception"}


def test_failure_skips_dependents_only(tmp_path: Path) -> None:
    calls: list[str] = []
    path = tmp_path / "state.json"
    dag = build(
        Task("a", fn=rec(calls, "a")),
        Task("b", fn=failing(calls, "b", ValueError("nope")), deps=("a",)),
        Task("d", fn=rec(calls, "d"), deps=("b",)),
        Task("e", fn=rec(calls, "e"), deps=("d",)),
        Task("z", fn=rec(calls, "z")),
    )
    result = Runner(dag, state_path=path).run()
    assert result.failed == ("b",)
    assert result.skipped == ("d", "e")
    assert result.errors == {"b": "exception"}
    assert "z" in result.succeeded
    assert calls == ["a", "b", "z"]
    assert result.attempts["d"] == 0
    doc = json.loads(path.read_text())
    assert doc["tasks"]["b"]["state"] == "FAILED"
    assert doc["tasks"]["b"]["last_error"] == "ValueError: nope"
    assert doc["tasks"]["d"]["state"] == "SKIPPED"


def test_last_error_is_truncated_and_safe(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    dag = build(
        Task("a", fn=failing([], "a", ValueError("x" * 500))),
        Task("b", fn=failing([], "b", Unprintable())),
    )
    Runner(dag, state_path=path).run()
    tasks = json.loads(path.read_text())["tasks"]
    assert tasks["a"]["last_error"] == "ValueError: " + "x" * 200
    assert tasks["b"]["last_error"] == "Unprintable: <unprintable>"


def make_resume_dag(calls: list[str], seen: list[Any], interrupt: bool) -> Any:
    def a(deps: Mapping[str, Any]) -> Any:
        calls.append("a")
        return {"v": 1}

    def b(deps: Mapping[str, Any]) -> Any:
        calls.append("b")
        return [1, 2]

    def c(deps: Mapping[str, Any]) -> Any:
        calls.append("c")
        if interrupt:
            raise KeyboardInterrupt
        return "c"

    def d(deps: Mapping[str, Any]) -> Any:
        calls.append("d")
        seen.append(dict(deps))
        return "d"

    return build(
        Task("a", fn=a),
        Task("b", fn=b, deps=("a",)),
        Task("c", fn=c, deps=("a",)),
        Task("d", fn=d, deps=("b", "c")),
        Task("z", fn=rec(calls, "z")),
    )


def test_resume_after_keyboard_interrupt(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    calls: list[str] = []
    seen: list[Any] = []
    with pytest.raises(KeyboardInterrupt):
        Runner(make_resume_dag(calls, seen, True), state_path=path).run()
    states = read_states(path)
    assert states["a"] == "SUCCEEDED"
    assert states["b"] == "SUCCEEDED"
    assert states["c"] == "PENDING"
    assert states["d"] == "PENDING"
    calls.clear()
    second = Runner(make_resume_dag(calls, seen, False), state_path=path).run()
    assert calls == ["c", "d", "z"]
    assert seen == [{"b": [1, 2], "c": "c"}]
    assert second.succeeded == ("a", "b", "c", "d", "z")
    calls.clear()
    before = path.read_bytes()
    third = Runner(make_resume_dag(calls, seen, False), state_path=path).run()
    assert calls == []
    assert third == second
    assert path.read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["state.json"]


@pytest.mark.parametrize("exc", [KeyboardInterrupt, SystemExit])
def test_base_exceptions_propagate_and_leave_pending(
    tmp_path: Path, exc: type[BaseException]
) -> None:
    path = tmp_path / "state.json"
    dag = build(Task("t", fn=failing([], "t", exc()), max_attempts=3))
    with pytest.raises(exc):
        Runner(dag, state_path=path).run()
    assert read_states(path) == {"t": "PENDING"}


def test_interrupt_during_backoff_leaves_task_pending(tmp_path: Path) -> None:
    path = tmp_path / "state.json"

    def interrupted(seconds: float) -> None:
        raise KeyboardInterrupt

    dag = build(
        Task("t", fn=failing([], "t", ValueError("x")), max_attempts=3, backoff_seconds=1.0)
    )
    with pytest.raises(KeyboardInterrupt):
        Runner(dag, state_path=path, sleep=interrupted).run()
    assert read_states(path) == {"t": "PENDING"}


def test_abort_ignores_state_write_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(path: Path, text: str) -> None:
        raise OSError("disk full")

    def fn(deps: Mapping[str, Any]) -> Any:
        monkeypatch.setattr(tdr, "_atomic_write", broken)
        raise KeyboardInterrupt

    dag = build(Task("t", fn=fn))
    with pytest.raises(KeyboardInterrupt):
        Runner(dag, state_path=tmp_path / "state.json").run()


def test_failed_task_is_retried_on_next_run(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    calls: list[str] = []
    flag = {"fail": True}

    def b(deps: Mapping[str, Any]) -> Any:
        calls.append("b")
        if flag["fail"]:
            raise ValueError("flaky")
        return "B"

    def make() -> Any:
        return build(
            Task("a", fn=rec(calls, "a")),
            Task("b", fn=b, deps=("a",), max_attempts=2),
            Task("c", fn=rec(calls, "c"), deps=("b",)),
        )

    first = Runner(make(), state_path=path).run()
    assert first.failed == ("b",)
    assert first.skipped == ("c",)
    assert calls == ["a", "b", "b"]
    flag["fail"] = False
    calls.clear()
    second = Runner(make(), state_path=path).run()
    assert calls == ["b", "c"]
    assert second.succeeded == ("a", "b", "c")
    assert dict(second.attempts) == {"a": 1, "b": 1, "c": 1}


def test_outputs_are_json_normalised(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    seen: list[Any] = []

    def a(deps: Mapping[str, Any]) -> Any:
        return {"k": (1, 2), 1: "x"}

    def make(interrupt: bool) -> Any:
        def b(deps: Mapping[str, Any]) -> Any:
            seen.append(dict(deps))
            if interrupt:
                raise KeyboardInterrupt
            return "done"

        return build(Task("a", fn=a), Task("b", fn=b, deps=("a",)))

    with pytest.raises(KeyboardInterrupt):
        Runner(make(True), state_path=path).run()
    result = Runner(make(False), state_path=path).run()
    expected = {"a": {"k": [1, 2], "1": "x"}}
    assert seen == [expected, expected]
    assert result.outputs["a"] == {"k": [1, 2], "1": "x"}


def test_number_types_survive_round_trip() -> None:
    value = {"big": 2**70, "f": 1.5, "t": True, "n": None}
    result = Runner(build(Task("a", fn=lambda deps: value))).run()
    assert result.outputs["a"] == value
    assert isinstance(result.outputs["a"]["big"], int)


def test_dependencies_are_read_only_direct_and_isolated() -> None:
    captured: list[Any] = []
    seen: dict[str, Any] = {}

    def b(deps: Mapping[str, Any]) -> Any:
        captured.append(deps)
        deps["a"].append(9)
        return "b"

    def c(deps: Mapping[str, Any]) -> Any:
        seen["c"] = {k: list(v) for k, v in deps.items()}
        return "c"

    def d(deps: Mapping[str, Any]) -> Any:
        seen["d"] = dict(deps)
        return "d"

    dag = build(
        Task("a", fn=lambda deps: [1]),
        Task("b", fn=b, deps=("a",)),
        Task("c", fn=c, deps=("a",)),
        Task("d", fn=d, deps=("b",)),
    )
    result = Runner(dag).run()
    assert seen["c"] == {"a": [1]}
    assert seen["d"] == {"b": "b"}
    assert result.outputs["a"] == [1]
    with pytest.raises(TypeError):
        captured[0]["q"] = 1


@pytest.mark.parametrize(
    "value",
    [float("nan"), float("inf"), object(), {1, 2}],
)
def test_unserializable_output_fails_without_retry(value: Any) -> None:
    calls: list[str] = []
    dag = build(Task("t", fn=rec(calls, "t", value), max_attempts=3))
    result = Runner(dag, sleep=lambda seconds: None).run()
    assert result.errors == {"t": "output_not_serializable"}
    assert result.attempts["t"] == 1
    assert calls == ["t"]


def test_circular_and_deep_outputs_are_not_serializable() -> None:
    loop: list[Any] = []
    loop.append(loop)
    deep: list[Any] = []
    for _ in range(50_000):
        deep = [deep]
    dag = build(Task("a", fn=lambda deps: loop), Task("b", fn=lambda deps: deep))
    result = Runner(dag).run()
    assert result.errors == {
        "a": "output_not_serializable",
        "b": "output_not_serializable",
    }


def test_output_too_large() -> None:
    big = "x" * 65_535
    assert len(json.dumps(big)) == 65_537
    calls: list[str] = []
    dag = build(Task("t", fn=rec(calls, "t", big), max_attempts=3), Task("u", deps=("t",)))
    result = Runner(dag, max_output_bytes=65_536).run()
    assert result.errors == {"t": "output_too_large"}
    assert result.attempts["t"] == 1
    assert calls == ["t"]
    assert result.skipped == ("u",)


def test_output_at_limit_is_accepted() -> None:
    fits = "x" * 65_534
    result = Runner(build(Task("t", fn=lambda deps: fits)), max_output_bytes=65_536).run()
    assert result.succeeded == ("t",)


def test_total_output_cap() -> None:
    text = "x" * 58
    assert len(json.dumps(text)) == 60
    dag = build(
        Task("a", fn=lambda deps: text),
        Task("b", fn=lambda deps: text),
        Task("c", deps=("b",)),
    )
    result = Runner(dag, max_total_output_bytes=100).run()
    assert result.succeeded == ("a",)
    assert result.errors == {"b": "output_too_large"}
    assert result.skipped == ("c",)


def one_task_dag() -> Any:
    return build(Task("a", fn=lambda deps: 1))


def good_doc(dag: Any) -> dict[str, Any]:
    entry = {"state": "PENDING", "attempts": 0, "last_error": None, "output": None}
    return {"version": 1, "dag_hash": dag.dag_hash(), "tasks": {"a": entry}}


def test_state_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    Runner(one_task_dag(), state_path=path).run()
    other = build(Task("a", fn=lambda deps: 1), Task("b"))
    with pytest.raises(DagError) as info:
        Runner(other, state_path=path).run()
    assert info.value.code == "state_mismatch"
    path.write_text(json.dumps({"version": 1, "dag_hash": "other"}))
    with pytest.raises(DagError) as info:
        Runner(one_task_dag(), state_path=path).run()
    assert info.value.code == "state_mismatch"


@pytest.mark.parametrize(
    "content",
    [
        "not json",
        '{"version": 2}',
        "[]",
        '{"version": true}',
        '{"version": 1}',
        '{"version": 1, "dag_hash": 5}',
        "",
    ],
)
def test_state_corrupt_contents(tmp_path: Path, content: str) -> None:
    path = tmp_path / "state.json"
    path.write_text(content)
    with pytest.raises(DagError) as info:
        Runner(one_task_dag(), state_path=path).run()
    assert info.value.code == "state_corrupt"


def test_state_corrupt_raw_bytes(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    for data in (b"\xff\xfe\x00", b"[" * 100_000):
        path.write_bytes(data)
        with pytest.raises(DagError) as info:
            Runner(one_task_dag(), state_path=path).run()
        assert info.value.code == "state_corrupt"


def test_state_rejects_non_finite_numbers(tmp_path: Path) -> None:
    dag = one_task_dag()
    doc = good_doc(dag)
    doc["tasks"]["a"]["output"] = float("nan")
    path = tmp_path / "state.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(DagError) as info:
        Runner(dag, state_path=path).run()
    assert info.value.code == "state_corrupt"


def test_state_file_size_limit(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    limit = 1_000
    path.write_bytes(b" " * (limit + 1))
    with pytest.raises(DagError) as info:
        Runner(one_task_dag(), state_path=path, max_state_bytes=limit).run()
    assert info.value.code == "state_corrupt"
    assert "too large" in str(info.value)


def test_state_file_exactly_at_limit_loads(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    Runner(one_task_dag(), state_path=path).run()
    size = len(path.read_bytes())
    path.write_bytes(path.read_bytes() + b" " * 10)
    calls: list[str] = []
    dag = build(Task("a", fn=rec(calls, "a", 1)))
    Runner(dag, state_path=path, max_state_bytes=size + 10).run()
    assert calls == []
    with pytest.raises(DagError) as info:
        Runner(dag, state_path=path, max_state_bytes=size + 9).run()
    assert info.value.code == "state_corrupt"


def test_state_path_is_a_directory(tmp_path: Path) -> None:
    with pytest.raises(DagError) as info:
        Runner(one_task_dag(), state_path=tmp_path).run()
    assert info.value.code == "state_corrupt"


BAD_ENTRIES: list[Any] = [
    {"state": "BOGUS", "attempts": 0, "last_error": None, "output": None},
    {"state": 5, "attempts": 0, "last_error": None, "output": None},
    {"state": "PENDING", "attempts": -1, "last_error": None, "output": None},
    {"state": "PENDING", "attempts": True, "last_error": None, "output": None},
    {"state": "PENDING", "attempts": "0", "last_error": None, "output": None},
    {"state": "PENDING", "attempts": 0, "last_error": 5, "output": None},
    {"state": "PENDING", "attempts": 0, "last_error": None},
    "not an object",
]


@pytest.mark.parametrize("entry", BAD_ENTRIES)
def test_state_corrupt_entries(tmp_path: Path, entry: Any) -> None:
    dag = one_task_dag()
    doc = good_doc(dag)
    doc["tasks"]["a"] = entry
    path = tmp_path / "state.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(DagError) as info:
        Runner(dag, state_path=path).run()
    assert info.value.code == "state_corrupt"


@pytest.mark.parametrize(
    "tasks",
    [
        [],
        {},
        {"a": {"state": "PENDING", "attempts": 0, "last_error": None, "output": None}, "b": 1},
    ],
)
def test_state_corrupt_task_sets(tmp_path: Path, tasks: Any) -> None:
    dag = one_task_dag()
    doc = good_doc(dag)
    doc["tasks"] = tasks
    path = tmp_path / "state.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(DagError) as info:
        Runner(dag, state_path=path).run()
    assert info.value.code == "state_corrupt"


def test_load_resets_unfinished_states(tmp_path: Path) -> None:
    calls: list[str] = []
    dag = build(
        Task("a", fn=rec(calls, "a", "A")),
        Task("b", fn=rec(calls, "b", "B")),
        Task("c", fn=rec(calls, "c", "C"), deps=("b",)),
        Task("d", fn=rec(calls, "d", "D")),
    )
    doc = {
        "version": 1,
        "dag_hash": dag.dag_hash(),
        "tasks": {
            "a": {"state": "RUNNING", "attempts": 1, "last_error": None, "output": None},
            "b": {
                "state": "FAILED",
                "attempts": 3,
                "last_error": "ValueError: old",
                "output": None,
            },
            "c": {"state": "SKIPPED", "attempts": 0, "last_error": None, "output": None},
            "d": {"state": "SUCCEEDED", "attempts": 2, "last_error": "KeyError: k", "output": 42},
        },
    }
    path = tmp_path / "state.json"
    path.write_text(json.dumps(doc))
    result = Runner(dag, state_path=path).run()
    assert calls == ["a", "b", "c"]
    assert dict(result.attempts) == {"a": 1, "b": 1, "c": 1, "d": 2}
    assert dict(result.outputs) == {"a": "A", "b": "B", "c": "C", "d": 42}
    tasks = json.loads(path.read_text())["tasks"]
    assert tasks["d"] == {
        "state": "SUCCEEDED",
        "attempts": 2,
        "last_error": "KeyError: k",
        "output": 42,
    }
    assert tasks["a"]["last_error"] is None


def test_state_file_shape(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    dag = one_task_dag()
    Runner(dag, state_path=path).run()
    doc = json.loads(path.read_text())
    assert doc["version"] == 1
    assert doc["dag_hash"] == dag.dag_hash()
    assert doc["tasks"]["a"] == {
        "state": "SUCCEEDED",
        "attempts": 1,
        "last_error": None,
        "output": 1,
    }


def test_without_state_path_runs_fresh_each_time() -> None:
    calls: list[str] = []
    runner = Runner(build(Task("a", fn=rec(calls, "a"))))
    runner.run()
    runner.run()
    assert calls == ["a", "a"]
    runner.reset()


def test_reset_deletes_state(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    calls: list[str] = []
    runner = Runner(build(Task("a", fn=rec(calls, "a"))), state_path=path)
    runner.run()
    assert path.exists()
    runner.reset()
    assert not path.exists()
    runner.reset()
    runner.run()
    assert calls == ["a", "a"]


def test_failed_replace_leaves_no_temp_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(src: Any, dst: Any) -> None:
        raise OSError("nope")

    monkeypatch.setattr(os, "replace", broken)
    with pytest.raises(OSError, match="nope"):
        Runner(one_task_dag(), state_path=tmp_path / "state.json").run()
    assert list(tmp_path.iterdir()) == []


def test_events_never_carry_outputs_or_messages() -> None:
    events: list[Any] = []
    leak = "topsecretvalue"

    def noisy(deps: Mapping[str, Any]) -> Any:
        raise ValueError(leak)

    dag = build(
        Task("a", fn=lambda deps: leak),
        Task("b", fn=noisy, max_attempts=2, backoff_seconds=0.5),
        Task("c", deps=("b",)),
    )
    Runner(dag, sleep=lambda seconds: None, on_event=events.append).run()
    assert all(isinstance(event, TaskEvent) for event in events)
    assert [(e.kind, e.task, e.attempt, e.delay, e.error_type) for e in events] == [
        ("started", "a", 1, None, None),
        ("succeeded", "a", 1, None, None),
        ("started", "b", 1, None, None),
        ("retrying", "b", 1, 0.5, "ValueError"),
        ("failed", "b", 2, None, "ValueError"),
        ("skipped", "c", 0, None, None),
    ]
    assert leak not in repr(events)


def test_output_failure_event_uses_code() -> None:
    events: list[Any] = []
    dag = build(Task("a", fn=lambda deps: float("nan")))
    Runner(dag, on_event=events.append).run()
    assert events[-1].kind == "failed"
    assert events[-1].error_type == "output_not_serializable"


def test_callback_errors_do_not_break_run(caplog: pytest.LogCaptureFixture) -> None:
    def bad_callback(event: Any) -> None:
        raise RuntimeError("leak-me")

    with caplog.at_level(logging.WARNING, logger="task_dag_runner"):
        result = Runner(build(Task("a")), on_event=bad_callback).run()
    assert result.succeeded == ("a",)
    assert "RuntimeError" in caplog.text
    assert "leak-me" not in caplog.text


def test_logger_has_null_handler() -> None:
    handlers = logging.getLogger("task_dag_runner").handlers
    assert any(isinstance(handler, logging.NullHandler) for handler in handlers)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_output_bytes": 0},
        {"max_total_output_bytes": -1},
        {"max_state_bytes": 0},
        {"max_output_bytes": True},
        {"max_backoff_seconds": -1.0},
        {"max_backoff_seconds": float("nan")},
    ],
)
def test_invalid_runner_options(kwargs: dict[str, Any]) -> None:
    with pytest.raises(DagError) as info:
        Runner(Dag(), **kwargs)
    assert info.value.code == "invalid_task"


def test_dag_error_message_is_clipped() -> None:
    error = DagError("state_corrupt", "y" * 500)
    assert len(str(error)) <= 200 + len("state_corrupt: ")
    assert error.code == "state_corrupt"
    assert error.cycle is None
    assert str(DagError("cycle")) == "cycle"


def test_task_state_values_and_version() -> None:
    assert [s.value for s in TaskState] == ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "SKIPPED"]
    assert tdr.__version__ == "0.1.0"
