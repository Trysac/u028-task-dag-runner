"""Run Python callables as a dependency graph with bounded retries and resumable state.

The public API is :class:`Task`, :class:`Dag`, :class:`Runner`, :class:`RunResult`,
:class:`TaskEvent`, :class:`TaskState` and :class:`DagError`.
"""

from __future__ import annotations

import contextlib
import enum
import hashlib
import heapq
import json
import logging
import math
import os
import re
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

__all__ = [
    "MAX_TASKS",
    "Dag",
    "DagError",
    "RunResult",
    "Runner",
    "Task",
    "TaskEvent",
    "TaskState",
    "__version__",
]

__version__ = "0.1.0"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

MAX_TASKS = 10_000
_MAX_ATTEMPTS = 100
_MAX_MESSAGE_CHARS = 200
_STATE_VERSION = 1
_ON_PATH = 1
_DONE = 2
_NAME_RE = re.compile(r"[A-Za-z0-9_.-]{1,64}")

ErrorCode = Literal[
    "cycle",
    "unknown_dependency",
    "duplicate_task",
    "invalid_task",
    "too_many_tasks",
    "state_mismatch",
    "state_corrupt",
]
EventKind = Literal["started", "succeeded", "retrying", "failed", "skipped"]
TaskFn = Callable[[Mapping[str, Any]], Any]


class DagError(Exception):
    """Base error of the library.

    ``code`` is a stable machine-readable identifier. ``cycle`` is set for ``cycle`` errors.
    Messages are clipped to 200 characters and never contain task outputs.
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str = "",
        cycle: list[str] | None = None,
    ) -> None:
        clipped = message[:_MAX_MESSAGE_CHARS]
        super().__init__(f"{code}: {clipped}" if clipped else code)
        self.code: ErrorCode = code
        self.message = clipped
        self.cycle = cycle


class TaskState(enum.Enum):
    """Lifecycle state of a task."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


_STATE_NAMES = frozenset(state.value for state in TaskState)
_BLOCKING = (TaskState.FAILED, TaskState.SKIPPED)


def _show(value: object) -> str:
    if isinstance(value, str):
        return repr(value[:40])
    return f"<{type(value).__name__}>"


def _valid_name(name: object) -> bool:
    return isinstance(name, str) and _NAME_RE.fullmatch(name) is not None


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_non_negative(value: object) -> bool:
    return (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _noop(deps: Mapping[str, Any]) -> Any:
    return None


@dataclass(frozen=True)
class Task:
    """One unit of work.

    ``fn`` receives a read-only mapping ``{dep: output}`` of the direct dependencies only.
    Its return value must be JSON serialisable. ``max_attempts`` is 1 to 100.
    """

    name: str
    fn: TaskFn = _noop
    deps: tuple[str, ...] = ()
    max_attempts: int = 1
    backoff_seconds: float = 0.0

    def __post_init__(self) -> None:
        if not _valid_name(self.name):
            raise DagError("invalid_task", f"invalid task name: {_show(self.name)}")
        if not callable(self.fn):
            raise DagError("invalid_task", f"task {self.name}: fn must be callable")
        if not isinstance(self.deps, tuple) or len(self.deps) > MAX_TASKS:
            raise DagError("invalid_task", f"task {self.name}: deps must be a short tuple")
        for dep in self.deps:
            if not _valid_name(dep):
                raise DagError(
                    "invalid_task",
                    f"task {self.name}: invalid dependency name {_show(dep)}",
                )
        if not _is_int(self.max_attempts) or not 1 <= self.max_attempts <= _MAX_ATTEMPTS:
            raise DagError("invalid_task", f"task {self.name}: max_attempts must be 1 to 100")
        if not _is_finite_non_negative(self.backoff_seconds):
            raise DagError(
                "invalid_task",
                f"task {self.name}: backoff_seconds must be finite and >= 0",
            )


class Dag:
    """A set of tasks with dependency edges."""

    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}

    def __len__(self) -> int:
        return len(self._tasks)

    @property
    def tasks(self) -> Mapping[str, Task]:
        """Read-only view of the tasks by name."""
        return MappingProxyType(self._tasks)

    def add(self, task: Task) -> None:
        """Add ``task``; raises ``duplicate_task``, ``invalid_task`` or ``too_many_tasks``."""
        if not isinstance(task, Task):
            raise DagError("invalid_task", "add() expects a Task")
        if task.name in self._tasks:
            raise DagError("duplicate_task", f"task {task.name} is already defined")
        if len(self._tasks) >= MAX_TASKS:
            raise DagError("too_many_tasks", f"a DAG holds at most {MAX_TASKS} tasks")
        self._tasks[task.name] = task

    def dag_hash(self) -> str:
        """Return the SHA-256 hex digest of names, dependencies and max_attempts."""
        rows: list[list[Any]] = []
        for task in self._tasks.values():
            rows.append([task.name, sorted(task.deps), task.max_attempts])
        rows.sort(key=lambda row: row[0])
        payload = json.dumps(rows, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()

    def validate(self) -> list[str]:
        """Check the graph and return the deterministic execution order.

        Raises ``unknown_dependency`` or ``cycle``. The order is Kahn's algorithm with a heap
        of ready names, so it depends only on the graph and never on insertion order.
        """
        tasks = self._tasks
        for name in sorted(tasks):
            for dep in sorted(set(tasks[name].deps)):
                if dep not in tasks:
                    raise DagError(
                        "unknown_dependency",
                        f"task {name} depends on unknown task {dep}",
                    )
        self._check_cycles()
        return self._topological_order()

    def _check_cycles(self) -> None:
        tasks = self._tasks
        color: dict[str, int] = {}
        for root in sorted(tasks):
            if root in color:
                continue
            color[root] = _ON_PATH
            path = [root]
            stack: list[Iterator[str]] = [iter(sorted(set(tasks[root].deps)))]
            while stack:
                descended = False
                for dep in stack[-1]:
                    seen = color.get(dep)
                    if seen is None:
                        color[dep] = _ON_PATH
                        path.append(dep)
                        stack.append(iter(sorted(set(tasks[dep].deps))))
                        descended = True
                        break
                    if seen == _ON_PATH:
                        cycle = [*path[path.index(dep) :], dep]
                        raise DagError(
                            "cycle",
                            "dependency cycle: " + " -> ".join(cycle),
                            cycle=cycle,
                        )
                if not descended:
                    color[path.pop()] = _DONE
                    stack.pop()

    def _topological_order(self) -> list[str]:
        tasks = self._tasks
        pending: dict[str, int] = {}
        dependents: dict[str, list[str]] = {name: [] for name in tasks}
        for name, task in tasks.items():
            unique = set(task.deps)
            pending[name] = len(unique)
            for dep in unique:
                dependents[dep].append(name)
        ready = [name for name, count in pending.items() if count == 0]
        heapq.heapify(ready)
        order: list[str] = []
        while ready:
            name = heapq.heappop(ready)
            order.append(name)
            for child in dependents[name]:
                pending[child] -= 1
                if pending[child] == 0:
                    heapq.heappush(ready, child)
        return order


@dataclass(frozen=True)
class TaskEvent:
    """Progress notification. Never carries outputs or exception messages."""

    kind: EventKind
    task: str
    attempt: int
    delay: float | None = None
    error_type: str | None = None


@dataclass(frozen=True)
class RunResult:
    """Outcome of :meth:`Runner.run`.

    ``errors`` maps a failed task to ``exception``, ``output_not_serializable`` or
    ``output_too_large``. ``outputs`` hold JSON-normalised values.
    """

    succeeded: tuple[str, ...]
    failed: tuple[str, ...]
    skipped: tuple[str, ...]
    attempts: Mapping[str, int]
    outputs: Mapping[str, Any]
    errors: Mapping[str, str]


def _reject_constant(name: str) -> Any:
    raise ValueError("non-finite number in state file")


def _message_of(exc: Exception) -> str:
    try:
        return str(exc)
    except Exception:
        return "<unprintable>"


def _positive_int(label: str, value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise DagError("invalid_task", f"{label} must be a positive integer")
    return value


def _atomic_write(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` through a temp file in the same folder and os.replace."""
    handle, temp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temp_name)
        raise


class _Execution:
    """Mutable bookkeeping for one call of :meth:`Runner.run`."""

    def __init__(self, path: Path | None, dag_hash: str, names: list[str]) -> None:
        self.path = path
        self.dag_hash = dag_hash
        self.names = sorted(names)
        self.state: dict[str, TaskState] = dict.fromkeys(names, TaskState.PENDING)
        self.attempts: dict[str, int] = dict.fromkeys(names, 0)
        self.last_error: dict[str, str | None] = dict.fromkeys(names)
        self.ser: dict[str, str] = {}
        self.errors: dict[str, str] = {}
        self.total = 0

    def persist(self) -> None:
        if self.path is None:
            return
        rows: list[str] = []
        for name in self.names:
            state = self.state[name]
            output = self.ser[name] if state is TaskState.SUCCEEDED else "null"
            rows.append(
                f'{json.dumps(name)}:{{"state":{json.dumps(state.value)},'
                f'"attempts":{self.attempts[name]},'
                f'"last_error":{json.dumps(self.last_error[name])},"output":{output}}}'
            )
        body = ",".join(rows)
        text = f'{{"version":{_STATE_VERSION},"dag_hash":"{self.dag_hash}","tasks":{{{body}}}}}'
        _atomic_write(self.path, text)

    def result(self) -> RunResult:
        def names_in(state: TaskState) -> tuple[str, ...]:
            return tuple(n for n in self.names if self.state[n] is state)

        succeeded = names_in(TaskState.SUCCEEDED)
        return RunResult(
            succeeded=succeeded,
            failed=names_in(TaskState.FAILED),
            skipped=names_in(TaskState.SKIPPED),
            attempts=MappingProxyType({n: self.attempts[n] for n in self.names}),
            outputs=MappingProxyType({n: json.loads(self.ser[n]) for n in succeeded}),
            errors=MappingProxyType({n: self.errors[n] for n in self.names if n in self.errors}),
        )


class Runner:
    """Execute a :class:`Dag` sequentially in deterministic order.

    With ``state_path`` every transition is written atomically so a crashed or interrupted
    run resumes where it stopped. Execution is single-threaded and the state file is not
    locked across processes: do not run two Runners on the same file at the same time.
    Choose ``max_state_bytes`` larger than ``max_total_output_bytes`` plus overhead.
    """

    def __init__(
        self,
        dag: Dag,
        state_path: Path | None = None,
        sleep: Callable[[float], None] = time.sleep,
        max_output_bytes: int = 65_536,
        max_total_output_bytes: int = 16_777_216,
        max_state_bytes: int = 67_108_864,
        max_backoff_seconds: float = 300.0,
        on_event: Callable[[TaskEvent], None] | None = None,
    ) -> None:
        self._dag = dag
        self._state_path = state_path
        self._sleep = sleep
        self._max_output = _positive_int("max_output_bytes", max_output_bytes)
        self._max_total = _positive_int("max_total_output_bytes", max_total_output_bytes)
        self._max_state = _positive_int("max_state_bytes", max_state_bytes)
        if not _is_finite_non_negative(max_backoff_seconds):
            raise DagError("invalid_task", "max_backoff_seconds must be finite and >= 0")
        self._max_backoff = float(max_backoff_seconds)
        self._on_event = on_event

    def reset(self) -> None:
        """Delete the state file, if any, so the next run starts from scratch."""
        if self._state_path is not None:
            self._state_path.unlink(missing_ok=True)

    def run(self) -> RunResult:
        """Validate the DAG, load any state and execute every task that is not SUCCEEDED.

        ``KeyboardInterrupt`` and other ``BaseException`` values propagate after the state
        records the running task as PENDING.
        """
        order = self._dag.validate()
        tasks = self._dag.tasks
        ex = _Execution(self._state_path, self._dag.dag_hash(), order)
        for name, (attempts, text, last_error) in self._load_state(ex.dag_hash, order).items():
            ex.state[name] = TaskState.SUCCEEDED
            ex.attempts[name] = attempts
            ex.ser[name] = text
            ex.last_error[name] = last_error
            ex.total += len(text)
        for name in order:
            task = tasks[name]
            if ex.state[name] is TaskState.SUCCEEDED:
                continue
            if any(ex.state[dep] in _BLOCKING for dep in task.deps):
                self._skip(ex, name)
            else:
                self._run_task(ex, task)
        return ex.result()

    def _emit(
        self,
        kind: EventKind,
        task: str,
        attempt: int,
        delay: float | None = None,
        error_type: str | None = None,
    ) -> None:
        if self._on_event is None:
            return
        try:
            self._on_event(TaskEvent(kind, task, attempt, delay, error_type))
        except Exception as exc:
            logger.warning("on_event callback raised %s", type(exc).__name__)

    def _skip(self, ex: _Execution, name: str) -> None:
        ex.state[name] = TaskState.SKIPPED
        ex.persist()
        self._emit("skipped", name, 0)

    def _abort(self, ex: _Execution, name: str) -> None:
        ex.state[name] = TaskState.PENDING
        with contextlib.suppress(OSError):
            ex.persist()

    def _fail(
        self,
        ex: _Execution,
        name: str,
        attempt: int,
        code: str,
        error_type: str,
        last_error: str,
    ) -> None:
        ex.state[name] = TaskState.FAILED
        ex.errors[name] = code
        ex.last_error[name] = last_error
        ex.persist()
        self._emit("failed", name, attempt, error_type=error_type)

    def _run_task(self, ex: _Execution, task: Task) -> None:
        name = task.name
        ex.state[name] = TaskState.RUNNING
        ex.attempts[name] = 0
        ex.persist()
        self._emit("started", name, 1)
        attempt = 0
        while True:
            attempt += 1
            ex.attempts[name] = attempt
            view = MappingProxyType(
                {dep: json.loads(ex.ser[dep]) for dep in sorted(set(task.deps))}
            )
            try:
                raw = task.fn(view)
            except Exception as exc:
                error_type = type(exc).__name__
                last_error = f"{error_type}: {_message_of(exc)[:_MAX_MESSAGE_CHARS]}"
            except BaseException:
                self._abort(ex, name)
                raise
            else:
                self._store_output(ex, name, attempt, raw)
                return
            if attempt >= task.max_attempts:
                self._fail(ex, name, attempt, "exception", error_type, last_error)
                return
            delay = float(min(self._max_backoff, task.backoff_seconds * 2 ** (attempt - 1)))
            self._emit("retrying", name, attempt, delay=delay, error_type=error_type)
            if delay > 0:
                try:
                    self._sleep(delay)
                except BaseException:
                    self._abort(ex, name)
                    raise

    def _store_output(self, ex: _Execution, name: str, attempt: int, raw: Any) -> None:
        try:
            text = json.dumps(raw, allow_nan=False)
            json.loads(text)
        except (TypeError, ValueError, RecursionError):
            code = "output_not_serializable"
            self._fail(ex, name, attempt, code, code, code)
            return
        size = len(text.encode())
        if size > self._max_output or ex.total + size > self._max_total:
            code = "output_too_large"
            self._fail(ex, name, attempt, code, code, code)
            return
        ex.ser[name] = text
        ex.total += size
        ex.state[name] = TaskState.SUCCEEDED
        ex.persist()
        self._emit("succeeded", name, attempt)

    def _load_state(
        self, dag_hash: str, names: list[str]
    ) -> dict[str, tuple[int, str, str | None]]:
        path = self._state_path
        if path is None:
            return {}
        try:
            with path.open("rb") as handle:
                data = handle.read(self._max_state + 1)
        except FileNotFoundError:
            return {}
        except OSError:
            raise DagError("state_corrupt", "state file cannot be read") from None
        if len(data) > self._max_state:
            raise DagError("state_corrupt", "state file is too large")
        try:
            doc = json.loads(data, parse_constant=_reject_constant)
        except (ValueError, RecursionError):
            raise DagError("state_corrupt", "state file is not valid JSON") from None
        if not isinstance(doc, dict) or not _is_int(doc.get("version")):
            raise DagError("state_corrupt", "state file has an unknown shape")
        if doc["version"] != _STATE_VERSION:
            raise DagError("state_corrupt", "unsupported state file version")
        stored_hash = doc.get("dag_hash")
        if not isinstance(stored_hash, str):
            raise DagError("state_corrupt", "state file has no dag_hash")
        if stored_hash != dag_hash:
            raise DagError("state_mismatch", "state file belongs to a different DAG")
        stored = doc.get("tasks")
        if not isinstance(stored, dict) or set(stored) != set(names):
            raise DagError("state_corrupt", "state file tasks do not match the DAG")
        loaded: dict[str, tuple[int, str, str | None]] = {}
        for name in names:
            parsed = _parse_entry(stored[name])
            if parsed is not None:
                loaded[name] = parsed
        return loaded


def _parse_entry(entry: object) -> tuple[int, str, str | None] | None:
    """Validate one stored task; return its data only when it is SUCCEEDED."""
    if not isinstance(entry, dict) or "output" not in entry:
        raise DagError("state_corrupt", "state file task entry is malformed")
    state = entry.get("state")
    attempts = entry.get("attempts")
    last_error = entry.get("last_error")
    if not isinstance(state, str) or state not in _STATE_NAMES:
        raise DagError("state_corrupt", "state file task has an unknown state")
    if not isinstance(attempts, int) or isinstance(attempts, bool) or attempts < 0:
        raise DagError("state_corrupt", "state file task has invalid attempts")
    if last_error is not None and not isinstance(last_error, str):
        raise DagError("state_corrupt", "state file task has an invalid last_error")
    if state != TaskState.SUCCEEDED.value:
        return None
    try:
        text = json.dumps(entry["output"], allow_nan=False)
    except (TypeError, ValueError, RecursionError):
        raise DagError("state_corrupt", "state file task output is invalid") from None
    return attempts, text, last_error
