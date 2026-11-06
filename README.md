# task-dag-runner

[![PyPI version](https://img.shields.io/pypi/v/trysac-task-dag-runner)](https://pypi.org/project/trysac-task-dag-runner/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-task-dag-runner)](LICENSE)
[![CI](https://github.com/Trysac/u028-task-dag-runner/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u028-task-dag-runner/actions/workflows/ci.yml)

Run Python tasks in a dependency graph with bounded retries, injected sleep, deterministic order and resume from an atomic JSON state file.

## Problem

Agent pipelines and batch jobs are often a handful of dependent steps where one flaky step forces a full rerun. Airflow or Prefect are too heavy for a script, and hand-written loops forget cycle detection, retries and resumption. This library runs a small DAG of callables with retries and an on-disk JSON state so a crashed run resumes where it stopped.

## Install

```sh
pip install trysac-task-dag-runner
# or
uv add trysac-task-dag-runner
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
from task_dag_runner import Dag, Runner, Task

dag = Dag()
dag.add(Task("fetch", fn=lambda deps: [1, 2, 3], max_attempts=3, backoff_seconds=1.0))
dag.add(Task("total", fn=lambda deps: sum(deps["fetch"]), deps=("fetch",)))
result = Runner(dag).run()
print(result.outputs["total"])  # 6
print(result.succeeded)  # ('fetch', 'total')
```

## Public API

- `Task(name, fn=..., deps=(), max_attempts=1, backoff_seconds=0.0)`: frozen dataclass. Names match `^[A-Za-z0-9_.-]{1,64}$`; `max_attempts` is 1 to 100; `backoff_seconds` is finite and >= 0.
- `Dag()`: `add(task)`, `validate()` returns the deterministic execution order, `dag_hash()`, `tasks`, `len()`.
- `Runner(dag, state_path=None, sleep=time.sleep, max_output_bytes=65_536, max_total_output_bytes=16_777_216, max_state_bytes=67_108_864, max_backoff_seconds=300.0, on_event=None)`: `run()` returns a `RunResult`; `reset()` deletes the state file.
- `RunResult`: `succeeded`, `failed`, `skipped`, `attempts`, `outputs`, `errors`. Failure codes are `exception`, `output_not_serializable` and `output_too_large`.
- `TaskEvent`, `TaskState`, `MAX_TASKS` (10,000).
- `DagError(code, message, cycle)`. Codes: `cycle`, `unknown_dependency`, `duplicate_task`, `invalid_task`, `too_many_tasks`, `state_mismatch`, `state_corrupt`.

Outputs are normalised through JSON: tuples become lists and integer keys become strings. Output failures are never retried. Only `Exception` is retried; `BaseException` propagates after the running task is recorded as PENDING.

## Limits

Tasks run sequentially: there is no parallel or async execution, no distributed workers and no scheduling. The state file has no cross-process locking, so do not run two runners on the same file at once. Choose `max_state_bytes` larger than `max_total_output_bytes` plus overhead.

## Why not graphlib?

`graphlib` only orders nodes; it does not run tasks, retry them or persist state. Prefect needs a server.

## Security

See [docs/SECURITY.md](docs/SECURITY.md).

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
