# Architecture

## Purpose

`trysac-task-dag-runner` is a small, dependency-free Python library that runs Python callables as a dependency graph with bounded retries and resumable JSON state.

## Shape

```text
caller code --> task_dag_runner (public API in __init__.py)
                    |
                    +-- Task, Dag (pure graph logic)
                    +-- Runner (execution, retries, state file I/O)
```

- **Module**: a single module, `src/task_dag_runner/__init__.py`.
- **Dag**: stores tasks by name; `validate()` checks unknown dependencies, detects cycles with an iterative DFS and returns a deterministic order from Kahn's algorithm with a heap.
- **Runner**: validates the DAG, loads the state file (if any), runs the pending tasks sequentially in order, normalises outputs through JSON and writes the state file atomically (temp file in the same folder, `os.replace`) after each transition.
- **Errors**: `DagError` with a stable `code` attribute. Messages are clipped to 200 characters.
- **Logging**: a module logger with a `NullHandler`; the callback failure is logged as a warning.

## Quality contract

See [TESTING.md](TESTING.md) for the test suite and the coverage policy.
