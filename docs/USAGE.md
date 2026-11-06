# Usage

`task_dag_runner` runs a small graph of Python callables sequentially, with bounded retries and an
optional atomic JSON state file so an interrupted run resumes where it stopped.

```python
from pathlib import Path

from task_dag_runner import Dag, Runner, Task

dag = Dag()
dag.add(Task("fetch", fn=lambda deps: [1, 2, 3], max_attempts=3, backoff_seconds=1.0))
dag.add(Task("total", fn=lambda deps: sum(deps["fetch"]), deps=("fetch",)))
result = Runner(dag, state_path=Path("state.json")).run()
print(result.outputs["total"])
```

## Behaviour

- `Dag.validate()` returns the execution order: Kahn's algorithm with a heap of ready names, so the
  order depends only on the graph. It raises `DagError` with code `unknown_dependency` or `cycle`
  (`DagError.cycle` lists the cycle path, closed with its first node).
- A task function receives a read-only mapping `{dep: output}` of its direct dependencies.
- Outputs are serialised with `json.dumps(allow_nan=False)` and parsed again, so a fresh run and a
  resumed run see identical values (tuples become lists, integer keys become strings).
  Failures are `output_not_serializable` or `output_too_large` and are never retried.
- The delay before attempt `n + 1` is `min(max_backoff_seconds, backoff_seconds * 2 ** (n - 1))`.
  The injected `sleep` is never called with `0`.
- `Exception` is retried; `KeyboardInterrupt` and other `BaseException` values propagate after the
  state records the task as `PENDING`.
- State is written after every transition to a temp file in the same folder, then `os.replace`d.
  On load, `RUNNING`, `FAILED` and `SKIPPED` tasks become `PENDING` with attempts reset to 0;
  `SUCCEEDED` tasks are never run or rewritten.

## Limits

| Limit | Default |
|---|---|
| Tasks per DAG | 10,000 |
| Task name | `^[A-Za-z0-9_.-]{1,64}$` |
| `max_attempts` | 1 to 100 |
| `max_output_bytes` | 65,536 |
| `max_total_output_bytes` | 16,777,216 |
| `max_state_bytes` | 67,108,864 |
| `max_backoff_seconds` | 300.0 |

Choose `max_state_bytes` larger than `max_total_output_bytes` plus overhead.

## Events

`on_event` receives `TaskEvent(kind, task, attempt, delay, error_type)`. Events never contain task
outputs or exception messages. Exceptions raised by the callback are logged by type name and ignored.

## Out of scope

Parallel or async execution, distributed workers, scheduling and cross-process locking of the state
file. Do not run two runners on the same state file at the same time.
