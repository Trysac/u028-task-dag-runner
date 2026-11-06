# Testing

## Running

```sh
uv run pytest --cov=task_dag_runner   # tests with coverage report
uv run python scripts/check.py        # gates as CI runs them
```

## Test suite

The suite is `tests/test_task_dag_runner.py`. It exercises the public API through `import task_dag_runner` and covers:

- deterministic execution order for a graph with insertion-order independence;
- cycle, unknown dependency, duplicate task and invalid task errors;
- retries with injected sleep, exponential backoff and the `max_backoff_seconds` cap;
- failure propagation to `failed` and `skipped` tasks;
- resume from the state file after `KeyboardInterrupt`, and idempotent re-runs;
- JSON normalisation of outputs, and the `output_not_serializable` and `output_too_large` codes;
- state file rejection (`state_mismatch`, `state_corrupt`);
- event payloads that carry no outputs or messages.

## Policy

- Branch coverage of `src/task_dag_runner` must stay at or above 80 %.
- Each bug fix starts with a failing test that reproduces it.
- Tests are deterministic: no network, injected sleep, no dependence on test order.

## Layout

`tests/test_task_dag_runner.py` is the only test module. Shared fixtures are not used.
