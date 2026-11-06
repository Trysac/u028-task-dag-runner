# Sequential DAG runner with JSON-normalised outputs and atomic state file

Status: accepted (generated, pending human review)

## Context

Agent pipelines and batch jobs are small graphs of dependent steps where one flaky step should not force a full rerun. graphlib only orders nodes and workflow engines such as Prefect need a server. A resumed run must behave exactly like a fresh run, and untrusted or oversized data must be bounded.

## Decision

Implement a standard-library-only runner that executes tasks sequentially in a deterministic order (Kahn with a heap of ready names, independent of insertion order). Every output is serialised with json.dumps(allow_nan=False), size-checked per output and in total, and parsed again, so dependents, the result and the state file always hold the same JSON-normalised value; output failures are permanent. Every state transition is written atomically (temp file in the same folder plus os.replace) and keyed by a SHA-256 of names, dependencies and max_attempts. SUCCEEDED is terminal; RUNNING, FAILED and SKIPPED reload as PENDING. Retries catch Exception only and BaseException propagates after the task is recorded as PENDING. Failure codes and DagError codes are stable strings and events never carry outputs or messages.

## Consequences

Resume is exact and crash-safe for one process, and the library has no runtime dependencies. Execution is single-threaded and the state file is not locked across processes, so concurrent runners on one file are unsupported. State is rewritten on every transition, which costs O(tasks) writes of up to the output cap; this is acceptable for the 10,000-task limit but not intended for large workloads. Outputs must be JSON serialisable.
