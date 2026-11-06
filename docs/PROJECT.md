# Project record

## Objective

Run Python tasks in a dependency graph with bounded retries, injected sleep, deterministic order and resume from an atomic JSON state file, so a flaky step in a small pipeline does not force a full rerun.

## Scope

- In scope: the public API documented in [README.md](../README.md): `Task`, `Dag`, `Runner`, `RunResult`, `TaskEvent`, `TaskState`, `DagError`; sequential execution; retries with capped exponential backoff; JSON-normalised outputs with size limits; atomic JSON state file with resume.
- Out of scope: parallel or async execution, distributed workers, scheduling, cross-process state locking, and any runtime dependency without an accepted ADR.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created from the factory utility template. |

## Effort metrics

Measured values only; a value that has not been measured stays "pending measurement".

| Metric | Value |
|---|---|
| Tokens (input / output) | 458338 / 85145 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.7342 |

## Factory run u028 (measured from API usage)

| Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|
| generation | claude-sonnet-5-5 | yes | 2790 | 73267 | 0 | 4246 | 0.3695 |
| fix-1 | claude-sonnet-5-5 | no | 4 | 1937 | 9299 | 25862 | 0.0452 |
| fix-2 | claude-sonnet-5-5 | no | 6 | 515 | 6863 | 40785 | 0.0264 |
| fix-3 | claude-sonnet-5-5 | no | 8 | 1078 | 7373 | 55997 | 0.0348 |
| audit | claude-sonnet-5-5 | no | 12 | 2582 | 42222 | 159815 | 0.1474 |
| fix-4 | claude-sonnet-5-5 | no | 6 | 1198 | 7761 | 40883 | 0.0355 |
| docs | claude-haiku-5-5 | no | 127924 | 4568 | 0 | 0 | 0.0754 |

Total: $0.7342. Prices: Anthropic list prices.
