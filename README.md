# task-dag-runner

[![PyPI version](https://img.shields.io/pypi/v/trysac-task-dag-runner)](https://pypi.org/project/trysac-task-dag-runner/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-task-dag-runner)](LICENSE)
[![CI](https://github.com/Trysac/u028-task-dag-runner/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u028-task-dag-runner/actions/workflows/ci.yml)

Run Python tasks in a dependency graph with bounded retries, injected sleep, deterministic order and resume from an atomic JSON state file

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-task-dag-runner
# or
uv add trysac-task-dag-runner
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import task_dag_runner

# normalize() trims surrounding whitespace and lower-cases the text.
print(task_dag_runner.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    task_dag_runner.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
