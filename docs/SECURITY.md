# Security notes

## Threat model

`trysac-task-dag-runner` has no network access and no runtime dependencies. It does perform local file I/O for the optional state file.

Defended:

- **Untrusted state file contents**: the file is read with at most `max_state_bytes` bytes, parsed with `json.loads` only (no `eval`, no pickle), `NaN` and `Infinity` constants are rejected, and the version, shape, `dag_hash` and task set are validated. Invalid content raises `DagError` with code `state_corrupt` or `state_mismatch`.
- **Oversized outputs**: each output is limited by `max_output_bytes` and the total by `max_total_output_bytes`; violations fail the task with `output_too_large`.
- **Non-serialisable outputs**: outputs that JSON cannot encode, or that contain NaN or infinity, fail with `output_not_serializable`.
- **Partial writes**: the state file is replaced atomically, so a crash leaves either the old or the new file.
- **Information leakage through events and errors**: `TaskEvent` never carries outputs or exception messages; `DagError` messages are clipped to 200 characters and never contain outputs.
- **Invalid input**: names, dependencies, attempts, backoff and sizes are validated, and invalid values raise `DagError`.

Not defended:

- **Concurrent runners**: the state file has no cross-process lock. Two runners on the same file can overwrite each other's transitions.
- **Task code**: the library runs arbitrary callables supplied by the caller with the caller's privileges; it does not sandbox them.
- **Secrets in state**: task outputs are stored in plain JSON in the state file; the library does not encrypt or redact them.
- **Exception messages in state**: `last_error` stores the exception type and the first 200 characters of its message in the state file.
- **Filesystem attacks**: the library does not check permissions or symlinks of the state path.

## Publishing

Releases are published to PyPI with trusted publishing (OIDC) when available, never with a long-lived token stored in the repository.

To report a vulnerability, see [SECURITY.md](../SECURITY.md).
