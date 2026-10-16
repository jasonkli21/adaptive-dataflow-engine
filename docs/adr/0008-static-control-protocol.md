# ADR-0008: Private bounded TCP control protocol for static workers

**Status:** Accepted\
**Date:** 2026-10-15

## Context

Phase 3 needs one coordinator, 2–4 workers and explicit stage barriers. Architecture
sections 7, 24, 26 and INV-1/4/5/6 require stable identity, bounded communication,
frontend independence and observable failure. An RPC framework is not otherwise
needed by the synchronous local runtime.

## Decision

Keep one cohesive `ade-distributed` crate. Use a separate blocking TCP control
listener with `ADETCP01` magic/version, little-endian UInt32 length, at most 1 MiB
of strict serde JSON, and an echoed request ID. Requests are Ping, PrepareJob,
RunProducerTask, RunFinalTask, CancelJob and Shutdown. Unknown fields/variants,
versions, oversized/truncated messages and mismatched response IDs fail. Replies
carry typed task metrics or a structured code/message error. No credentials.

PrepareJob installs the complete expected topology before producers can send.
Job, worker, task, partition, exchange and attempt IDs are numeric newtypes;
attempt zero is required. Producer tasks are worker indices; final task IDs are
worker count plus worker index. Files are assigned in manifest order round robin.
Task identity is independent of PID/connection. Only one job/task runs per worker.

The listener is nonblocking but accepted sockets are explicitly blocking (macOS
inherits O_NONBLOCK). At most eight control handlers exist; CancelJob can run
while the task handler is active. Socket inactivity/connect timeout is five
seconds. Control reads/writes also have five-second total deadlines. Task RPC has a configurable total deadline (default 120 seconds), enforced
across individual reads/writes; a coordinator stage also has a total deadline.
CancelJob signals the public local cancellation token and releases job ownership;
active task/receiver references retain their temporary root until teardown.
Every failure fails the whole job, cancels survivors best effort and returns no
partial result. There is no retry. Local parents own child handles, use bounded
pipe readiness after sockets bind, and kill/wait before removing their temp root.

## Alternatives and consequences

Tokio/tonic/Flight would add an execution/framework migration without removing the
small static state machine. This private protocol adds no new resolved package.
It is intentionally not a public compatibility commitment. No authentication,
TLS, multi-job arbitration, heartbeat membership or scheduler. A public Rust
coordinator API suffices; a separate coordinator CLI is deferred.

## Architecture impact and follow-up

Refines architecture sections 7/24/26 and INV-1/4/5/6 for the static batch phase.
Operators remain in `ade-exec`; serialization contains no operator implementation.
The synchronous pull model of ADR-0005 is preserved. Phase 4 may replace control
encoding/framework when additional mechanisms justify it. Blocking filesystem
operations are not preemptible; cooperative cancellation and finite network waits
are not process-level CPU/I/O isolation.
