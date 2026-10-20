# Chaos test report (2026-10-20 11:04)

- tasks: 100,000 unique (+5,000 re-sent with the same idempotency key)
- workers: 8, ledger guard: dedupe, redis faults: on, seed: 42
- chaos phase: 357s, total: 358s, throughput: 280 tasks/s
- final states: {'completed': 100000}

## Faults injected

- bounce: 38
- kill: 78
- pause: 44
- stop: 131
- term: 54
- total: 345

## What the system had to deal with (from worker logs)

- duplicate charges blocked by the ledger: 316
- stale charges blocked by the ledger: 0
- stale acks rejected (fencing token): 412
- leases lost while working: 109
- tasks reaped after a lease expired: 368
- tasks released on graceful shutdown: 6
- handler failures (FAIL_RATE): 1,055
- redis unavailable, worker retried: 699

## Invariants

- PASS  Liveness: queues drained after faults stopped: ready+leased+delayed = 0
- PASS  Safety: no task lost: 0 task(s) neither completed nor dead []
- PASS  Safety: no task charged twice: 0 []
- PASS  Safety: every completed task charged exactly once: 0 violation(s) []
- PASS  Safety: re-sent tasks created nothing: 100000 created for 100000 unique keys
- PASS  Non-vacuous: duplicate executions actually happened and were caught: 728

**ALL INVARIANTS HELD**
