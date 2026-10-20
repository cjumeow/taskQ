# taskQ

A Redis-backed task queue with leases, heartbeats, fencing tokens, retries with exponential backoff, delayed tasks, a dead letter queue, and idempotent enqueue. Every multi-key operation is an atomic Lua script. Chaos-tested with 100k tasks under worker crashes and Redis outages.

The worker runs a simulated handler (sleep, optional random failure, optional ledger charge). The point of the project is the delivery machinery around it, not a pluggable handler API.

## Guarantees

| | |
|---|---|
| **Delivery** | At-least-once. A task is never lost: it ends up `completed` or in the DLQ. |
| **Effect** | Exactly-once *effect* when the side effect is idempotent. The demo ledger achieves this with the `dedupe` guard. |
| **Not guaranteed** | Exactly-once *execution*. A paused or partitioned worker can run a task that another worker also runs. Handlers must tolerate this. |
| **Durability** | Redis AOF with `appendfsync everysec`: a Redis crash can lose up to ~1s of writes. |

Chaos test result ([full report](docs/chaos-report.md)): 100,000 tasks plus 5,000 duplicate sends, 8 workers, and 345 injected faults (SIGKILL, SIGSTOP, SIGTERM, Redis pause and restart). All 100,000 tasks completed, none was charged twice, and the duplicate sends created no tasks.

## Quick start

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
docker compose up -d                      # Redis 7.4 with AOF
uv sync
uv run python -m taskq.worker             # terminal 1 (start more for more workers)
uv run python -m taskq.producer 10        # terminal 2
./scripts/dashboard                       # terminal 3: live queue view
```

## Architecture

### Task lifecycle

```
                 delay = 0
  enqueue ──────────────────────────────┐
     │ delay > 0                        ▼
     ▼                   LPUSH    ┌──────────┐ ◄── RPUSH ── reap (lease expired, retries left)
  delayed ── promote ───────────► │  ready   │ ◄── RPUSH ── release (graceful shutdown)
     ▲                            └──────────┘ ◄── LPUSH ── retry_dead (manual)
     │                                 │ lease
     │ fail (retries left)             ▼
     └─────────────────────────── leased ─── ack ──► completed
                                       │
                                       └── fail / reap (out of retries) ──► dlq
```

Workers take tasks from the right end of `queue:ready` (`RPOP`). `RPUSH` therefore puts a task at the front of the line and `LPUSH` at the back.

### Redis keys

| Key | Type | Contents |
|---|---|---|
| `queue:ready` | LIST | Task IDs that can run now |
| `queue:leased` | ZSET | Leased task IDs, score = lease expiry |
| `queue:delayed` | ZSET | Waiting task IDs, score = earliest run time |
| `queue:dlq` | LIST | Task IDs that ran out of retries |
| `task:<id>` | HASH | Task record (fields below) |
| `idem:<key>` | STRING | Task ID created with this idempotency key. Expires after 24h. |

`task:<id>` fields: `state` (`pending` / `delayed` / `running` / `completed` / `dead`), `max_retries`, `retries`, `fencing_token`, `worker`, `payload` (JSON), `idempotency_key`, `last_error`, and the timestamps `created_at`, `leased_at`, `heartbeat_at`, `run_at`, `completed_at`, `dead_at`.

### Lua scripts (`src/taskq/lua/`)

| Script | Does |
|---|---|
| `enqueue` | Creates the task record and puts the task on ready, or on delayed if `delay > 0`. A reused idempotency key returns the original task instead. |
| `lease` | Pops from ready into leased and issues a new fencing token |
| `heartbeat` | Extends the lease (current token holder only) |
| `ack` | Marks the task completed (current token holder only) |
| `fail` | Schedules a retry on delayed with backoff, or moves the task to the DLQ |
| `release` | Returns the task to the front of ready without using a retry |
| `reap` | Handles expired leases: counts a failed attempt, then sends the task to the front of ready or to the DLQ |
| `promote` | Moves tasks that are due from delayed to the back of ready |
| `retry_dead` | Moves a task from the DLQ back to ready and resets `retries` to 0 |

Each worker runs `promote` and `reap` on every loop iteration, so the system needs no separate scheduler process.

## Design decisions

- **Lease + reap instead of `BRPOP`.** With a plain `BRPOP`, a worker killed mid-task loses that task. Here a task stays in `queue:leased` until it is acked, and a lease that expires sends it back to ready.
- **Heartbeats.** A background thread renews the lease every `LEASE_SECONDS / 3`, so a lease survives two missed heartbeats. Long tasks don't need long leases, and dead workers are detected quickly.
- **Fencing tokens.** Every lease increments `task:<id>.fencing_token`. `ack`, `fail`, `heartbeat` and `release` must present the current token **and** the task must still be in `queue:leased`. A worker that paused past its lease therefore can't complete or fail a task that another worker took over. The token is also passed to the side effect so downstream systems can fence (see `ledger.py`).
- **Lua for every multi-key change.** Redis runs each script atomically, so there are no partial states, such as a task record that exists while its ID is in no queue.
- **Redis server time only.** Every expiry and run time comes from Redis `TIME`, never from a worker's clock, so clock skew between machines doesn't matter.
- **An expired lease counts as a failure.** A task that keeps crashing its worker never gets to call `fail`, but it still ends up in the DLQ instead of looping forever.
- **Queue position.** A reaped or released task already reached the front of the queue once, so it goes back to the front (`RPUSH`). A promoted task only became runnable now, so it goes to the back (`LPUSH`).
- **Backoff.** The base delay is `min(base · 2^(retries−1), cap)`. "Equal jitter" then scales it to between 50% and 100%, so tasks that failed together don't all retry at the same moment. Python generates the random number because `math.random` inside Redis scripts is not reliably random.
- **Idempotent enqueue.** `SET idem:<key> NX EX` makes a producer that retries after a timeout reuse the original task.
- **Graceful shutdown.** The first SIGTERM or SIGINT stops the worker from leasing new tasks. The task in hand gets `SHUTDOWN_GRACE_SECONDS` to finish and is released if it doesn't. A second signal exits immediately, and the task is reaped later.
- **Redis outages.** The client retries brief blips itself. During a longer outage the worker waits with backoff instead of crashing. `ack`, `fail` and `release` are retried for up to one lease length, because a lost ack means the task runs again.

## Configuration

### Environment variables

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | all | Redis connection |
| `REDIS_TIMEOUT_SECONDS` | `5` | all | Socket connect and read timeout |
| `LEDGER_PATH` | `ledger.db` | worker, ledger | SQLite ledger file |
| `LEASE_SECONDS` | `10` | worker | Lease length (heartbeat interval = 1/3 of it) |
| `WORK_SECONDS` | `3` | worker | Simulated work before the side effect |
| `AFTER_EFFECT_SECONDS` | `0` | worker | Simulated work after the side effect, before the ack |
| `FAIL_RATE` | `0` | worker | Probability that the handler raises |
| `LEDGER_GUARD` | `dedupe` | worker | `none` / `dedupe` / `fence` (see [Tools](#tools)) |
| `BACKOFF_BASE_SECONDS` | `1` | worker | Backoff before the first retry |
| `BACKOFF_MAX_SECONDS` | `60` | worker | Backoff cap |
| `POLL_SECONDS` | `0.5` | worker | Sleep when the ready queue is empty |
| `SHUTDOWN_GRACE_SECONDS` | `10` | worker | Time the task in hand gets on shutdown |

Example: `WORK_SECONDS=15 LEASE_SECONDS=5 FAIL_RATE=0.5 uv run python -m taskq.worker`

### Producer

```
uv run python -m taskq.producer [count] [--delay SECONDS] [--max-retries N]
                                [--charge AMOUNT] [--idempotency-prefix PREFIX]
```

| Option | Default | Meaning |
|---|---|---|
| `count` | `5` | Number of tasks |
| `--delay` | `0` | Seconds before the tasks may run |
| `--max-retries` | `3` | Retries before a task goes to the DLQ |
| `--charge` | none | Each task charges this amount to account `alice` in the ledger |
| `--idempotency-prefix` | none | Gives task `i` the key `PREFIX-i`, so re-running the same command creates nothing |

### Python API (`taskq.broker`)

| Call | Returns |
|---|---|
| `connect()` | `redis.Redis` configured from the environment |
| `Broker(r).enqueue(task_id, delay=0, max_retries=3, payload=None, idempotency_key=None, idempotency_ttl=86400)` | `Enqueued(task_id, created)` |
| `.enqueue_many(tasks: Sequence[NewTask], batch_size=1000)` | `list[Enqueued]`, sent as pipelined batches |
| `.lease(worker_id, lease_seconds)` | `(task_id, token)` or `None` |
| `.heartbeat(task_id, token, lease_seconds)` | `bool`, False = lease lost |
| `.ack(task_id, token)` | `bool`, False = stale token |
| `.fail(task_id, token, error, backoff_base, backoff_max)` | `FailResult(status="retry" \| "dead" \| "stale", retries, delay)` |
| `.release(task_id, token)` | `bool` |
| `.reap(limit=100)` | `list[(task_id, "ready" \| "dead")]` |
| `.promote(limit=100)` | `list[task_id]` |
| `.payload(task_id)` | `dict` (`{}` if the task has no payload) |
| `.dead()` | `list[task_id]`, most recently dead first |
| `.retry_dead(task_id)` | `bool`, False = the task was not in the DLQ |

## Tools

| Command | Purpose |
|---|---|
| `./scripts/dashboard [--once]` | Live view of all queues with lease and run countdowns, plus DLQ errors |
| `./scripts/rc [args]` | `redis-cli` inside the Redis container, e.g. `./scripts/rc LRANGE queue:ready 0 -1` |
| `uv run python -m taskq.dlq` | List dead tasks with their retry counts and last errors |
| `uv run python -m taskq.dlq retry <id> \| --all` | Send dead tasks back to ready |
| `uv run python -m taskq.ledger` | Summarize the ledger and list tasks charged more than once |
| `uv run python -m taskq.chaos [options]` | Chaos test (see [Testing](#testing)) |

The ledger (`ledger.py`) is a SQLite stand-in for a downstream system. Its guard decides how it handles duplicate executions:

- `none`: no protection. A task that runs twice charges twice.
- `dedupe`: records each applied task in the same transaction as the charge, so a second run is a no-op.
- `fence`: rejects writes whose token is older than the largest token already written. It cannot stop an older token that writes first.

## Testing

Tests run against the real Redis from `docker compose` on DB 15 (flushed around each test).

```bash
uv run pytest                    # everything
uv run pytest -m "not outage"    # skip tests that stop, pause or kill the Redis container
```

The chaos test uses DB 14 (flushed at start) and its own ledger, and writes logs to `.chaos/`:

```bash
uv run python -m taskq.chaos --redis-faults --report docs/chaos-report.md
```

| Option | Default | Meaning |
|---|---|---|
| `--tasks` | `100000` | Unique tasks to enqueue |
| `--workers` | `8` | Worker processes |
| `--resend-every` | `20` | Re-send every Nth task with the same idempotency key |
| `--fault-interval` | `0.5` | Average seconds between faults |
| `--redis-faults` | off | Also pause and restart the Redis container |
| `--max-seconds` | `1800` | Stop injecting faults after this long |
| `--drain-timeout` | `120` | Seconds to wait for all queues to drain at the end |
| `--guard` | `dedupe` | Ledger guard for the workers (`none` makes the test fail) |
| `--seed` | random | RNG seed |
| `--redis-url` | `redis://localhost:6379/14` | Redis to use |
| `--report` | none | Also write the report to this Markdown file |

## Known limitations

- **Single-node Redis only.** Scripts build `task:<id>` and `idem:<key>` key names at runtime instead of receiving them in `KEYS`, so they would fail across Redis Cluster slots. Hash tags (`{taskq}:...`) would fix this.
- **Reap has no backoff.** An expired lease counts as a failure, as in `fail`, but the task goes straight back to the front of ready instead of waiting on delayed.
- **Task records never expire.** `task:<id>` hashes of completed and dead tasks accumulate.
- **Enqueue without an idempotency key can duplicate.** The client retries a command whose reply was lost, so the task may be created twice.
- **An abandoned handler may still run.** When the shutdown grace period runs out, the task is released, but its handler thread keeps running until the process exits and may still apply its side effect.
- **A lost `lease` reply orphans the task.** If the client retries `lease`, the first leased task sits in leased until it is reaped.

## Project structure

```
src/taskq/
  broker.py     queue operations (wraps the Lua scripts)
  worker.py     worker loop, heartbeat, shutdown, outage handling
  producer.py   enqueue CLI
  dlq.py        DLQ inspect/retry CLI
  ledger.py     SQLite downstream system with duplicate guards
  chaos.py      chaos test
  lua/          one atomic script per operation
scripts/        dashboard, rc (redis-cli)
tests/          pytest suite (real Redis)
docs/           chaos test report
compose.yaml    Redis 7.4 with AOF
```
