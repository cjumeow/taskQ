"""Queue operations on Redis. Anything that touches multiple keys is a Lua script, so it runs atomically."""

import json
import os
import random
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import redis
from redis.backoff import ExponentialWithJitterBackoff
from redis.retry import Retry

READY_QUEUE = "queue:ready"
LEASED_SET = "queue:leased"
DELAYED_SET = "queue:delayed"
DEAD_QUEUE = "queue:dlq"

LUA_DIR = Path(__file__).parent / "lua"

# Long enough to cover any realistic producer retry, short enough that keys don't pile up forever
IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60

# Override with e.g. REDIS_URL=redis://localhost:6379/15 to use another database (the tests do this)
DEFAULT_REDIS_URL = "redis://localhost:6379/0"


# Errors that mean "Redis is unreachable right now", as opposed to a bug such as a failing script
REDIS_DOWN = (redis.ConnectionError, redis.TimeoutError)


def connect() -> redis.Redis:
    """Connect to the Redis named by REDIS_URL, returning str replies instead of bytes."""
    timeout = float(os.environ.get("REDIS_TIMEOUT_SECONDS", 5))
    return redis.Redis.from_url(
        os.environ.get("REDIS_URL", DEFAULT_REDIS_URL),
        decode_responses=True,
        # Give up on a command that gets no reply instead of hanging, so callers notice Redis is down
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
        # Ride out a brief blip by retrying a command a few times, then let the caller handle the outage.
        # A retried script can run twice when only its reply was lost. That is safe for ack, fail,
        # heartbeat and release (the second run fails its token and lease checks) and for lease (the
        # extra task sits leased until it is reaped). A retried enqueue without an idempotency key can
        # create a duplicate task.
        retry=Retry(ExponentialWithJitterBackoff(base=0.05, cap=0.5), retries=3),
    )


@dataclass
class NewTask:
    """A task to enqueue; see Broker.enqueue for what each field means."""

    task_id: str
    delay: float = 0
    max_retries: int = 3
    payload: dict[str, Any] | None = None
    idempotency_key: str | None = None
    idempotency_ttl: int = IDEMPOTENCY_TTL_SECONDS


@dataclass
class Enqueued:
    task_id: str
    # False when the idempotency key was already used: task_id is then the original task, nothing new was created
    created: bool


@dataclass
class FailResult:
    # retry: scheduled on queue:delayed; dead: moved to the DLQ; stale: token was stale, nothing changed
    status: Literal["retry", "dead", "stale"]
    retries: int = 0
    delay: float = 0.0


class Broker:
    def __init__(self, r: redis.Redis) -> None:
        self._r = r
        # register_script sends the source to Redis on first call, then only the SHA (EVALSHA)
        self._enqueue = r.register_script((LUA_DIR / "enqueue.lua").read_text())
        self._lease = r.register_script((LUA_DIR / "lease.lua").read_text())
        self._ack = r.register_script((LUA_DIR / "ack.lua").read_text())
        self._fail = r.register_script((LUA_DIR / "fail.lua").read_text())
        self._heartbeat = r.register_script((LUA_DIR / "heartbeat.lua").read_text())
        self._reap = r.register_script((LUA_DIR / "reap.lua").read_text())
        self._promote = r.register_script((LUA_DIR / "promote.lua").read_text())
        self._retry_dead = r.register_script((LUA_DIR / "retry_dead.lua").read_text())
        self._release = r.register_script((LUA_DIR / "release.lua").read_text())

    def enqueue(
        self,
        task_id: str,
        delay: float = 0,
        max_retries: int = 3,
        payload: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        idempotency_ttl: int = IDEMPOTENCY_TTL_SECONDS,
    ) -> Enqueued:
        """Create a task. With delay > 0 it waits on queue:delayed until it is due.

        If idempotency_key was already used within idempotency_ttl, nothing is created and the
        original task's ID is returned with created=False.
        """
        return self.enqueue_many([NewTask(task_id, delay, max_retries, payload, idempotency_key, idempotency_ttl)])[0]

    def enqueue_many(self, tasks: Sequence[NewTask], batch_size: int = 1000) -> list[Enqueued]:
        """Enqueue many tasks, sending them in pipelined batches instead of one round trip each.

        Each task is still created by its own atomic script run; only the network round trips are batched.
        """
        results: list[Enqueued] = []
        for start in range(0, len(tasks), batch_size):
            pipe = self._r.pipeline(transaction=False)
            for task in tasks[start : start + batch_size]:
                self._enqueue(
                    keys=[READY_QUEUE, DELAYED_SET],
                    args=[
                        task.task_id,
                        task.delay,
                        task.max_retries,
                        json.dumps(task.payload) if task.payload is not None else "",
                        task.idempotency_key or "",
                        task.idempotency_ttl,
                    ],
                    client=pipe,
                )
            results += [Enqueued(task_id, created == 1) for task_id, created in pipe.execute()]
        return results

    def payload(self, task_id: str) -> dict[str, Any]:
        """The task's payload, or {} if it was enqueued without one."""
        raw = self._r.hget(f"task:{task_id}", "payload")
        return json.loads(cast(str, raw)) if raw else {}

    def lease(self, worker_id: str, lease_seconds: float) -> tuple[str, int] | None:
        """Lease one task. Returns (task id, fencing token), or None if the queue is empty."""
        result = self._lease(keys=[READY_QUEUE, LEASED_SET], args=[worker_id, lease_seconds])
        if result is None:
            return None
        task_id, token = result
        return task_id, int(token)

    def ack(self, task_id: str, token: int) -> bool:
        """Mark a task completed. False means the token is stale: the ack was rejected and nothing changed."""
        return self._ack(keys=[LEASED_SET], args=[task_id, token]) == 1

    def fail(
        self, task_id: str, token: int, error: str, backoff_base: float, backoff_max: float
    ) -> FailResult:
        """Report a failed attempt: retry later with exponential backoff, or move to the DLQ when out of retries."""
        result = self._fail(
            keys=[LEASED_SET, DELAYED_SET, DEAD_QUEUE],
            args=[task_id, token, error, backoff_base, backoff_max, random.random()],
        )
        match result:
            case ["retry", retries, delay]:
                return FailResult("retry", int(retries), float(delay))
            case ["dead", retries]:
                return FailResult("dead", int(retries))
            case _:
                return FailResult("stale")

    def release(self, task_id: str, token: int) -> bool:
        """Give a leased task back to ready without using up a retry. False means the token is stale."""
        return self._release(keys=[LEASED_SET, READY_QUEUE], args=[task_id, token]) == 1

    def heartbeat(self, task_id: str, token: int, lease_seconds: float) -> bool:
        """Extend the lease. False means the token is stale and the task is no longer this worker's."""
        return self._heartbeat(keys=[LEASED_SET], args=[task_id, token, lease_seconds]) == 1

    def reap(self, limit: int = 100) -> list[tuple[str, str]]:
        """Handle expired leases. Returns (task id, "ready" | "dead") for each reclaimed task."""
        return [
            (task_id, outcome)
            for task_id, outcome in self._reap(keys=[LEASED_SET, READY_QUEUE, DEAD_QUEUE], args=[limit])
        ]

    def promote(self, limit: int = 100) -> list[str]:
        """Move delayed tasks that are due onto ready. Returns the promoted task IDs."""
        return self._promote(keys=[DELAYED_SET, READY_QUEUE], args=[limit])

    def dead(self) -> list[str]:
        """Task IDs in the DLQ, most recently dead first."""
        # The client is created with decode_responses=True, so replies are str, not bytes
        return cast(list[str], self._r.lrange(DEAD_QUEUE, 0, -1))

    def retry_dead(self, task_id: str) -> bool:
        """Move a task from the DLQ back onto ready with its retry count reset. False if it was not in the DLQ."""
        return self._retry_dead(keys=[DEAD_QUEUE, READY_QUEUE], args=[task_id]) == 1
