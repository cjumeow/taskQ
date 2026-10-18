"""Queue operations on Redis. Anything that touches multiple keys is a Lua script, so it runs atomically."""

import random
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

import redis

READY_QUEUE = "queue:ready"
LEASED_SET = "queue:leased"
DELAYED_SET = "queue:delayed"
DEAD_QUEUE = "queue:dlq"

LUA_DIR = Path(__file__).parent / "lua"


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

    def enqueue(self, task_id: str, delay: float = 0, max_retries: int = 3) -> None:
        """Create a task. With delay > 0 it waits on queue:delayed until it is due."""
        self._enqueue(keys=[READY_QUEUE, DELAYED_SET], args=[task_id, delay, max_retries])

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
