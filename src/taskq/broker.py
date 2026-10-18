"""Queue operations on Redis. Anything that touches multiple keys is a Lua script, so it runs atomically."""

from pathlib import Path

import redis

READY_QUEUE = "queue:ready"
LEASED_SET = "queue:leased"

LUA_DIR = Path(__file__).parent / "lua"


class Broker:
    def __init__(self, r: redis.Redis) -> None:
        # register_script sends the source to Redis on first call, then only the SHA (EVALSHA)
        self._lease = r.register_script((LUA_DIR / "lease.lua").read_text())
        self._ack = r.register_script((LUA_DIR / "ack.lua").read_text())
        self._reap = r.register_script((LUA_DIR / "reap.lua").read_text())
        self._heartbeat = r.register_script((LUA_DIR / "heartbeat.lua").read_text())

    def lease(self, worker_id: str, lease_seconds: float) -> str | None:
        """Lease one task. Returns None if the queue is empty."""
        return self._lease(keys=[READY_QUEUE, LEASED_SET], args=[worker_id, lease_seconds])

    def ack(self, task_id: str) -> bool:
        """Mark a task completed. False means the lease had already expired and another worker may have redone it."""
        return self._ack(keys=[LEASED_SET], args=[task_id]) == 1

    def heartbeat(self, task_id: str, worker_id: str, lease_seconds: float) -> bool:
        """Extend the lease. False means this worker no longer holds it and should stop treating the task as its own."""
        return self._heartbeat(keys=[LEASED_SET], args=[task_id, worker_id, lease_seconds]) == 1

    def reap(self, limit: int = 100) -> list[str]:
        """Move tasks with expired leases back to ready. Returns the reclaimed task IDs."""
        return self._reap(keys=[LEASED_SET, READY_QUEUE], args=[limit])
