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

    def heartbeat(self, task_id: str, token: int, lease_seconds: float) -> bool:
        """Extend the lease. False means the token is stale and the task is no longer this worker's."""
        return self._heartbeat(keys=[LEASED_SET], args=[task_id, token, lease_seconds]) == 1

    def reap(self, limit: int = 100) -> list[str]:
        """Move tasks with expired leases back to ready. Returns the reclaimed task IDs."""
        return self._reap(keys=[LEASED_SET, READY_QUEUE], args=[limit])
