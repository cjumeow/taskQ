"""Small helpers shared by the tests."""

import time
from collections.abc import Callable

import redis


def wait_until(condition: Callable[[], bool], timeout: float = 10.0, interval: float = 0.05) -> None:
    """Poll condition until it is true, or fail the test after timeout seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")


def redis_now(r: redis.Redis) -> float:
    """Current time on the Redis server, the clock every lease decision uses."""
    seconds, microseconds = r.time()
    return seconds + microseconds / 1_000_000


def task(r: redis.Redis, task_id: str) -> dict[str, str]:
    return r.hgetall(f"task:{task_id}")
