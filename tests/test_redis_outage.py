"""Redis going away: dropped connections, restarts, crashes, and a frozen server.

Most of these stop, pause, or kill the shared Redis container, so they briefly affect every database on it.
Skip them with: uv run pytest -m "not outage"
"""

import shutil
import subprocess
import time
from pathlib import Path

import pytest
import redis
from conftest import StartWorker
from helpers import task, wait_until

from taskq.broker import READY_QUEUE, Broker

PROJECT_ROOT = Path(__file__).parent.parent

pytestmark = pytest.mark.outage


def compose(*args: str) -> None:
    if shutil.which("docker") is None:
        pytest.skip("needs the docker CLI to control the Redis container")
    subprocess.run(["docker", "compose", *args], cwd=PROJECT_ROOT, check=True, capture_output=True)


def wait_for_redis(r: redis.Redis) -> None:
    def up() -> bool:
        try:
            return bool(r.ping())
        except redis.RedisError:
            return False

    wait_until(up, timeout=30, interval=0.2)


def all_completed(r: redis.Redis, task_ids: list[str]) -> bool:
    pipe = r.pipeline()
    for task_id in task_ids:
        pipe.hget(f"task:{task_id}", "state")
    return all(state == "completed" for state in pipe.execute())


def test_worker_rides_out_dropped_connections(broker: Broker, r: redis.Redis, start_worker: StartWorker) -> None:
    workers = [start_worker(WORK_SECONDS=0.01) for _ in range(2)]
    task_ids = [f"t{i}" for i in range(200)]
    for task_id in task_ids:
        broker.enqueue(task_id)

    # Keep cutting every other client's connection while the workers are busy
    deadline = time.monotonic() + 30
    while not all_completed(r, task_ids) and time.monotonic() < deadline:
        r.client_kill_filter(_type="normal", skipme=True)
        time.sleep(0.2)

    assert all_completed(r, task_ids)
    assert all(worker.is_alive() for worker in workers)


def test_worker_survives_a_redis_restart(broker: Broker, r: redis.Redis, start_worker: StartWorker) -> None:
    worker = start_worker(WORK_SECONDS=0.05, REDIS_TIMEOUT_SECONDS=1)
    task_ids = [f"t{i}" for i in range(40)]
    for task_id in task_ids:
        broker.enqueue(task_id)
    wait_until(lambda: "done t5" in worker.log())

    # Stay down for 2s: longer than redis-py's own quick retries (about 1.5s), so the worker's loop must cope
    compose("stop", "redis")
    time.sleep(2)
    compose("start", "redis")
    wait_for_redis(r)

    wait_until(lambda: all_completed(r, task_ids), timeout=30)
    assert worker.is_alive()
    assert "redis unavailable" in worker.log()


def test_tasks_survive_a_redis_crash(broker: Broker, r: redis.Redis) -> None:
    task_ids = [f"t{i}" for i in range(50)]
    for task_id in task_ids:
        broker.enqueue(task_id)
    # appendfsync everysec: writes older than about a second are on disk
    time.sleep(1.5)

    # SIGKILL, not a clean shutdown, so Redis gets no chance to write a final snapshot: only the AOF can save us
    compose("kill", "-s", "SIGKILL", "redis")
    compose("start", "redis")
    wait_for_redis(r)

    assert r.llen(READY_QUEUE) == 50
    assert all(task(r, task_id)["state"] == "pending" for task_id in task_ids)


def test_heartbeat_gives_up_the_lease_while_redis_is_unreachable(
    broker: Broker, r: redis.Redis, start_worker: StartWorker
) -> None:
    broker.enqueue("t")
    worker = start_worker(WORK_SECONDS=4, LEASE_SECONDS=1, REDIS_TIMEOUT_SECONDS=0.3)
    wait_until(lambda: task(r, "t").get("state") == "running")

    # A frozen Redis accepts connections but never answers, unlike a stopped one
    compose("pause", "redis")
    try:
        wait_until(lambda: "assuming it is lost" in worker.log(), timeout=10)
    finally:
        compose("unpause", "redis")
    wait_for_redis(r)

    # The worker carried on, and the task still completes once Redis is back
    wait_until(lambda: task(r, "t")["state"] == "completed", timeout=15)
    assert worker.is_alive()
