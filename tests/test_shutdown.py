"""Graceful shutdown: what SIGTERM does to the task in hand, compared with SIGKILL."""

import os
import signal
import time

import redis
from conftest import StartWorker
from helpers import task, wait_until

from taskq.broker import LEASED_SET, READY_QUEUE, Broker


def test_sigterm_lets_the_task_in_hand_finish(broker: Broker, r: redis.Redis, start_worker: StartWorker) -> None:
    broker.enqueue("t")
    worker = start_worker(WORK_SECONDS=1, SHUTDOWN_GRACE_SECONDS=10)
    wait_until(lambda: task(r, "t").get("state") == "running")

    worker.proc.send_signal(signal.SIGTERM)

    assert worker.proc.wait(timeout=5) == 0
    assert task(r, "t")["state"] == "completed"
    assert "got SIGTERM" in worker.log()


def test_sigterm_stops_taking_new_tasks(broker: Broker, r: redis.Redis, start_worker: StartWorker) -> None:
    broker.enqueue("first")
    broker.enqueue("second")
    worker = start_worker(WORK_SECONDS=1, SHUTDOWN_GRACE_SECONDS=10)
    wait_until(lambda: task(r, "first").get("state") == "running")

    worker.proc.send_signal(signal.SIGTERM)

    assert worker.proc.wait(timeout=5) == 0
    assert task(r, "first")["state"] == "completed"
    assert task(r, "second")["state"] == "pending"
    assert r.lrange(READY_QUEUE, 0, -1) == ["second"]


def test_sigterm_releases_a_task_that_cannot_finish_in_time(
    broker: Broker, r: redis.Redis, start_worker: StartWorker
) -> None:
    broker.enqueue("t")
    worker = start_worker(WORK_SECONDS=30, LEASE_SECONDS=10, SHUTDOWN_GRACE_SECONDS=0.3)
    wait_until(lambda: task(r, "t").get("state") == "running")

    started = time.monotonic()
    worker.proc.send_signal(signal.SIGTERM)
    assert worker.proc.wait(timeout=5) == 0

    # Back on ready right away, long before the 10s lease would have expired, and not counted as a failure
    assert time.monotonic() - started < 3
    assert r.lrange(READY_QUEUE, 0, -1) == ["t"]
    assert r.zcard(LEASED_SET) == 0
    assert task(r, "t")["state"] == "pending"
    assert task(r, "t")["retries"] == "0"
    assert "released t back to ready" in worker.log()

    # So another worker can pick it up immediately
    start_worker()
    wait_until(lambda: task(r, "t")["state"] == "completed", timeout=3)
    assert task(r, "t")["fencing_token"] == "2"


def test_sigterm_while_idle_exits_promptly(start_worker: StartWorker) -> None:
    worker = start_worker(POLL_SECONDS=5)

    started = time.monotonic()
    worker.proc.send_signal(signal.SIGTERM)

    # It wakes from the 5s idle wait instead of sleeping it out
    assert worker.proc.wait(timeout=3) == 0
    assert time.monotonic() - started < 1


def test_second_signal_exits_immediately(broker: Broker, r: redis.Redis, start_worker: StartWorker) -> None:
    broker.enqueue("t")
    worker = start_worker(WORK_SECONDS=30, SHUTDOWN_GRACE_SECONDS=30)
    wait_until(lambda: task(r, "t").get("state") == "running")

    worker.proc.send_signal(signal.SIGTERM)
    wait_until(lambda: "got SIGTERM" in worker.log())
    worker.proc.send_signal(signal.SIGTERM)

    assert worker.proc.wait(timeout=3) == 1
    # Forced exit is like a crash: the task stays leased until it is reaped
    assert r.zscore(LEASED_SET, "t") is not None


def test_sigkill_costs_a_retry_where_sigterm_does_not(
    broker: Broker, r: redis.Redis, start_worker: StartWorker
) -> None:
    broker.enqueue("t")
    victim = start_worker(WORK_SECONDS=30, LEASE_SECONDS=0.5)
    wait_until(lambda: task(r, "t").get("state") == "running")

    os.kill(victim.pid, signal.SIGKILL)
    start_worker(LEASE_SECONDS=0.5)

    # No chance to release: the task is only recovered by the reaper, which counts it as a failed attempt
    wait_until(lambda: task(r, "t")["state"] == "completed")
    assert task(r, "t")["retries"] == "1"
    assert task(r, "t")["last_error"] == "lease expired"
