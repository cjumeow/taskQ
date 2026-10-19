"""Broker scripts called directly, one step at a time, so timing-sensitive behavior is deterministic."""

import threading
import time

import pytest
import redis
from helpers import redis_now, task, wait_until

import taskq.broker
from taskq.broker import DEAD_QUEUE, DELAYED_SET, LEASED_SET, READY_QUEUE, Broker


def lease_and_expire(broker: Broker, task_id: str) -> int:
    """Lease a task with a tiny lease, then let it expire as if the worker had died."""
    leased = broker.lease("doomed-worker", 0.1)
    assert leased is not None and leased[0] == task_id
    time.sleep(0.15)
    return leased[1]


# --- Atomic leasing ---


def test_concurrent_leases_never_hand_out_the_same_task(broker: Broker) -> None:
    for i in range(500):
        broker.enqueue(f"t{i}")
    leased: list[str] = []

    def grab() -> None:
        while (got := broker.lease("worker", 30)) is not None:
            leased.append(got[0])

    threads = [threading.Thread(target=grab) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(leased) == 500
    assert len(set(leased)) == 500


# --- Lease expiry and the single clock ---


def test_expired_lease_is_reaped_back_to_ready(broker: Broker, r: redis.Redis) -> None:
    broker.enqueue("t")
    lease_and_expire(broker, "t")

    assert broker.reap() == [("t", "ready")]
    assert r.lrange(READY_QUEUE, 0, -1) == ["t"]
    assert task(r, "t")["state"] == "pending"
    assert task(r, "t")["last_error"] == "lease expired"


def test_live_lease_is_not_reaped(broker: Broker, r: redis.Redis) -> None:
    broker.enqueue("t")
    broker.lease("worker", 30)

    assert broker.reap() == []
    assert r.zscore(LEASED_SET, "t") is not None


def test_lease_expiry_is_measured_on_the_redis_clock_not_the_workers(
    broker: Broker, r: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    broker.enqueue("t")
    real_time = time.time

    # The leasing worker's clock runs an hour slow: if it were used, the lease would be born expired
    monkeypatch.setattr(time, "time", lambda: real_time() - 3600)
    broker.lease("worker", 30)
    assert r.zscore(LEASED_SET, "t") == pytest.approx(redis_now(r) + 30, abs=1)

    # A reaping worker whose clock runs an hour fast must not see the lease as expired either
    monkeypatch.setattr(time, "time", lambda: real_time() + 3600)
    assert broker.reap() == []


# --- Heartbeat ---


def test_heartbeat_extends_the_lease(broker: Broker, r: redis.Redis) -> None:
    broker.enqueue("t")
    _, token = broker.lease("worker", 0.3)

    assert broker.heartbeat("t", token, 30)
    time.sleep(0.4)
    assert broker.reap() == []
    assert r.zscore(LEASED_SET, "t") == pytest.approx(redis_now(r) + 29.6, abs=1)


# --- Fencing tokens ---


def test_each_lease_gets_a_larger_token(broker: Broker) -> None:
    broker.enqueue("t")
    first = lease_and_expire(broker, "t")
    broker.reap()
    _, second = broker.lease("worker", 30)

    assert second > first


def test_stale_token_cannot_ack_heartbeat_or_fail(broker: Broker, r: redis.Redis) -> None:
    broker.enqueue("t")
    old = lease_and_expire(broker, "t")
    broker.reap()
    _, new = broker.lease("new-owner", 30)
    expires_at = r.zscore(LEASED_SET, "t")

    assert not broker.ack("t", old)
    assert not broker.heartbeat("t", old, 60)
    assert broker.fail("t", old, "late failure", 1, 60).status == "stale"

    # None of the stale calls touched the new owner's lease or the task state
    assert r.zscore(LEASED_SET, "t") == expires_at
    assert task(r, "t")["state"] == "running"
    assert task(r, "t")["worker"] == "new-owner"
    assert broker.ack("t", new)


def test_ack_is_rejected_after_reap_even_before_anyone_releases_the_task(broker: Broker, r: redis.Redis) -> None:
    broker.enqueue("t")
    token = lease_and_expire(broker, "t")
    broker.reap()

    # The token is still the latest one, but the task is back on ready and will run again
    assert not broker.ack("t", token)
    assert task(r, "t")["state"] == "pending"
    assert r.lrange(READY_QUEUE, 0, -1) == ["t"]


# --- Retries with exponential backoff ---


@pytest.mark.parametrize("jitter", [0.0, 0.999999])
def test_failures_back_off_exponentially_up_to_the_cap(
    broker: Broker, r: redis.Redis, monkeypatch: pytest.MonkeyPatch, jitter: float
) -> None:
    monkeypatch.setattr(taskq.broker.random, "random", lambda: jitter)
    broker.enqueue("t", max_retries=4)

    # base 0.2s doubling each time, capped at 1s
    for retries, backoff in enumerate([0.2, 0.4, 0.8, 1.0], start=1):
        _, token = broker.lease("worker", 30)
        result = broker.fail("t", token, "boom", backoff_base=0.2, backoff_max=1.0)

        assert result.status == "retry"
        assert result.retries == retries
        # Equal jitter: somewhere between half and all of the backoff
        assert result.delay == pytest.approx(backoff * (0.5 + 0.5 * jitter))
        assert r.zscore(DELAYED_SET, "t") == pytest.approx(redis_now(r) + result.delay, abs=0.05)
        assert task(r, "t")["state"] == "delayed"

        # Fast-forward instead of sleeping through the backoff (XX: only if it is actually delayed)
        r.zadd(DELAYED_SET, {"t": 0}, xx=True)
        assert broker.promote() == ["t"]


def test_jitter_spreads_out_retries(broker: Broker) -> None:
    delays = []
    for i in range(20):
        broker.enqueue(f"t{i}")
        task_id, token = broker.lease("worker", 30)
        delays.append(broker.fail(task_id, token, "boom", backoff_base=1, backoff_max=60).delay)

    assert all(0.5 <= delay <= 1.0 for delay in delays)
    assert len(set(delays)) > 1


def test_retried_task_is_not_runnable_until_its_backoff_is_over(broker: Broker) -> None:
    broker.enqueue("t")
    _, token = broker.lease("worker", 30)
    broker.fail("t", token, "boom", backoff_base=1, backoff_max=60)

    assert broker.promote() == []
    assert broker.lease("worker", 30) is None


# --- DLQ ---


def test_task_moves_to_dlq_after_max_retries(broker: Broker, r: redis.Redis) -> None:
    broker.enqueue("t", max_retries=2)

    for attempt in range(1, 4):
        _, token = broker.lease("worker", 30)
        result = broker.fail("t", token, f"boom #{attempt}", backoff_base=1, backoff_max=60)
        r.zadd(DELAYED_SET, {"t": 0}, xx=True)
        broker.promote()

    assert result.status == "dead"
    assert result.retries == 3
    assert broker.dead() == ["t"]
    assert task(r, "t")["state"] == "dead"
    assert task(r, "t")["last_error"] == "boom #3"
    assert r.llen(READY_QUEUE) == 0
    assert r.zcard(DELAYED_SET) == 0
    assert r.zcard(LEASED_SET) == 0


def test_task_that_keeps_killing_its_worker_ends_in_dlq(broker: Broker, r: redis.Redis) -> None:
    broker.enqueue("poison", max_retries=2)

    # Each attempt "crashes" the worker, so only the reaper ever sees it fail
    outcomes = []
    for _ in range(3):
        lease_and_expire(broker, "poison")
        outcomes += [outcome for _, outcome in broker.reap()]

    assert outcomes == ["ready", "ready", "dead"]
    assert broker.dead() == ["poison"]
    assert task(r, "poison")["last_error"] == "lease expired"


def test_dead_task_can_be_retried_with_a_fresh_budget(broker: Broker, r: redis.Redis) -> None:
    broker.enqueue("t", max_retries=0)
    _, token = broker.lease("worker", 30)
    broker.fail("t", token, "boom", backoff_base=1, backoff_max=60)

    assert broker.retry_dead("t")
    assert r.llen(DEAD_QUEUE) == 0
    assert task(r, "t")["state"] == "pending"
    assert task(r, "t")["retries"] == "0"
    assert task(r, "t")["last_error"] == "boom"  # History is kept

    task_id, token = broker.lease("worker", 30)
    assert broker.ack(task_id, token)


def test_retrying_a_task_that_is_not_dead_does_nothing(broker: Broker, r: redis.Redis) -> None:
    broker.enqueue("t")

    assert not broker.retry_dead("t")
    assert r.lrange(READY_QUEUE, 0, -1) == ["t"]


# --- Delayed tasks ---


def test_delayed_task_does_not_run_before_it_is_due(broker: Broker, r: redis.Redis) -> None:
    started = time.monotonic()
    broker.enqueue("t", delay=0.5)

    assert task(r, "t")["state"] == "delayed"
    assert broker.promote() == []
    assert broker.lease("worker", 30) is None

    wait_until(lambda: broker.promote() == ["t"], timeout=2, interval=0.02)
    assert time.monotonic() - started >= 0.45
    assert broker.lease("worker", 30) is not None
