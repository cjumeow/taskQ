"""End-to-end tests with real worker processes: crashes, pauses, and long-running tasks."""

import os
import signal

import pytest
import redis
from conftest import StartWorker
from helpers import task, wait_until

from taskq.broker import DEAD_QUEUE, DELAYED_SET, LEASED_SET, READY_QUEUE, Broker
from taskq.ledger import Ledger


def queues_drained(r: redis.Redis) -> bool:
    return r.llen(READY_QUEUE) == 0 and r.zcard(LEASED_SET) == 0 and r.zcard(DELAYED_SET) == 0


def test_all_tasks_complete_exactly_once_with_many_workers(
    broker: Broker, r: redis.Redis, start_worker: StartWorker
) -> None:
    # Start the workers first: with no work to do, one early worker could otherwise drain the queue alone
    workers = [start_worker() for _ in range(4)]
    task_ids = [f"t{i}" for i in range(1000)]
    for task_id in task_ids:
        broker.enqueue(task_id)

    wait_until(lambda: queues_drained(r), timeout=60)

    pipe = r.pipeline()
    for task_id in task_ids:
        pipe.hmget(f"task:{task_id}", "state", "fencing_token")
    results = pipe.execute()
    assert all(state == "completed" for state, _ in results)
    # Token 1 on every task: each one was leased exactly once
    assert all(token == "1" for _, token in results)
    # The work was actually shared
    worker_ids = {r.hget(f"task:{task_id}", "worker") for task_id in task_ids}
    assert worker_ids == {worker.worker_id for worker in workers}


def test_task_of_a_killed_worker_is_finished_by_another(
    broker: Broker, r: redis.Redis, start_worker: StartWorker
) -> None:
    broker.enqueue("t")
    victim = start_worker(WORK_SECONDS=30, LEASE_SECONDS=0.6)
    wait_until(lambda: task(r, "t").get("worker") == victim.worker_id)

    os.kill(victim.pid, signal.SIGKILL)
    rescuer = start_worker(LEASE_SECONDS=0.6)

    wait_until(lambda: task(r, "t")["state"] == "completed")
    assert task(r, "t")["worker"] == rescuer.worker_id
    assert task(r, "t")["fencing_token"] == "2"


def test_heartbeat_keeps_a_long_task_from_being_handed_out_again(
    broker: Broker, r: redis.Redis, start_worker: StartWorker
) -> None:
    broker.enqueue("t")
    # Work takes 2s but the lease is only 0.6s, so without heartbeats the idle worker would steal it
    busy = start_worker(WORK_SECONDS=2, LEASE_SECONDS=0.6)
    wait_until(lambda: task(r, "t").get("worker") == busy.worker_id)
    idle = start_worker(LEASE_SECONDS=0.6)

    wait_until(lambda: task(r, "t")["state"] == "completed", timeout=5)
    assert task(r, "t")["fencing_token"] == "1"
    assert task(r, "t")["worker"] == busy.worker_id
    assert "got t" not in idle.log()


def test_failing_task_ends_in_dlq_and_the_worker_survives(
    broker: Broker, r: redis.Redis, start_worker: StartWorker
) -> None:
    broker.enqueue("t", max_retries=2)
    worker = start_worker(FAIL_RATE=1, BACKOFF_BASE_SECONDS=0.05)

    wait_until(lambda: task(r, "t")["state"] == "dead")
    assert r.lrange(DEAD_QUEUE, 0, -1) == ["t"]
    assert task(r, "t")["retries"] == "3"
    assert "simulated failure" in task(r, "t")["last_error"]
    assert worker.is_alive()

    # And it keeps working on other tasks
    broker.enqueue("next")
    wait_until(lambda: "failed next" in worker.log())


def test_paused_worker_cannot_complete_a_task_that_was_taken_over(
    broker: Broker, r: redis.Redis, start_worker: StartWorker
) -> None:
    broker.enqueue("t")
    # SIGSTOP freezes the whole process, heartbeat thread included, like a long GC pause
    paused = start_worker(WORK_SECONDS=1.5, LEASE_SECONDS=0.6)
    wait_until(lambda: task(r, "t").get("worker") == paused.worker_id)
    os.kill(paused.pid, signal.SIGSTOP)

    successor = start_worker(WORK_SECONDS=1, LEASE_SECONDS=0.6)
    wait_until(lambda: task(r, "t").get("fencing_token") == "2")
    os.kill(paused.pid, signal.SIGCONT)

    wait_until(lambda: "ack rejected for t" in paused.log())
    wait_until(lambda: task(r, "t")["state"] == "completed")
    assert task(r, "t")["worker"] == successor.worker_id
    assert "done t" in successor.log()
    assert "done t" not in paused.log()


# --- Side effects on a downstream system (the ledger) when a paused worker's task is taken over ---


@pytest.mark.parametrize(
    ("guard", "pause", "charged_tokens"),
    [
        # No guard: both workers charge, whichever order they do it in
        ("none", "before_charge", [2, 1]),
        ("none", "after_charge", [1, 2]),
        # dedupe: only the first charge for the task gets through
        ("dedupe", "before_charge", [2]),
        ("dedupe", "after_charge", [1]),
        # fence: catches the old worker if it charges late, but not if it charged before pausing
        ("fence", "before_charge", [2]),
        ("fence", "after_charge", [1, 2]),
    ],
)
def test_paused_worker_and_its_successor_charge_the_ledger(
    broker: Broker,
    r: redis.Redis,
    start_worker: StartWorker,
    ledger: Ledger,
    guard: str,
    pause: str,
    charged_tokens: list[int],
) -> None:
    broker.enqueue("t", payload={"type": "charge", "account": "alice", "amount": 100})
    if pause == "before_charge":
        # Paused while still working, before it gets to the charge
        paused = start_worker(LEDGER_GUARD=guard, WORK_SECONDS=1.5, LEASE_SECONDS=0.6)
        wait_until(lambda: task(r, "t").get("worker") == paused.worker_id)
    else:
        # Paused after the charge was written, before the ack
        paused = start_worker(LEDGER_GUARD=guard, AFTER_EFFECT_SECONDS=1.5, LEASE_SECONDS=0.6)
        wait_until(lambda: len(ledger.charges_for("t")) == 1)
    os.kill(paused.pid, signal.SIGSTOP)

    successor = start_worker(LEDGER_GUARD=guard, WORK_SECONDS=0.2, LEASE_SECONDS=0.6)
    wait_until(lambda: task(r, "t")["state"] == "completed")
    os.kill(paused.pid, signal.SIGCONT)
    wait_until(lambda: "ack rejected for t" in paused.log())

    # The queue's own state is right in every case: fencing tokens see to that
    assert task(r, "t")["worker"] == successor.worker_id
    # What happens downstream depends on the guard
    assert [token for token, _ in ledger.charges_for("t")] == charged_tokens
