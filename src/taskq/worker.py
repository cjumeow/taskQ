"""Worker: lease a task -> do the work while heartbeating -> ack it, or report the failure for retry.

Every loop it also promotes due delayed tasks and reaps expired leases.

Usage: uv run python -m taskq.worker
Behavior can be tuned with environment variables for experiments:
    WORK_SECONDS=15 LEASE_SECONDS=5 FAIL_RATE=0.5 LEDGER_GUARD=none uv run python -m taskq.worker
"""

import os
import random
import socket
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, cast

from taskq.broker import Broker, connect
from taskq.ledger import GUARDS, Guard, Ledger

WORK_SECONDS = float(os.environ.get("WORK_SECONDS", 3))  # How long the fake work takes before the side effect
AFTER_EFFECT_SECONDS = float(os.environ.get("AFTER_EFFECT_SECONDS", 0))  # Work left after the side effect, before ack
FAIL_RATE = float(os.environ.get("FAIL_RATE", 0))  # Probability that the fake work raises
LEDGER_GUARD = cast(Guard, os.environ.get("LEDGER_GUARD", "dedupe"))  # How the ledger handles duplicate executions
LEASE_SECONDS = float(os.environ.get("LEASE_SECONDS", 10))  # Task is reclaimed if not acked within this
HEARTBEAT_SECONDS = LEASE_SECONDS / 3  # Renew often enough to survive two missed heartbeats
BACKOFF_BASE_SECONDS = float(os.environ.get("BACKOFF_BASE_SECONDS", 1))  # First retry waits about this long
BACKOFF_MAX_SECONDS = float(os.environ.get("BACKOFF_MAX_SECONDS", 60))  # Retries never wait longer than this
POLL_SECONDS = float(os.environ.get("POLL_SECONDS", 0.5))  # How long to wait before polling again when the queue is empty


def handle(ledger: Ledger, task_id: str, payload: dict[str, Any], token: int, worker_id: str) -> None:
    """Fake handler: work for WORK_SECONDS, maybe fail, apply the side effect, then work AFTER_EFFECT_SECONDS more.

    The token is handed to the side effect so the downstream system can fence off stale lease holders.
    """
    time.sleep(WORK_SECONDS)
    if random.random() < FAIL_RATE:
        raise RuntimeError(f"simulated failure (FAIL_RATE={FAIL_RATE})")

    if payload.get("type") == "charge":
        outcome = ledger.charge(task_id, payload["account"], payload["amount"], token, worker_id, LEDGER_GUARD)
        print(f"charge {task_id} (token {token}): {outcome}")

    # E.g. sending a receipt. A pause here lands after the charge but before the ack
    time.sleep(AFTER_EFFECT_SECONDS)


@contextmanager
def heartbeat(broker: Broker, task_id: str, token: int) -> Generator[threading.Event, None, None]:
    """Keep renewing the lease in a background thread while the body runs.

    Yields an Event that gets set if the lease is lost.
    """
    stop = threading.Event()
    lost = threading.Event()

    def run() -> None:
        # wait() returns True as soon as stop is "set", so the loop exits right after the work finishes
        while not stop.wait(HEARTBEAT_SECONDS):
            if not broker.heartbeat(task_id, token, LEASE_SECONDS):
                print(f"lost lease on {task_id}, stopping heartbeat")
                lost.set()
                return

    # daemon=True so a stuck heartbeat thread never keeps the process alive after Ctrl+C
    thread = threading.Thread(target=run, name=f"heartbeat-{task_id}", daemon=True)
    thread.start()
    try:
        yield lost # Signal that the lease is lost
    finally:
        stop.set()
        thread.join()


def report_failure(broker: Broker, task_id: str, token: int, error: str) -> None:
    result = broker.fail(task_id, token, error, BACKOFF_BASE_SECONDS, BACKOFF_MAX_SECONDS)
    match result.status:
        case "retry":
            print(f"failed {task_id}: {error} -> retry #{result.retries} in {result.delay:.1f}s")
        case "dead":
            print(f"failed {task_id}: {error} -> out of retries after {result.retries} failures, moved to DLQ")
        case "stale":
            print(f"failed {task_id}: {error} -> report rejected, token {token} is stale")


def main() -> None:
    if LEDGER_GUARD not in GUARDS:
        raise SystemExit(f"LEDGER_GUARD must be one of {', '.join(GUARDS)}, got {LEDGER_GUARD!r}")

    r = connect()
    broker = Broker(r)
    ledger = Ledger()
    # PID alone can collide across machines, so prefix it with the hostname
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    print(
        f"worker started, id = {worker_id}, lease = {LEASE_SECONDS}s, "
        f"heartbeat = {HEARTBEAT_SECONDS:.1f}s, work = {WORK_SECONDS}s, fail rate = {FAIL_RATE}, "
        f"ledger guard = {LEDGER_GUARD}"
    )

    try:
        while True:
            for task_id in broker.promote():
                print(f"promoted {task_id} (delay is over, back to ready)")
            for task_id, outcome in broker.reap():
                if outcome == "dead":
                    print(f"reaped {task_id} (lease expired, out of retries, moved to DLQ)")
                else:
                    print(f"reaped {task_id} (lease expired, back to ready)")

            leased = broker.lease(worker_id, LEASE_SECONDS)
            if leased is None:
                time.sleep(POLL_SECONDS)
                continue
            task_id, token = leased

            print(f"got {task_id} (token {token}), working...")
            try:
                with heartbeat(broker, task_id, token) as lost:
                    handle(ledger, task_id, broker.payload(task_id), token, worker_id)
            except Exception as e:
                # Catch everything so one bad task can't kill the worker; KeyboardInterrupt is not an Exception
                report_failure(broker, task_id, token, f"{type(e).__name__}: {e}")
                continue

            if lost.is_set():
                print(f"finished {task_id}, but the lease was lost mid-way")
            # Always try to ack: Redis is the single judge, and rejects it if the token is stale
            if broker.ack(task_id, token):
                print(f"done {task_id}")
            else:
                print(f"ack rejected for {task_id}: token {token} is stale, another worker owns it now")
    except KeyboardInterrupt:
        print("worker stopped")


if __name__ == "__main__":
    main()
