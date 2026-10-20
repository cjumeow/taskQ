"""Worker: lease a task -> do the work while heartbeating -> ack it, or report the failure for retry.

Every loop it also promotes due delayed tasks and reaps expired leases.

Shutdown: SIGTERM or Ctrl+C stops it from taking new tasks. The task in hand gets SHUTDOWN_GRACE_SECONDS
to finish; if it doesn't, it is released back to ready without using up a retry. A second signal exits
immediately.

Redis outages: the worker waits and retries with backoff instead of crashing, and resumes once Redis is back.

Usage: uv run python -m taskq.worker
Behavior can be tuned with environment variables for experiments:
    WORK_SECONDS=15 LEASE_SECONDS=5 FAIL_RATE=0.5 LEDGER_GUARD=none uv run python -m taskq.worker
"""

import os
import random
import signal
import socket
import sys
import threading
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from types import FrameType
from typing import Any, TypeVar, cast

from taskq.broker import REDIS_DOWN, Broker, connect
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
SHUTDOWN_GRACE_SECONDS = float(os.environ.get("SHUTDOWN_GRACE_SECONDS", 10))  # Time the task in hand gets on shutdown
REDIS_RETRY_MAX_SECONDS = 5.0  # Longest wait between attempts to reach Redis during an outage

T = TypeVar("T")


class Shutdown:
    """Tracks a shutdown request from SIGTERM or SIGINT. A second signal exits on the spot."""

    def __init__(self) -> None:
        self._requested = threading.Event()
        self._requested_at = 0.0

    def install(self) -> None:
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)

    def _on_signal(self, signum: int, frame: FrameType | None) -> None:
        if self._requested.is_set():
            print("second signal, exiting immediately")
            sys.stdout.flush()
            # Like a crash: no cleanup, the task in hand stays leased until it is reaped
            os._exit(1)
        self._requested_at = time.monotonic()
        self._requested.set()
        print(f"got {signal.Signals(signum).name}, shutting down after the current task (send again to exit now)")

    def is_requested(self) -> bool:
        return self._requested.is_set()

    def wait(self, timeout: float) -> bool:
        """Sleep up to timeout seconds, waking early on a shutdown request. Returns True if one was made."""
        return self._requested.wait(timeout)

    def grace_left(self) -> float:
        """Seconds the task in hand still has to finish before it must be released."""
        return SHUTDOWN_GRACE_SECONDS - (time.monotonic() - self._requested_at)


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


def run_until_done_or_grace_over(fn: Callable[[], None], shutdown: Shutdown) -> bool:
    """Run fn in a thread and wait for it. Returns False if a shutdown's grace period ran out first.

    fn runs in its own thread because Python can't safely interrupt a running function: this way the
    main thread can stop waiting when the grace period is over. An abandoned fn keeps running until
    the process exits, so it may still apply its side effect after the task was released.
    """
    errors: list[BaseException] = []

    def target() -> None:
        try:
            fn()
        except BaseException as e:
            errors.append(e)

    # daemon=True so an abandoned handler doesn't keep the process alive after its task was released
    thread = threading.Thread(target=target, name="handler", daemon=True)
    thread.start()
    while thread.is_alive():
        thread.join(timeout=0.1)
        if thread.is_alive() and shutdown.is_requested() and shutdown.grace_left() <= 0:
            return False
    if errors:
        raise errors[0]
    return True


def keep_trying(fn: Callable[[], T], give_up_after: float) -> T:
    """Call fn, retrying while Redis is unreachable, for up to give_up_after seconds.

    For ack, fail and release: if they never reach Redis the task runs again, so they are worth waiting for.
    """
    deadline = time.monotonic() + give_up_after
    delay = 0.1
    while True:
        try:
            return fn()
        except REDIS_DOWN as e:
            if time.monotonic() + delay > deadline:
                raise
            print(f"redis unavailable ({type(e).__name__}), retrying in {delay:.1f}s")
            time.sleep(delay)
            delay = min(delay * 2, REDIS_RETRY_MAX_SECONDS)


@contextmanager
def heartbeat(broker: Broker, task_id: str, token: int) -> Generator[threading.Event, None, None]:
    """Keep renewing the lease in a background thread while the body runs.

    Yields an Event that gets set if the lease is lost.
    """
    stop = threading.Event()
    lost = threading.Event()

    def run() -> None:
        # A local monotonic clock is fine here: it only measures how long this worker has failed to renew,
        # it never decides whether the lease has expired (Redis does that)
        last_renewed = time.monotonic()
        # wait() returns True as soon as stop is "set", so the loop exits right after the work finishes
        while not stop.wait(HEARTBEAT_SECONDS):
            try:
                if not broker.heartbeat(task_id, token, LEASE_SECONDS):
                    print(f"lost lease on {task_id}, stopping heartbeat")
                    lost.set()
                    return
                last_renewed = time.monotonic()
            except REDIS_DOWN as e:
                # Can't tell whether the lease still exists; after a full lease without a renewal, assume it doesn't
                if time.monotonic() - last_renewed >= LEASE_SECONDS:
                    print(f"could not renew lease on {task_id} for {LEASE_SECONDS}s, assuming it is lost")
                    lost.set()
                    return
                print(f"heartbeat for {task_id} failed ({type(e).__name__}), retrying")

    # daemon=True so a stuck heartbeat thread never keeps the process alive after shutdown
    thread = threading.Thread(target=run, name=f"heartbeat-{task_id}", daemon=True)
    thread.start()
    try:
        yield lost  # Signal that the lease is lost
    finally:
        stop.set()
        thread.join()


def report_failure(broker: Broker, task_id: str, token: int, error: str) -> None:
    result = keep_trying(
        lambda: broker.fail(task_id, token, error, BACKOFF_BASE_SECONDS, BACKOFF_MAX_SECONDS), LEASE_SECONDS
    )
    match result.status:
        case "retry":
            print(f"failed {task_id}: {error} -> retry #{result.retries} in {result.delay:.1f}s")
        case "dead":
            print(f"failed {task_id}: {error} -> out of retries after {result.retries} failures, moved to DLQ")
        case "stale":
            print(f"failed {task_id}: {error} -> report rejected, token {token} is stale")


def work_once(broker: Broker, ledger: Ledger, worker_id: str, shutdown: Shutdown) -> None:
    """One pass of the worker loop: housekeeping, then lease and process at most one task."""
    for task_id in broker.promote():
        print(f"promoted {task_id} (delay is over, back to ready)")
    for task_id, outcome in broker.reap():
        if outcome == "dead":
            print(f"reaped {task_id} (lease expired, out of retries, moved to DLQ)")
        else:
            print(f"reaped {task_id} (lease expired, back to ready)")

    leased = broker.lease(worker_id, LEASE_SECONDS)
    if leased is None:
        shutdown.wait(POLL_SECONDS)
        return
    task_id, token = leased
    payload = broker.payload(task_id)

    print(f"got {task_id} (token {token}), working...")
    try:
        with heartbeat(broker, task_id, token) as lost:
            finished = run_until_done_or_grace_over(
                lambda: handle(ledger, task_id, payload, token, worker_id), shutdown
            )
    except Exception as e:
        # Catch everything so one bad task can't kill the worker
        report_failure(broker, task_id, token, f"{type(e).__name__}: {e}")
        return

    if not finished:
        if keep_trying(lambda: broker.release(task_id, token), LEASE_SECONDS):
            print(f"shutdown grace period is over, released {task_id} back to ready")
        else:
            print(f"shutdown grace period is over, but {task_id} was no longer ours to release")
        return

    if lost.is_set():
        print(f"finished {task_id}, but the lease was lost mid-way")
    # Always try to ack: Redis is the single judge, and rejects it if the token is stale
    if keep_trying(lambda: broker.ack(task_id, token), LEASE_SECONDS):
        print(f"done {task_id}")
    else:
        print(f"ack rejected for {task_id}: token {token} is stale, another worker owns it now")


def main() -> None:
    if LEDGER_GUARD not in GUARDS:
        raise SystemExit(f"LEDGER_GUARD must be one of {', '.join(GUARDS)}, got {LEDGER_GUARD!r}")

    shutdown = Shutdown()
    shutdown.install()
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

    redis_retry_delay = 0.1
    while not shutdown.is_requested():
        try:
            work_once(broker, ledger, worker_id, shutdown)
            redis_retry_delay = 0.1
        except REDIS_DOWN as e:
            # Don't crash: wait for Redis to come back. Whatever we held will be reaped if we were gone too long
            print(f"redis unavailable ({type(e).__name__}: {e}), retrying in {redis_retry_delay:.1f}s")
            shutdown.wait(redis_retry_delay)
            redis_retry_delay = min(redis_retry_delay * 2, REDIS_RETRY_MAX_SECONDS)
    print("worker stopped")


if __name__ == "__main__":
    main()
