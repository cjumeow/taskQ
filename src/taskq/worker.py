"""Worker: lease a task -> do the work while heartbeating -> ack it, reaping expired leases on every loop.

Usage: uv run python -m taskq.worker
Timings can be tuned with environment variables for experiments:
    WORK_SECONDS=15 LEASE_SECONDS=5 uv run python -m taskq.worker
"""

import os
import socket
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager

import redis

from taskq.broker import Broker

WORK_SECONDS = float(os.environ.get("WORK_SECONDS", 3))  # How long the fake work takes
LEASE_SECONDS = float(os.environ.get("LEASE_SECONDS", 10))  # Task is reclaimed if not acked within this
HEARTBEAT_SECONDS = LEASE_SECONDS / 3  # Renew often enough to survive two missed heartbeats
POLL_SECONDS = 0.5  # How long to wait before polling again when the queue is empty


@contextmanager
def heartbeat(broker: Broker, task_id: str, worker_id: str) -> Generator[threading.Event, None, None]:
    """Keep renewing the lease in a background thread while the body runs.

    Yields an Event that gets set if the lease is lost.
    """
    stop = threading.Event()
    lost = threading.Event()

    def run() -> None:
        # wait() returns True as soon as stop is "set", so the loop exits right after the work finishes
        while not stop.wait(HEARTBEAT_SECONDS):
            if not broker.heartbeat(task_id, worker_id, LEASE_SECONDS):
                print(f"lost lease on {task_id}, stopping heartbeat")
                lost.set()
                return

    # daemon=True so a stuck heartbeat thread never keeps the process alive after Ctrl+C
    thread = threading.Thread(target=run, name=f"heartbeat-{task_id}", daemon=True)
    thread.start()
    try:
        yield lost
    finally:
        stop.set()
        thread.join()

def main() -> None:
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    broker = Broker(r)
    # PID alone can collide across machines, so prefix it with the hostname
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    print(
        f"worker started, id = {worker_id}, lease = {LEASE_SECONDS}s, "
        f"heartbeat = {HEARTBEAT_SECONDS:.1f}s, work = {WORK_SECONDS}s"
    )

    try:
        while True:
            for task_id in broker.reap():
                print(f"reaped {task_id} (lease expired, back to ready)")

            task_id = broker.lease(worker_id, LEASE_SECONDS)
            if task_id is None:
                time.sleep(POLL_SECONDS)
                continue

            print(f"got {task_id}, working...")
            with heartbeat(broker, task_id, worker_id) as lost:
                time.sleep(WORK_SECONDS)  # Pretend to do the work

            if lost.is_set():
                print(f"done {task_id}, but the lease was lost mid-way (someone may have redone it!)")
            # ack still runs after a lost lease: deliberate hole, closed by fencing tokens in M2
            if broker.ack(task_id):
                print(f"done {task_id}")
            else:
                print(f"done {task_id}, but lease had already expired (someone may have redone it!)")
    except KeyboardInterrupt:
        print("worker stopped")


if __name__ == "__main__":
    main()
