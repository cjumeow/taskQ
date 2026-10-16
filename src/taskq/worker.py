"""Worker: lease a task -> do the work -> ack it, reaping expired leases on every loop.

Usage: uv run python -m taskq.worker
Timings can be tuned with environment variables for experiments:
    WORK_SECONDS=15 LEASE_SECONDS=5 uv run python -m taskq.worker
"""

import os
import socket
import time

import redis

from taskq.broker import Broker

WORK_SECONDS = float(os.environ.get("WORK_SECONDS", 3))  # How long the fake work takes
LEASE_SECONDS = float(os.environ.get("LEASE_SECONDS", 10))  # Task is reclaimed if not acked within this
POLL_SECONDS = 0.5  # How long to wait before polling again when the queue is empty


def main() -> None:
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    broker = Broker(r)
    # PID alone can collide across machines, so prefix it with the hostname
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    print(f"worker started, id = {worker_id}, lease = {LEASE_SECONDS}s, work = {WORK_SECONDS}s")

    try:
        while True:
            for task_id in broker.reap():
                print(f"reaped {task_id} (lease expired, back to ready)")

            task_id = broker.lease(worker_id, LEASE_SECONDS)
            if task_id is None:
                time.sleep(POLL_SECONDS)
                continue

            print(f"got {task_id}, working...")
            time.sleep(WORK_SECONDS)  # Pretend to do the work

            if broker.ack(task_id):
                print(f"done {task_id}")
            else:
                print(f"done {task_id}, but lease had already expired (someone may have redone it!)")
    except KeyboardInterrupt:
        print("worker stopped")


if __name__ == "__main__":
    main()
