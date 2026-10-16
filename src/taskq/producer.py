"""Simplest possible producer: push N task IDs onto queue:ready.

Usage: uv run python -m taskq.producer [number of tasks, default 5]
"""

import sys
import time

import redis

READY_QUEUE = "queue:ready"


def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # Timestamp prefix keeps IDs unique across multiple producer runs
    batch = int(time.time())
    task_ids = [f"task-{batch}-{i}" for i in range(count)]

    r.lpush(READY_QUEUE, *task_ids)
    print(f"enqueued {count} tasks, queue length = {r.llen(READY_QUEUE)}")


if __name__ == "__main__":
    main()
