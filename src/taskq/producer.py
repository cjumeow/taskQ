"""Producer: enqueue N tasks, optionally delayed.

Usage: uv run python -m taskq.producer [count] [--delay SECONDS] [--max-retries N]
"""

import argparse
import time

import redis

from taskq.broker import Broker


def main() -> None:
    parser = argparse.ArgumentParser(description="Enqueue test tasks.")
    parser.add_argument("count", type=int, nargs="?", default=5, help="number of tasks (default 5)")
    parser.add_argument("--delay", type=float, default=0, help="seconds before the tasks may run (default 0)")
    parser.add_argument("--max-retries", type=int, default=3, help="retries before a task goes to the DLQ (default 3)")
    args = parser.parse_args()

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    broker = Broker(r)

    # Timestamp prefix keeps IDs unique across multiple producer runs
    batch = int(time.time())
    for i in range(args.count):
        broker.enqueue(f"{batch}-{i}", delay=args.delay, max_retries=args.max_retries)

    when = f"in {args.delay}s" if args.delay > 0 else "now"
    print(f"enqueued {args.count} tasks to run {when}, max_retries = {args.max_retries}")


if __name__ == "__main__":
    main()
