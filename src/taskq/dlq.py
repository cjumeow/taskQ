"""Inspect the dead letter queue and send tasks back for another try.

Usage: uv run python -m taskq.dlq               list dead tasks and why they died
       uv run python -m taskq.dlq retry <id>    move one task back to ready
       uv run python -m taskq.dlq retry --all   move every dead task back to ready
"""

import argparse

import redis

from taskq.broker import Broker


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect and retry dead tasks.")
    sub = parser.add_subparsers(dest="command")
    retry = sub.add_parser("retry", help="move dead tasks back to ready")
    target = retry.add_mutually_exclusive_group(required=True)
    target.add_argument("task_id", nargs="?", help="task to retry")
    target.add_argument("--all", action="store_true", help="retry every dead task")
    args = parser.parse_args()

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    broker = Broker(r)

    if args.command is None:
        dead = broker.dead()
        print(f"{len(dead)} dead task(s)")
        for task_id in dead:
            retries, error = r.hmget(f"task:{task_id}", "retries", "last_error")
            print(f"  {task_id}  retries={retries}  last_error={error}")
        return

    task_ids = broker.dead() if args.all else [args.task_id]
    for task_id in task_ids:
        if broker.retry_dead(task_id):
            print(f"retried {task_id}")
        else:
            print(f"{task_id} is not in the DLQ")


if __name__ == "__main__":
    main()
