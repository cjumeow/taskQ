"""Producer: enqueue N tasks, optionally delayed, optionally as charges against the ledger.

Usage: uv run python -m taskq.producer [count] [--delay SECONDS] [--max-retries N]
                                       [--charge AMOUNT] [--idempotency-prefix PREFIX]

With --idempotency-prefix, task i gets the key PREFIX-i, so running the same command twice
(like a producer retrying after a timeout) creates nothing the second time.
"""

import argparse
import time

from taskq.broker import Broker, connect


def main() -> None:
    parser = argparse.ArgumentParser(description="Enqueue test tasks.")
    parser.add_argument("count", type=int, nargs="?", default=5, help="number of tasks (default 5)")
    parser.add_argument("--delay", type=float, default=0, help="seconds before the tasks may run (default 0)")
    parser.add_argument("--max-retries", type=int, default=3, help="retries before a task goes to the DLQ (default 3)")
    parser.add_argument("--charge", type=int, metavar="AMOUNT", help="make each task charge this amount to the ledger")
    parser.add_argument("--idempotency-prefix", metavar="PREFIX", help="give task i the idempotency key PREFIX-i")
    args = parser.parse_args()

    r = connect()
    broker = Broker(r)
    payload = {"type": "charge", "account": "alice", "amount": args.charge} if args.charge is not None else None

    # Timestamp prefix keeps IDs unique across multiple producer runs
    batch = int(time.time())
    created = 0
    for i in range(args.count):
        key = f"{args.idempotency_prefix}-{i}" if args.idempotency_prefix else None
        result = broker.enqueue(
            f"{batch}-{i}", delay=args.delay, max_retries=args.max_retries, payload=payload, idempotency_key=key
        )
        created += result.created

    when = f"in {args.delay}s" if args.delay > 0 else "now"
    skipped = args.count - created
    print(f"enqueued {created} tasks to run {when}, max_retries = {args.max_retries}", end="")
    print(f", skipped {skipped} already enqueued with the same idempotency key" if skipped else "")


if __name__ == "__main__":
    main()
