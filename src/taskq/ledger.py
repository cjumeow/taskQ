"""A stand-in for a downstream system the worker has side effects on: a SQLite ledger of charges.

It deliberately lives outside Redis. Fencing tokens keep the queue's own state in Redis correct,
but they cannot take back a charge that was already written here. A guard decides what the ledger
does about the duplicate executions that at-least-once delivery allows:

    none    no protection; a task that runs twice charges twice
    dedupe  record each applied task in the same transaction as the charge; a second run is a no-op
    fence   remember the largest fencing token seen per task; reject writes carrying an older one

Usage: uv run python -m taskq.ledger   summarize the ledger and list tasks charged more than once
"""

import os
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Literal, get_args

Guard = Literal["none", "dedupe", "fence"]
GUARDS: tuple[Guard, ...] = get_args(Guard)

# charged: the charge was written; duplicate: dedupe found this task already applied; stale: fence rejected an old token
Outcome = Literal["charged", "duplicate", "stale"]

DEFAULT_LEDGER_PATH = "ledger.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS charges (
    id         INTEGER PRIMARY KEY,
    task_id    TEXT    NOT NULL,
    account    TEXT    NOT NULL,
    amount     INTEGER NOT NULL,
    token      INTEGER NOT NULL,
    worker     TEXT    NOT NULL,
    charged_at REAL    NOT NULL
);
-- Guard "dedupe": one row per task whose charge has been applied
CREATE TABLE IF NOT EXISTS processed (
    task_id TEXT PRIMARY KEY
);
-- Guard "fence": the largest fencing token that has written for each task
CREATE TABLE IF NOT EXISTS fences (
    task_id   TEXT    PRIMARY KEY,
    max_token INTEGER NOT NULL
);
"""


@dataclass
class Summary:
    charges: int
    tasks: int
    # task id -> number of charges, only for tasks charged more than once
    duplicated: dict[str, int]


class Ledger:
    def __init__(self, path: str | os.PathLike[str] | None = None) -> None:
        path = path or os.environ.get("LEDGER_PATH", DEFAULT_LEDGER_PATH)
        # isolation_level=None turns off implicit transactions, so the explicit BEGIN below is the only one.
        # timeout makes concurrent workers wait for the write lock instead of failing.
        # (Not WAL: switching a fresh file to WAL from several processes at once can fail with "database is locked")
        self._db = sqlite3.connect(path, timeout=10, isolation_level=None)
        self._db.executescript(SCHEMA)

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        # IMMEDIATE takes the write lock up front, so two workers can't both pass a check before either writes
        self._db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        self._db.execute("COMMIT")

    def charge(self, task_id: str, account: str, amount: int, token: int, worker: str, guard: Guard) -> Outcome:
        """Write a charge for task_id, applying guard in the same transaction as the write."""
        with self._transaction():
            if guard == "dedupe":
                # The primary key makes the second application of a task a no-op instead of a second charge
                inserted = self._db.execute("INSERT OR IGNORE INTO processed (task_id) VALUES (?)", (task_id,))
                if inserted.rowcount == 0:
                    return "duplicate"
            elif guard == "fence":
                row = self._db.execute("SELECT max_token FROM fences WHERE task_id = ?", (task_id,)).fetchone()
                # Only catches an older lease holder writing AFTER a newer one has; if it wrote first, both get through
                if row is not None and token < row[0]:
                    return "stale"
                self._db.execute(
                    "INSERT INTO fences (task_id, max_token) VALUES (?, ?) "
                    "ON CONFLICT (task_id) DO UPDATE SET max_token = excluded.max_token",
                    (task_id, token),
                )
            self._db.execute(
                "INSERT INTO charges (task_id, account, amount, token, worker, charged_at) VALUES (?, ?, ?, ?, ?, ?)",
                (task_id, account, amount, token, worker, time.time()),
            )
        return "charged"

    def charges_for(self, task_id: str) -> list[tuple[int, str]]:
        """(token, worker) of every charge written for task_id, oldest first."""
        return self._db.execute(
            "SELECT token, worker FROM charges WHERE task_id = ? ORDER BY id", (task_id,)
        ).fetchall()

    def summary(self) -> Summary:
        charges, tasks = self._db.execute("SELECT COUNT(*), COUNT(DISTINCT task_id) FROM charges").fetchone()
        duplicated = dict(
            self._db.execute(
                "SELECT task_id, COUNT(*) FROM charges GROUP BY task_id HAVING COUNT(*) > 1 ORDER BY task_id"
            ).fetchall()
        )
        return Summary(charges, tasks, duplicated)


def main() -> None:
    summary = Ledger().summary()
    print(f"{summary.charges} charge(s) for {summary.tasks} task(s)")
    if summary.duplicated:
        print(f"{len(summary.duplicated)} task(s) charged more than once:")
        for task_id, count in summary.duplicated.items():
            print(f"  {task_id}  charged {count} times")
    else:
        print("no task was charged more than once")


if __name__ == "__main__":
    main()
