"""Chaos test: run many charge tasks through real workers while randomly breaking things, then check
that nothing was lost and nothing was charged twice.

Faults injected at random while the run is in progress:
    kill   SIGKILL a worker (and start a replacement)
    stop   SIGSTOP a worker for a while, long enough to lose its lease, then SIGCONT it
    term   SIGTERM a worker (graceful shutdown), then start a replacement
    pause  freeze the Redis container for a few seconds        (with --redis-faults)
    bounce stop the Redis container for a few seconds, restart  (with --redis-faults)
The producer also re-sends some tasks with the same idempotency key, like a client retrying after a timeout.

Afterwards it checks:
    liveness  once faults stop, every task reaches completed or the DLQ (nothing stuck in ready/leased/delayed)
    safety    no task lost; no task charged twice; every completed task charged exactly once;
              re-sent tasks created nothing

Usage: uv run python -m taskq.chaos [--tasks 100000] [--workers 8] [--redis-faults] [--report PATH]

It uses its own Redis database (default DB 14, flushed at the start) and its own ledger file.
"""

import argparse
import os
import random
import signal
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import redis

from taskq.broker import DEAD_QUEUE, DELAYED_SET, LEASED_SET, READY_QUEUE, Broker, NewTask
from taskq.ledger import Ledger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REDIS_URL = "redis://localhost:6379/14"

# Worker settings: short leases so a few seconds of SIGSTOP is enough for another worker to take over
WORKER_ENV = {
    "WORK_SECONDS": "0.005",
    "AFTER_EFFECT_SECONDS": "0.002",
    "LEASE_SECONDS": "1.5",
    "FAIL_RATE": "0.01",
    "BACKOFF_BASE_SECONDS": "0.05",
    "BACKOFF_MAX_SECONDS": "1",
    "POLL_SECONDS": "0.05",
    "SHUTDOWN_GRACE_SECONDS": "1",
    "REDIS_TIMEOUT_SECONDS": "1",
    "LEDGER_GUARD": "dedupe",
    "PYTHONUNBUFFERED": "1",
}

# Lines in worker logs worth counting for the report: label -> substring
LOG_EVENTS = {
    "duplicate charges blocked by the ledger": ": duplicate",
    "stale charges blocked by the ledger": ": stale",
    "stale acks rejected (fencing token)": "ack rejected",
    "leases lost while working": "lease was lost mid-way",
    "tasks reaped after a lease expired": "reaped ",
    "tasks released on graceful shutdown": "released ",
    "handler failures (FAIL_RATE)": "failed ",
    "redis unavailable, worker retried": "redis unavailable",
}


@dataclass
class Worker:
    proc: subprocess.Popen[bytes]
    log_path: Path
    stopped_until: float | None = None  # Set while SIGSTOPped: when to SIGCONT it


@dataclass
class Chaos:
    """Owns the worker processes and injects faults into them and into Redis."""

    redis_url: str
    r: redis.Redis
    ledger_path: Path
    log_dir: Path
    worker_env: dict[str, str]
    rng: random.Random
    redis_faults: bool
    workers: list[Worker] = field(default_factory=list)
    faults: Counter[str] = field(default_factory=Counter)
    _spawned: int = 0

    def spawn(self) -> None:
        env = {**os.environ, **self.worker_env, "REDIS_URL": self.redis_url, "LEDGER_PATH": str(self.ledger_path)}
        log_path = self.log_dir / f"worker-{self._spawned}.log"
        self._spawned += 1
        with log_path.open("w") as log:
            proc = subprocess.Popen([sys.executable, "-m", "taskq.worker"], env=env, stdout=log, stderr=subprocess.STDOUT)
        self.workers.append(Worker(proc, log_path))

    def keep_staffed(self, size: int) -> None:
        """Replace workers that exited (killed or shut down) so the pool stays at size."""
        self.workers = [w for w in self.workers if w.proc.poll() is None]
        while len(self.workers) < size:
            self.spawn()

    def resume_due(self, now: float) -> None:
        for w in self.workers:
            if w.stopped_until is not None and now >= w.stopped_until:
                w.proc.send_signal(signal.SIGCONT)
                w.stopped_until = None

    def inject_one(self) -> None:
        running = [w for w in self.workers if w.stopped_until is None and w.proc.poll() is None]
        # Always leave at least half the pool running, so the run keeps making progress
        can_stop = len(running) > len(self.workers) // 2
        choices = [("kill", 3), ("term", 2)] + ([("stop", 4)] if can_stop else [])
        if self.redis_faults:
            choices += [("pause", 1), ("bounce", 1)]
        kinds, weights = zip(*choices)
        kind = self.rng.choices(kinds, weights)[0]
        if kind in ("kill", "term", "stop") and not running:
            return

        if kind == "kill":
            self.rng.choice(running).proc.send_signal(signal.SIGKILL)
        elif kind == "term":
            self.rng.choice(running).proc.send_signal(signal.SIGTERM)
        elif kind == "stop":
            victim = self.rng.choice(running)
            victim.proc.send_signal(signal.SIGSTOP)
            # Lease is 1.5s: short stops usually survive, longer ones force a takeover
            victim.stopped_until = time.monotonic() + self.rng.uniform(0.5, 4)
        elif kind == "pause":
            compose("pause", "redis")
            time.sleep(self.rng.uniform(1, 3))
            compose("unpause", "redis")
        elif kind == "bounce":
            compose("stop", "redis")
            time.sleep(self.rng.uniform(0.5, 2))
            compose("start", "redis")
            # The container is up before Redis has reloaded the AOF; until then it answers LOADING
            wait_for_redis(self.r)
        self.faults[kind] += 1

    def calm_down(self) -> None:
        """Undo anything still in effect: resume stopped workers."""
        for w in self.workers:
            if w.stopped_until is not None:
                w.proc.send_signal(signal.SIGCONT)
                w.stopped_until = None

    def stop_all(self) -> None:
        for w in self.workers:
            if w.proc.poll() is None:
                w.proc.send_signal(signal.SIGTERM)
        for w in self.workers:
            try:
                w.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                w.proc.kill()


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=PROJECT_ROOT, check=True, capture_output=True)


def wait_for_redis(r: redis.Redis, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if r.ping():
                return
        except redis.RedisError:
            time.sleep(0.2)
    raise SystemExit("Redis did not come back")


def unfinished(r: redis.Redis) -> int:
    """Tasks not yet completed or dead: waiting, running, or backing off."""
    return r.llen(READY_QUEUE) + r.zcard(LEASED_SET) + r.zcard(DELAYED_SET)


def count_log_events(log_dir: Path) -> dict[str, int]:
    counts = dict.fromkeys(LOG_EVENTS, 0)
    for log_path in log_dir.glob("worker-*.log"):
        for line in log_path.read_text(errors="replace").splitlines():
            for label, needle in LOG_EVENTS.items():
                if needle in line:
                    counts[label] += 1
    return counts


@dataclass
class Verdict:
    lines: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    def check(self, ok: bool, description: str, detail: str) -> None:
        self.lines.append(f"- {'PASS' if ok else 'FAIL'}  {description}: {detail}")
        if not ok:
            self.failures.append(description)


def verify(r: redis.Redis, ledger: Ledger, task_ids: list[str], created: int, drained: bool) -> tuple[Verdict, Counter[str]]:
    verdict = Verdict()

    states: Counter[str] = Counter()
    state_of: dict[str, str | None] = {}
    for start in range(0, len(task_ids), 1000):
        pipe = r.pipeline(transaction=False)
        batch = task_ids[start : start + 1000]
        for task_id in batch:
            pipe.hget(f"task:{task_id}", "state")
        for task_id, state in zip(batch, pipe.execute()):
            state_of[task_id] = state
            states[state or "missing"] += 1

    charge_counts = dict(ledger._db.execute("SELECT task_id, COUNT(*) FROM charges GROUP BY task_id").fetchall())
    lost = [t for t, s in state_of.items() if s not in ("completed", "dead")]
    charged_twice = [t for t, n in charge_counts.items() if n > 1]
    completed_uncharged = [t for t, s in state_of.items() if s == "completed" and charge_counts.get(t, 0) != 1]

    verdict.check(drained, "Liveness: queues drained after faults stopped", f"ready+leased+delayed = {unfinished(r)}")
    verdict.check(not lost, "Safety: no task lost", f"{len(lost)} task(s) neither completed nor dead {lost[:5]}")
    verdict.check(not charged_twice, "Safety: no task charged twice", f"{len(charged_twice)} {charged_twice[:5]}")
    verdict.check(
        not completed_uncharged,
        "Safety: every completed task charged exactly once",
        f"{len(completed_uncharged)} violation(s) {completed_uncharged[:5]}",
    )
    verdict.check(
        created == len(task_ids),
        "Safety: re-sent tasks created nothing",
        f"{created} created for {len(task_ids)} unique keys",
    )
    return verdict, states


def main() -> None:
    parser = argparse.ArgumentParser(description="Chaos test the task queue.")
    parser.add_argument("--tasks", type=int, default=100_000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--resend-every", type=int, default=20, help="re-send every Nth task with the same key")
    parser.add_argument("--fault-interval", type=float, default=0.5, help="average seconds between faults")
    parser.add_argument("--redis-faults", action="store_true", help="also pause and restart the Redis container")
    parser.add_argument("--drain-timeout", type=float, default=120, help="seconds to wait for liveness at the end")
    parser.add_argument("--max-seconds", type=float, default=1800, help="stop injecting faults after this long")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--guard", default="dedupe", help="ledger guard for workers (use none to see the test fail)")
    parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    parser.add_argument("--report", type=Path, help="also write the report to this Markdown file")
    args = parser.parse_args()

    seed = args.seed if args.seed is not None else random.randrange(1_000_000)
    rng = random.Random(seed)
    run_dir = PROJECT_ROOT / ".chaos" / datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir.mkdir(parents=True)
    ledger_path = run_dir / "ledger.db"

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    r.flushdb()
    broker = Broker(r)
    ledger = Ledger(ledger_path)
    print(f"seed {seed}, logs and ledger in {run_dir}")

    # Every Nth task is sent twice, as if the producer retried after a timeout
    task_ids = [f"c{i}" for i in range(args.tasks)]
    payload = {"type": "charge", "account": "alice", "amount": 100}
    new_tasks: list[NewTask] = []
    for i, task_id in enumerate(task_ids):
        new_tasks.append(NewTask(task_id, max_retries=5, payload=payload, idempotency_key=f"order-{i}"))
        if i % args.resend_every == 0:
            new_tasks.append(NewTask(f"{task_id}-resent", max_retries=5, payload=payload, idempotency_key=f"order-{i}"))
    started = time.monotonic()
    created = sum(e.created for e in broker.enqueue_many(new_tasks))
    print(f"enqueued {created} tasks ({len(new_tasks) - created} re-sends) in {time.monotonic() - started:.1f}s")

    worker_env = {**WORKER_ENV, "LEDGER_GUARD": args.guard}
    chaos = Chaos(args.redis_url, r, ledger_path, run_dir, worker_env, rng, args.redis_faults)
    started = time.monotonic()
    next_fault = started + rng.expovariate(1 / args.fault_interval)
    next_progress = started
    try:
        chaos.keep_staffed(args.workers)
        # Chaos phase: inject faults until the work is done (or we run out of time)
        while True:
            now = time.monotonic()
            chaos.resume_due(now)
            chaos.keep_staffed(args.workers)
            if now >= next_progress:
                next_progress = now + 5
                remaining: int | None = None
                dead = 0
                try:
                    remaining = unfinished(r)
                    dead = r.llen(DEAD_QUEUE)
                except redis.RedisError as e:
                    # Redis may be mid-fault (e.g. still loading after a restart); just skip this progress line
                    print(f"{now - started:6.0f}s  redis unavailable ({type(e).__name__}), faults {dict(chaos.faults)}")
                if remaining is not None:
                    print(f"{now - started:6.0f}s  unfinished {remaining:>7}  dlq {dead:>4}  faults {dict(chaos.faults)}")
                    if remaining == 0:
                        break
                if now - started > args.max_seconds:
                    break
            if now >= next_fault:
                chaos.inject_one()
                next_fault = time.monotonic() + rng.expovariate(1 / args.fault_interval)
            time.sleep(0.05)
        chaos_seconds = time.monotonic() - started

        # Calm phase: undo faults and give the system time to finish everything (liveness)
        chaos.calm_down()
        wait_for_redis(r)
        chaos.keep_staffed(args.workers)
        deadline = time.monotonic() + args.drain_timeout
        while unfinished(r) > 0 and time.monotonic() < deadline:
            chaos.keep_staffed(args.workers)
            time.sleep(0.5)
        drained = unfinished(r) == 0
    finally:
        chaos.calm_down()
        chaos.stop_all()
    total_seconds = time.monotonic() - started

    verdict, states = verify(r, ledger, task_ids, created, drained)
    events = count_log_events(run_dir)
    caught = events["duplicate charges blocked by the ledger"] + events["stale acks rejected (fencing token)"]
    verdict.check(caught > 0, "Non-vacuous: duplicate executions actually happened and were caught", f"{caught}")

    report = [
        f"# Chaos test report ({datetime.now():%Y-%m-%d %H:%M})",
        "",
        f"- tasks: {args.tasks:,} unique (+{len(new_tasks) - args.tasks:,} re-sent with the same idempotency key)",
        f"- workers: {args.workers}, ledger guard: {args.guard}, redis faults: {'on' if args.redis_faults else 'off'}, seed: {seed}",
        f"- chaos phase: {chaos_seconds:.0f}s, total: {total_seconds:.0f}s, throughput: {args.tasks / total_seconds:,.0f} tasks/s",
        f"- final states: {dict(states)}",
        "",
        "## Faults injected",
        "",
        *[f"- {kind}: {count}" for kind, count in sorted(chaos.faults.items())],
        f"- total: {sum(chaos.faults.values())}",
        "",
        "## What the system had to deal with (from worker logs)",
        "",
        *[f"- {label}: {count:,}" for label, count in events.items()],
        "",
        "## Invariants",
        "",
        *verdict.lines,
        "",
        f"**{'ALL INVARIANTS HELD' if not verdict.failures else 'VIOLATED: ' + ', '.join(verdict.failures)}**",
    ]
    text = "\n".join(report)
    print("\n" + text)
    (run_dir / "report.md").write_text(text + "\n")
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text + "\n")
    sys.exit(1 if verdict.failures else 0)


if __name__ == "__main__":
    main()
