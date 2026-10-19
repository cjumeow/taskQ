"""Shared fixtures. Tests run against the real Redis from docker compose, on DB 15 so they never touch DB 0."""

import os
import signal
import socket
import subprocess
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import redis
from helpers import wait_until

from taskq.broker import Broker
from taskq.ledger import Ledger

TEST_REDIS_URL = "redis://localhost:6379/15"


@pytest.fixture
def r() -> Iterator[redis.Redis]:
    client = redis.Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.exit("Redis is not running, start it with: docker compose up -d", returncode=1)
    client.flushdb()
    yield client
    client.flushdb()


@pytest.fixture
def broker(r: redis.Redis) -> Broker:
    return Broker(r)


@dataclass
class WorkerProcess:
    proc: subprocess.Popen[bytes]
    log_path: Path

    @property
    def pid(self) -> int:
        return self.proc.pid

    @property
    def worker_id(self) -> str:
        # Must match how the worker names itself
        return f"{socket.gethostname()}:{self.proc.pid}"

    def log(self) -> str:
        return self.log_path.read_text()

    def is_alive(self) -> bool:
        return self.proc.poll() is None


StartWorker = Callable[..., WorkerProcess]


@pytest.fixture
def ledger_path(tmp_path: Path) -> Path:
    """Each test gets its own ledger file, shared by every worker it starts."""
    return tmp_path / "ledger.db"


@pytest.fixture
def ledger(ledger_path: Path) -> Ledger:
    return Ledger(ledger_path)


@pytest.fixture
def start_worker(r: redis.Redis, tmp_path: Path, ledger_path: Path) -> Iterator[StartWorker]:
    """Start real worker processes on the test DB. Keyword arguments become environment variables."""
    workers: list[WorkerProcess] = []

    def start(**env_overrides: float | str) -> WorkerProcess:
        env = {
            **os.environ,
            "REDIS_URL": TEST_REDIS_URL,
            "LEDGER_PATH": str(ledger_path),
            "PYTHONUNBUFFERED": "1",
            "POLL_SECONDS": "0.05",
            "WORK_SECONDS": "0",
            **{name: str(value) for name, value in env_overrides.items()},
        }
        log_path = tmp_path / f"worker-{len(workers)}.log"
        # Log to a file, not a pipe: nobody reads the pipe while the test runs, so a full pipe would block the worker
        with log_path.open("w") as log:
            # Run the interpreter directly (no uv or shell wrapper) so proc.pid is the worker itself and signals reach it
            proc = subprocess.Popen(
                [sys.executable, "-m", "taskq.worker"], env=env, stdout=log, stderr=subprocess.STDOUT
            )
        worker = WorkerProcess(proc, log_path)
        workers.append(worker)
        # Fail loudly here if the worker crashes on startup, instead of as a confusing assertion later
        wait_until(lambda: "worker started" in worker.log() or not worker.is_alive())
        assert worker.is_alive(), f"worker exited on startup:\n{worker.log()}"
        return worker

    yield start

    for worker in workers:
        if worker.is_alive():
            worker.proc.send_signal(signal.SIGCONT)  # In case a test left it stopped
            worker.proc.kill()
        worker.proc.wait()
