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

from taskq.broker import Broker

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
def start_worker(r: redis.Redis, tmp_path: Path) -> Iterator[StartWorker]:
    """Start real worker processes on the test DB. Keyword arguments become environment variables."""
    workers: list[WorkerProcess] = []

    def start(**env_overrides: float | str) -> WorkerProcess:
        env = {
            **os.environ,
            "REDIS_URL": TEST_REDIS_URL,
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
        return worker

    yield start

    for worker in workers:
        if worker.is_alive():
            worker.proc.send_signal(signal.SIGCONT)  # In case a test left it stopped
            worker.proc.kill()
        worker.proc.wait()
