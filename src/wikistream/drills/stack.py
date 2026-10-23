"""Breaking things: `docker compose` on the services of this project, and
a look at the Spark job's REST API to catch it in the middle of a batch."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Callable

import httpx

SPARK_UI = "http://localhost:4040"


def compose(*args: str) -> str:
    done = subprocess.run(
        ["docker", "compose", *args], capture_output=True, text=True, check=True, timeout=180
    )
    return done.stdout


def _inspect(service: str, template: str) -> str:
    container = compose("ps", "--all", "-q", service).strip()
    if not container:
        return ""
    done = subprocess.run(
        ["docker", "inspect", "-f", template, container],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return done.stdout.strip()


def kill(service: str) -> None:
    """SIGKILL: no shutdown hook runs, like a crash or a power cut."""
    compose("kill", service)


def stop(service: str) -> None:
    compose("stop", service)


def start(service: str) -> None:
    compose("start", service)


def running(service: str) -> bool:
    return service in compose("ps", "--status", "running", "--services").split()


def healthy(service: str) -> bool:
    return _inspect(service, "{{if .State.Health}}{{.State.Health.Status}}{{end}}") == "healthy"


def restarts(service: str) -> int:
    """How often Docker restarted the container after it exited on its own."""
    count = _inspect(service, "{{.RestartCount}}")
    return int(count) if count.isdigit() else 0


def spark_busy(base: str = SPARK_UI) -> bool:
    """Whether the Spark job is running a batch right now (a Spark job is
    active). False when the UI doesn't answer: the job is down or starting."""
    try:
        apps = httpx.get(f"{base}/api/v1/applications", timeout=2).json()
        if not apps:
            return False
        jobs = httpx.get(
            f"{base}/api/v1/applications/{apps[0]['id']}/jobs",
            params={"status": "running"},
            timeout=2,
        ).json()
    except (httpx.HTTPError, ValueError, KeyError, IndexError):
        return False
    return bool(jobs)


def wait_until(
    condition: Callable[[], bool],
    timeout_seconds: float,
    every: float = 0.5,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Poll until `condition` holds; False if it didn't within the timeout."""
    deadline = clock() + timeout_seconds
    while True:
        if condition():
            return True
        if clock() >= deadline:
            return False
        sleep(every)
