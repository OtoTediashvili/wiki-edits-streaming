"""Breaking things: the docker commands, and catching Spark mid-batch."""

from __future__ import annotations

import subprocess
from typing import Any

import httpx
import pytest
import respx

from wikistream.drills import stack


@pytest.fixture
def docker(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Records docker commands; answers like Docker would for a few."""
    calls: list[list[str]] = []
    answers: dict[tuple[str, ...], str] = {
        ("compose", "ps", "--status", "running", "--services"): "redpanda\nproducer\n",
        ("compose", "ps", "--all", "-q", "spark"): "c0ffee\n",
        ("compose", "ps", "--all", "-q", "producer"): "",
    }

    def run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        out = answers.get(tuple(cmd[1:]), "")
        if cmd[:2] == ["docker", "inspect"]:
            out = "3\n" if "RestartCount" in cmd[3] else "healthy\n"
        return subprocess.CompletedProcess(cmd, 0, stdout=out, stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    return calls


def test_kill_is_a_sigkill_through_compose(docker: list[list[str]]) -> None:
    stack.kill("producer")
    stack.stop("seaweedfs")
    stack.start("seaweedfs")
    assert docker == [
        ["docker", "compose", "kill", "producer"],
        ["docker", "compose", "stop", "seaweedfs"],
        ["docker", "compose", "start", "seaweedfs"],
    ]


def test_what_runs_and_how_often_it_restarted(docker: list[list[str]]) -> None:
    assert stack.running("producer") and not stack.running("spark")
    assert stack.restarts("spark") == 3
    assert stack.restarts("producer") == 0  # no container at all
    assert stack.healthy("spark")
    assert ["docker", "inspect", "-f", "{{.RestartCount}}", "c0ffee"] in docker


@respx.mock
def test_spark_is_busy_only_while_a_job_runs() -> None:
    apps = respx.get(f"{stack.SPARK_UI}/api/v1/applications")
    jobs = respx.get(f"{stack.SPARK_UI}/api/v1/applications/app-1/jobs")
    apps.respond(json=[])
    assert not stack.spark_busy()
    apps.respond(json=[{"id": "app-1"}])
    jobs.respond(json=[])
    assert not stack.spark_busy()
    jobs.respond(json=[{"jobId": 17, "status": "RUNNING"}])
    assert stack.spark_busy()
    assert jobs.calls.last.request.url.params["status"] == "running"
    apps.mock(side_effect=httpx.ConnectError("refused"))
    assert not stack.spark_busy()  # the job is down or starting


def test_wait_until_gives_up_at_the_timeout() -> None:
    now = [0.0]

    def tick(seconds: float) -> None:
        now[0] += seconds

    answers = iter([False, False, True])
    assert stack.wait_until(lambda: next(answers), 10, every=1, clock=lambda: now[0], sleep=tick)
    assert now[0] == 2
    assert not stack.wait_until(lambda: False, 5, every=1, clock=lambda: now[0], sleep=tick)
