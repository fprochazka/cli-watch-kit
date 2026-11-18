"""Two watchers of one target from two Claude Code sessions: each writes its own base dir (its session's scratchpad)
and its own stream, and both share one resource of one real daemon process.

Only a second watcher into the same base dir is refused; different base dirs always work, also across a daemon
restart, a drain, one watcher ending first, and one watcher starting later.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
from pathlib import Path
from typing import Any

import pytest

from cli_watch_kit.app import CodeStamp, WatchApp
from cli_watch_kit.daemon.lifecycle import daemon_running, ensure_daemon
from cli_watch_kit.daemon.transport import request as daemon_request
from fakeapp.app import APP
from fakeapp.source import World
from test_daemon import DaemonEnv, _watch, daemon_env, wait_for

__all__ = ["daemon_env"]  # the fixture, imported for pytest


def _start(env: DaemonEnv, base: Path, *extra: str) -> subprocess.Popen[str]:
    return _watch(["item", "a", "--events-stream=stdout", "--base-dir", str(base), *extra], env.env())


def _events(base: Path) -> list[dict[str, Any]]:
    path = base / "item" / "a" / "events.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def _kinds(base: Path) -> list[str]:
    return [e["kind"] for e in _events(base)]


def _wait_kinds(base: Path, kinds: list[str], timeout: float = 20) -> None:
    wait_for(lambda: _kinds(base) == kinds, timeout, f"{kinds} in {base} (have {_kinds(base)})")


def _finish(watch: subprocess.Popen[str], base: Path) -> list[dict[str, Any]]:
    """Wait for the watcher's end; check that its stream and its changelog agree and that its seq has no gap."""
    out, err = watch.communicate(timeout=30)
    assert watch.returncode == 0, err
    events = _events(base)
    assert [json.loads(line) for line in out.splitlines()] == events, err
    assert [e["seq"] for e in events] == list(range(1, len(events) + 1)), events
    return events


def _comparable(events: list[dict[str, Any]]) -> list[tuple[str, Any]]:
    return [(e["kind"], {k: v for k, v in e["data"].items() if k != "snapshot_hash"}) for e in events]


def _status(env: DaemonEnv) -> dict[str, Any]:
    return daemon_request(env.files, {"op": "status"})


def _one_resource_with(env: DaemonEnv, subscriptions: int) -> bool:
    resources = [r for r in _status(env)["resources"] if r["resource"] == "item a"]
    return len(resources) == 1 and resources[0]["subscriptions"] == subscriptions


def test_two_watchers_in_two_base_dirs_share_one_resource_and_each_gets_every_event(daemon_env: DaemonEnv) -> None:
    one, two = daemon_env.root / "one", daemon_env.root / "two"
    first, second = _start(daemon_env, one, "--timeout=8s"), _start(daemon_env, two, "--timeout=8s")
    _wait_kinds(one, ["watch_started"])
    _wait_kinds(two, ["watch_started"])
    wait_for(lambda: _one_resource_with(daemon_env, 2), 10, "one resource with two subscriptions")
    daemon_env.world(World(items={"a": {"status": "review", "notes": 1}}))
    _wait_kinds(one, ["watch_started", "status_changed", "note_added"])
    _wait_kinds(two, ["watch_started", "status_changed", "note_added"])
    a, b = _finish(first, one), _finish(second, two)
    assert _comparable(a) == _comparable(b)
    assert [e["kind"] for e in a] == ["watch_started", "status_changed", "note_added", "watch_ended"]
    for base in (one, two):
        assert json.loads((base / "item" / "a" / "baseline.json").read_text())["snapshot"] == {
            "status": "review",
            "notes": 1,
        }


@pytest.mark.skipif(os.name == "nt", reason="uses SIGSTOP")
@pytest.mark.parametrize("how", ["restart", "drain"])
def test_after_a_restart_or_a_drain_each_of_two_watchers_gets_the_gap_once(daemon_env: DaemonEnv, how: str) -> None:
    one, two = daemon_env.root / "one", daemon_env.root / "two"
    first, second = _start(daemon_env, one, "--timeout=10s"), _start(daemon_env, two, "--timeout=10s")
    _wait_kinds(one, ["watch_started"])
    _wait_kinds(two, ["watch_started"])
    hello = daemon_request(daemon_env.files, {"op": "hello"})
    # Freeze both watchers, so neither comes back before the world changed.
    for watch in (first, second):
        os.kill(watch.pid, signal.SIGSTOP)
    if how == "restart":
        os.kill(hello["pid"], signal.SIGKILL)
        wait_for(lambda: not daemon_running(daemon_env.files), 10, "the daemon to die")
    else:
        newer = WatchApp(APP.name, APP.source, APP.daemon_module, APP.app_protocol, "1.2.4", APP.status_prefix, "t")
        newer.__dict__["code"] = CodeStamp("newer", hello["code_mtime"] + 100)
        assert ensure_daemon(newer)["pid"] != hello["pid"]
    daemon_env.world(World(items={"a": {"status": "review", "notes": 1}}))
    os.kill(first.pid, signal.SIGCONT)
    # The first watcher brings the resource back from its baseline; the second comes later, to a resource that the
    # new daemon already polls, and still gets the gap from its own baseline.
    _wait_kinds(one, ["watch_started", "watch_started", "status_changed", "note_added"])
    os.kill(second.pid, signal.SIGCONT)
    a, b = _finish(first, one), _finish(second, two)
    expected = ["watch_started", "watch_started", "status_changed", "note_added", "watch_ended"]
    assert [e["kind"] for e in a] == expected
    assert [e["kind"] for e in b] == expected
    assert _comparable(a)[2:4] == _comparable(b)[2:4]


def test_when_one_watcher_ends_the_other_keeps_the_resource_and_its_events(daemon_env: DaemonEnv) -> None:
    one, two = daemon_env.root / "one", daemon_env.root / "two"
    short, long = _start(daemon_env, one, "--timeout=30s"), _start(daemon_env, two, "--timeout=8s")
    _wait_kinds(one, ["watch_started"])
    _wait_kinds(two, ["watch_started"])
    wait_for(lambda: _one_resource_with(daemon_env, 2), 10, "one resource with two subscriptions")
    short.send_signal(signal.SIGTERM)
    ended = _finish(short, one)
    assert ended[-1]["data"]["reason"] == "signal SIGTERM"
    wait_for(lambda: _one_resource_with(daemon_env, 1), 10, "the resource to keep its second subscription")
    daemon_env.world(World(items={"a": {"status": "review"}}))
    _wait_kinds(two, ["watch_started", "status_changed"])
    assert [e["kind"] for e in _finish(long, two)] == ["watch_started", "status_changed", "watch_ended"]
    assert _kinds(one) == ["watch_started", "watch_ended"]


def test_a_later_watcher_gets_watch_started_with_the_current_state_and_no_backlog(daemon_env: DaemonEnv) -> None:
    one, two = daemon_env.root / "one", daemon_env.root / "two"
    early = _start(daemon_env, one, "--timeout=10s")
    _wait_kinds(one, ["watch_started"])
    daemon_env.world(World(items={"a": {"status": "review", "notes": 1}}))
    _wait_kinds(one, ["watch_started", "status_changed", "note_added"])
    late = _start(daemon_env, two, "--timeout=6s")
    _wait_kinds(two, ["watch_started"])
    started = _events(two)[0]
    assert started["data"]["summary"] == {"status": "review", "notes": 1}
    assert json.loads((two / "item" / "a" / "baseline.json").read_text())["snapshot"] == {
        "status": "review",
        "notes": 1,
    }
    wait_for(lambda: _one_resource_with(daemon_env, 2), 10, "one resource with two subscriptions")
    daemon_env.world(World(items={"a": {"status": "review", "notes": 2}}))
    _wait_kinds(two, ["watch_started", "note_added"])
    assert [e["kind"] for e in _finish(late, two)] == ["watch_started", "note_added", "watch_ended"]
    assert [e["kind"] for e in _finish(early, one)] == [
        "watch_started",
        "status_changed",
        "note_added",
        "note_added",
        "watch_ended",
    ]


def test_a_second_watcher_into_the_same_base_dir_is_refused_while_another_dir_works(daemon_env: DaemonEnv) -> None:
    one = daemon_env.root / "one"
    first = _start(daemon_env, one, "--timeout=4s")
    _wait_kinds(one, ["watch_started"])
    refused = _start(daemon_env, one, "--timeout=4s")
    out, err = refused.communicate(timeout=30)
    assert refused.returncode == 1 and out == ""
    assert "another watcher already watches item a" in err
    other = _start(daemon_env, daemon_env.root / "two", "--timeout=2s")
    assert [e["kind"] for e in _finish(other, daemon_env.root / "two")] == ["watch_started", "watch_ended"]
    assert [e["kind"] for e in _finish(first, one)] == ["watch_started", "watch_ended"]
