"""The daemon with real processes: spawn, the single-instance lock, leases, idle shutdown, restart, the version
handshake, params over the socket, batching, and the fake app's watch command end to end.

Each test gets a short private state dir (Unix socket paths are limited to about 104 bytes) and a world file for the
fake source, and stops every daemon it started.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from cli_watch_kit.app import LIB_PROTOCOL, CodeStamp, WatchApp
from cli_watch_kit.daemon.lifecycle import daemon_running, ensure_daemon, needs_drain, spawn_daemon, stop_daemon
from cli_watch_kit.daemon.transport import DaemonFiles, DaemonStartFailed, DaemonUnavailable, _connect
from cli_watch_kit.daemon.transport import request as daemon_request
from cli_watch_kit.source import Target
from fakeapp.app import APP
from fakeapp.source import World, write_world

TESTS = str(Path(__file__).resolve().parent)


@dataclass
class DaemonEnv:
    files: DaemonFiles
    spec: Path
    calls: Path
    root: Path

    def world(self, world: World) -> None:
        write_world(self.spec, world)

    def env(self, **extra: str) -> dict[str, str]:
        return {**os.environ, **extra}

    def started_count(self) -> int:
        return self.files.log.read_text().count("daemon fakeapp 1.2.3 (cli-watch-kit")

    def calls_made(self) -> list[dict[str, Any]]:
        if not self.calls.exists():
            return []
        return [json.loads(line) for line in self.calls.read_text().splitlines()]


def wait_for(condition: Callable[[], bool], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.fixture(params=["unix", "tcp"] if os.name != "nt" else ["tcp"])
def daemon_env(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Iterator[DaemonEnv]:
    root = Path(tempfile.mkdtemp(prefix="cwk-"))
    state = root / "s"
    spec, calls = root / "world.json", root / "calls.jsonl"
    write_world(spec, World(items={"a": {"status": "open"}, "b": {"status": "open"}}))
    monkeypatch.setenv("FAKEAPP_STATE_DIR", str(state))
    monkeypatch.setenv("FAKEAPP_BASE_DIR", str(root / "base"))
    monkeypatch.setenv("FAKEAPP_IDLE_TIMEOUT", "2")
    monkeypatch.setenv("FAKEAPP_LEASE_TIMEOUT", "2")
    monkeypatch.setenv("FAKEAPP_CLIENT_POLL_MS", "200")
    monkeypatch.setenv("FAKE_SOURCE_SPEC", str(spec))
    monkeypatch.setenv("FAKE_SOURCE_LOG", str(calls))
    monkeypatch.setenv("FAKE_SOURCE_INTERVAL", "0.5")
    monkeypatch.setenv("PYTHONPATH", TESTS + os.pathsep + os.environ.get("PYTHONPATH", ""))
    if request.param == "tcp":
        monkeypatch.setenv("FAKEAPP_TRANSPORT", "tcp")
    else:
        monkeypatch.delenv("FAKEAPP_TRANSPORT", raising=False)
    files = DaemonFiles.for_app(APP)
    env = DaemonEnv(files, spec, calls, root)
    try:
        yield env
    finally:
        stop_daemon(files)
        assert not daemon_running(files), "a daemon survived the test"
        shutil.rmtree(root, ignore_errors=True)


CLIENT = (
    "from cli_watch_kit.daemon.lifecycle import ensure_daemon; from fakeapp.app import APP;"
    " print(ensure_daemon(APP)['pid'])"
)


def _subscribe(files: DaemonFiles, client_id: str, ident: str = "a", **params: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "op": "subscribe",
        "client_id": client_id,
        "target": Target("item", ident).to_wire(),
        "options": {"events": ["status_changed", "note_added"]},
    }
    if params:
        payload["params"] = params
    return daemon_request(files, payload)


def _watch(args: list[str], env: dict[str, str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-c", "from fakeapp.cli import main; main()", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        env=env,
    )


def test_the_socket_name_carries_both_protocol_versions(daemon_env: DaemonEnv) -> None:
    assert daemon_env.files.socket.name == f"daemon-l{LIB_PROTOCOL}-a1.sock"
    other = DaemonFiles.for_app(WatchApp("x", APP.source, "x.daemon", 7, "0", "", "x"), daemon_env.files.state_dir)
    assert other.lock.name == f"daemon-l{LIB_PROTOCOL}-a7.lock"


def test_concurrent_clients_start_exactly_one_daemon(daemon_env: DaemonEnv) -> None:
    clients = [
        subprocess.Popen([sys.executable, "-c", CLIENT], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for _ in range(6)
    ]
    pids = set()
    for client in clients:
        out, err = client.communicate(timeout=30)
        assert client.returncode == 0, err
        pids.add(int(out.strip()))
    assert len(pids) == 1
    assert daemon_running(daemon_env.files)
    assert daemon_env.started_count() == 1


def test_hello_reports_the_versions_and_the_code_stamp(daemon_env: DaemonEnv) -> None:
    hello = ensure_daemon(APP)
    mine = APP.identity()
    assert {k: hello[k] for k in mine} == mine
    assert hello["app"] == "fakeapp" and hello["app_version"] == "1.2.3" and hello["lib_protocol"] == LIB_PROTOCOL


@pytest.mark.skipif(os.name == "nt", reason="only a Unix socket has a path length limit")
def test_a_socket_path_too_long_for_a_unix_socket_fails_at_once_with_the_reason(tmp_path: Path) -> None:
    files = DaemonFiles.for_app(APP, tmp_path / ("d" * 120))
    with pytest.raises(DaemonUnavailable, match=r"above the 10[37]-byte limit of Unix sockets; set FAKEAPP_STATE_DIR"):
        ensure_daemon(APP, files)


def test_a_second_daemon_exits_at_once(daemon_env: DaemonEnv) -> None:
    first = ensure_daemon(APP)
    second = spawn_daemon(APP, daemon_env.files)
    assert second.wait(timeout=15) == 0
    assert ensure_daemon(APP)["pid"] == first["pid"]
    assert "another daemon holds" in daemon_env.files.log.read_text()


def test_leases_expire_and_the_daemon_shuts_down_when_idle(daemon_env: DaemonEnv) -> None:
    ensure_daemon(APP)
    first, second = _subscribe(daemon_env.files, "client-a"), _subscribe(daemon_env.files, "client-b")
    assert first["ok"] and second["ok"]

    def started() -> bool:
        poll = {"op": "poll", "client_id": "client-a", "cursors": {first["subscription_id"]: 0}}
        return bool(daemon_request(daemon_env.files, poll)["events"])

    wait_for(started, 15, "watch_started")
    status = daemon_request(daemon_env.files, {"op": "status"})
    assert len(status["subscriptions"]) == 2 and len(status["resources"]) == 1

    # client-a keeps polling; client-b went silent, so its lease runs out.
    def only_a_left() -> bool:
        poll = {"op": "poll", "client_id": "client-a", "cursors": {first["subscription_id"]: 0}}
        daemon_request(daemon_env.files, poll)
        return len(daemon_request(daemon_env.files, {"op": "status"})["subscriptions"]) == 1

    wait_for(only_a_left, 15, "client-b's lease to expire")
    # Now client-a goes silent too: the lease expires, then the idle timeout stops the daemon.
    wait_for(lambda: not daemon_running(daemon_env.files), 20, "idle shutdown")
    assert not daemon_env.files.socket.exists() and not daemon_env.files.endpoint.exists()
    assert "no subscriptions for 2 s, exiting" in daemon_env.files.log.read_text()


def test_params_reach_the_source_over_the_socket_and_never_show_in_the_status(daemon_env: DaemonEnv) -> None:
    ensure_daemon(APP)
    sub = _subscribe(daemon_env.files, "client-a", token="secret-token-1", user="someone")
    assert sub["ok"]
    refused = _subscribe(daemon_env.files, "client-a", token="bad")
    assert refused == {"ok": False, "error": "the token was refused", "reason": "auth"}
    wait_for(lambda: bool(daemon_env.calls_made()), 15, "the first poll")
    assert daemon_env.calls_made()[0]["account"] == "someone"
    status = daemon_request(daemon_env.files, {"op": "status"})
    assert "someone" in status["accounts"] and "secret-token-1" not in json.dumps(status)
    assert "secret-token-1" not in daemon_env.files.log.read_text()
    echoed = daemon_request(
        daemon_env.files, {"op": "echo", "target": Target("item", "a").to_wire(), "params": {"user": "someone"}}
    )
    assert echoed["ok"] and echoed["account"] == "someone" and echoed["snapshot"]["status"] == "open"
    assert daemon_request(daemon_env.files, {"op": "nope"}) == {"ok": False, "error": "unknown op 'nope'"}


def test_watch_command_streams_jsonl_and_header_goes_to_stderr(daemon_env: DaemonEnv) -> None:
    daemon_env.world(World(items={"a": {"status": "done"}}))
    base = daemon_env.root / "out"
    watch = _watch(["item", "a", "--events-stream=stdout", "--base-dir", str(base)], daemon_env.env())
    out, err = watch.communicate(timeout=30)
    assert watch.returncode == 0, err
    lines = out.splitlines()
    assert [json.loads(line)["kind"] for line in lines] == ["watch_started", "watch_ended"]
    assert json.loads(lines[1])["data"] == {"reason": "done", "status": "done", "already_ended": True}
    header = [line for line in err.splitlines() if line.startswith("fakeapp watch: ")]
    assert "fakeapp watch: ended: done (already done when the watch started)" in header
    assert header[0] == "fakeapp watch: resource: item a"
    assert "fakeapp watch: ignored: (none)" in header
    assert any(line.startswith("fakeapp watch: restart with: fakeapp watch item a --events=") for line in header)
    events_file = base / "item" / "a" / "events.jsonl"
    assert events_file.read_text().splitlines() == lines
    assert f"fakeapp watch: changelog: {events_file}" in header
    assert json.loads((events_file.parent / "baseline.json").read_text())["resource"] == "item a"
    assert (events_file.parent / "README.txt").is_file()


def test_usage_errors_exit_with_2_before_any_daemon_starts(daemon_env: DaemonEnv) -> None:
    for args, message in [
        (["item", "a", "--events=box_changed"], "never produces 'box_changed'"),
        (["item", "a", "--until=box_changed"], "never produces 'box_changed'"),
        (["item", "a", "--timeout=soon"], "cannot read the duration"),
        (["item", "a", "--events=stale"], "only with --stale-after"),
    ]:
        watch = _watch([*args, "--events-stream=stdout"], daemon_env.env())
        out, err = watch.communicate(timeout=30)
        assert watch.returncode == 2, (args, err)
        assert message in err and out == ""
    assert not daemon_running(daemon_env.files)


def test_watcher_survives_a_daemon_restart_without_a_duplicate_start(daemon_env: DaemonEnv) -> None:
    watch = _watch(
        ["item", "a", "--events-stream=stdout", "--timeout=6s", "--base-dir", str(daemon_env.root / "c")],
        daemon_env.env(),
    )
    assert watch.stdout is not None
    assert json.loads(watch.stdout.readline())["kind"] == "watch_started"
    old_pid = daemon_request(daemon_env.files, {"op": "hello"})["pid"]
    stop_daemon(daemon_env.files)
    rest, err = watch.communicate(timeout=30)
    assert watch.returncode == 0, err
    kinds = [json.loads(line)["kind"] for line in rest.splitlines()]
    assert kinds == ["watch_ended"], (kinds, err)
    assert json.loads(rest.splitlines()[-1])["data"]["reason"] == "timeout"
    assert "daemon restarted, subscribing again" in err
    assert ensure_daemon(APP)["pid"] != old_pid


@pytest.mark.skipif(os.name == "nt", reason="uses SIGSTOP")
def test_a_change_while_the_daemon_was_down_is_reported_once_after_the_restart(daemon_env: DaemonEnv) -> None:
    watch = _watch(
        ["item", "a", "--events-stream=stdout", "--timeout=8s", "--base-dir", str(daemon_env.root / "c")],
        daemon_env.env(),
    )
    assert watch.stdout is not None
    assert json.loads(watch.stdout.readline())["kind"] == "watch_started"
    # Freeze the watcher, so it cannot start a new daemon before the world changed.
    os.kill(watch.pid, signal.SIGSTOP)
    os.kill(daemon_request(daemon_env.files, {"op": "hello"})["pid"], signal.SIGKILL)
    wait_for(lambda: not daemon_running(daemon_env.files), 10, "the daemon to die")
    daemon_env.world(World(items={"a": {"status": "review", "notes": 1}}))
    os.kill(watch.pid, signal.SIGCONT)
    rest, err = watch.communicate(timeout=30)
    assert watch.returncode == 0, err
    events = [json.loads(line) for line in rest.splitlines()]
    # The new daemon diffed against the watcher's baseline: one watch_started with the new snapshot, then the changes.
    assert [e["kind"] for e in events] == ["watch_started", "status_changed", "note_added", "watch_ended"], err
    assert [e["seq"] for e in events] == [2, 3, 4, 5]


def test_a_newer_watcher_drains_an_older_daemon_and_an_older_one_does_not(daemon_env: DaemonEnv) -> None:
    watch = _watch(
        ["item", "a", "--events-stream=stdout", "--timeout=8s", "--base-dir", str(daemon_env.root / "c")],
        daemon_env.env(),
    )
    assert watch.stdout is not None
    assert json.loads(watch.stdout.readline())["kind"] == "watch_started"
    hello = daemon_request(daemon_env.files, {"op": "hello"})

    older = WatchApp(APP.name, APP.source, APP.daemon_module, APP.app_protocol, "1.2.2", APP.status_prefix, "t")
    older.__dict__["code"] = CodeStamp("older", hello["code_mtime"] - 100)
    assert ensure_daemon(older)["pid"] == hello["pid"]

    newer = WatchApp(APP.name, APP.source, APP.daemon_module, APP.app_protocol, "1.2.4", APP.status_prefix, "t")
    newer.__dict__["code"] = CodeStamp("newer", hello["code_mtime"] + 100)
    replaced = ensure_daemon(newer)
    assert replaced["pid"] != hello["pid"]
    assert "draining for a watcher with newer code (1.2.4 fakeapp" in daemon_env.files.log.read_text()

    rest, err = watch.communicate(timeout=30)
    assert watch.returncode == 0, err
    # The old daemon's watchers resubscribe from their baselines: nothing is reported twice.
    assert [json.loads(line)["kind"] for line in rest.splitlines()] == ["watch_ended"], err
    assert "daemon restarted, subscribing again" in err


def test_needs_drain_compares_the_code_stamps() -> None:
    mine = {"fingerprint": "b", "code_mtime": 20.0}
    assert needs_drain({"fingerprint": "a", "code_mtime": 10.0}, mine)
    assert not needs_drain({"fingerprint": "a", "code_mtime": 30.0}, mine)
    assert not needs_drain({"fingerprint": "b", "code_mtime": 10.0}, mine)
    assert not needs_drain({"fingerprint": "a", "code_mtime": "?"}, mine)


def test_two_watchers_of_one_batch_key_share_one_call(daemon_env: DaemonEnv) -> None:
    env = daemon_env.env(FAKE_SOURCE_BATCHING="1")
    watches = [
        _watch(
            ["item", ident, "--events-stream=stdout", "--timeout=4s", "--base-dir", str(daemon_env.root / ident)], env
        )
        for ident in ("a", "b")
    ]
    for watch in watches:
        out, err = watch.communicate(timeout=30)
        assert watch.returncode == 0, err
    assert any(sorted(call["keys"]) == ["item a", "item b"] for call in daemon_env.calls_made())


def test_daemon_cli_status_and_stop(daemon_env: DaemonEnv) -> None:
    ensure_daemon(APP)
    _subscribe(daemon_env.files, "client-a")
    cli = [sys.executable, "-c", "from fakeapp.cli import main; main()", "daemon"]
    status = subprocess.run([*cli, "status", "--json"], capture_output=True, text=True, timeout=30)
    assert status.returncode == 0, status.stderr
    assert json.loads(status.stdout)["app_protocol"] == 1
    text = subprocess.run([*cli, "status"], capture_output=True, text=True, timeout=30)
    assert text.stdout.startswith("daemon: pid ") and "subscription " in text.stdout and "account -:" in text.stdout
    assert 'source: {"calls":' in text.stdout
    logs = subprocess.run([*cli, "logs", "-n", "5"], capture_output=True, text=True, timeout=30)
    assert logs.stdout.startswith(f"==> {daemon_env.files.log}")
    stop = subprocess.run([*cli, "stop"], capture_output=True, text=True, timeout=30)
    assert stop.stdout.strip() == "daemon: stopped"
    assert not daemon_running(daemon_env.files)
    again = subprocess.run([*cli, "status"], capture_output=True, text=True, timeout=30)
    assert again.stdout.strip() == "daemon: not running"


def _descendants(pid: int) -> list[int]:
    rows = subprocess.run(["ps", "-eo", "pid=,ppid="], capture_output=True, text=True, check=True).stdout.splitlines()
    pairs = [(int(a), int(b)) for a, b in (row.split() for row in rows if row.strip())]
    found, todo = [], [pid]
    while todo:
        parent = todo.pop()
        children = [child for child, ppid in pairs if ppid == parent]
        found += children
        todo += children
    return found


@pytest.mark.skipif(os.name == "nt", reason="POSIX process trees; Windows uses job objects")
def test_killing_a_watchers_whole_process_tree_leaves_the_shared_daemon_running(daemon_env: DaemonEnv) -> None:
    common = ["item", "a", "--events-stream=stdout"]
    first = _watch([*common, "--base-dir", str(daemon_env.root / "c1")], daemon_env.env())
    assert first.stdout is not None and json.loads(first.stdout.readline())["kind"] == "watch_started"
    daemon_pid = daemon_request(daemon_env.files, {"op": "hello"})["pid"]
    # The launcher exits right after the daemon is ready, but its interpreter takes a moment to shut down; until
    # then the daemon is still its child.
    wait_for(lambda: daemon_pid not in _descendants(first.pid), 10, "the launcher to let go of the daemon")

    second = _watch([*common, "--timeout=4s", "--base-dir", str(daemon_env.root / "c2")], daemon_env.env())
    assert second.stdout is not None and json.loads(second.stdout.readline())["kind"] == "watch_started"

    # What a tree kill does: SIGKILL the process and everything below it.
    for pid in [first.pid, *_descendants(first.pid)]:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
    first.wait(timeout=10)

    rest, err = second.communicate(timeout=30)
    assert second.returncode == 0, err
    assert [json.loads(line)["data"]["reason"] for line in rest.splitlines()] == ["timeout"]
    assert "daemon restarted" not in err
    assert daemon_request(daemon_env.files, {"op": "hello"})["pid"] == daemon_pid
    assert daemon_env.started_count() == 1


@pytest.mark.skipif(os.name == "nt", reason="uses SIGSTOP and SIGTERM")
def test_a_stopping_watcher_never_starts_a_daemon(daemon_env: DaemonEnv) -> None:
    watch = _watch(["item", "a", "--events-stream=stdout", "--base-dir", str(daemon_env.root / "c")], daemon_env.env())
    assert watch.stdout is not None and json.loads(watch.stdout.readline())["kind"] == "watch_started"
    # Freeze the watcher so it cannot poll in between, take the daemon away, then stop the watcher.
    os.kill(watch.pid, signal.SIGSTOP)
    daemon_pid = daemon_request(daemon_env.files, {"op": "hello"})["pid"]
    os.kill(daemon_pid, signal.SIGKILL)
    wait_for(lambda: not daemon_running(daemon_env.files), 10, "the daemon to die")
    os.kill(watch.pid, signal.SIGTERM)
    os.kill(watch.pid, signal.SIGCONT)
    rest, err = watch.communicate(timeout=30)
    assert watch.returncode == 0, err
    assert json.loads(rest.splitlines()[-1])["data"]["reason"] == "signal SIGTERM"
    time.sleep(1.0)
    assert not daemon_running(daemon_env.files), "the stopping watcher started a new daemon"
    assert daemon_env.started_count() == 1


def test_no_daemon_polls_in_process(daemon_env: DaemonEnv) -> None:
    daemon_env.world(World(items={"a": {"status": "done"}}))
    watch = _watch(["item", "a", "--events-stream=stdout", "--no-daemon"], daemon_env.env())
    out, err = watch.communicate(timeout=30)
    assert watch.returncode == 0, err
    assert [json.loads(line)["kind"] for line in out.splitlines()] == ["watch_started", "watch_ended"]
    assert "fakeapp watch: daemon: in-process (--no-daemon), private budget" in err
    assert not daemon_running(daemon_env.files)


def test_a_silent_connection_holds_up_neither_a_stop_nor_a_drain(daemon_env: DaemonEnv) -> None:
    first = ensure_daemon(APP)
    silent, _ = _connect(daemon_env.files, 5)
    started = time.monotonic()
    assert stop_daemon(daemon_env.files)
    assert time.monotonic() - started < 5
    silent.close()

    second = ensure_daemon(APP)
    assert second["pid"] != first["pid"]
    silent, _ = _connect(daemon_env.files, 5)
    assert daemon_request(daemon_env.files, {"op": "drain"})["ok"]
    started = time.monotonic()
    third = ensure_daemon(APP)
    assert third["pid"] != second["pid"] and time.monotonic() - started < 6
    silent.close()


def test_a_daemon_that_fails_to_start_says_why_and_is_not_started_again(
    daemon_env: DaemonEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SOURCE_FAIL", "1")
    started = time.monotonic()
    with pytest.raises(
        DaemonStartFailed, match="the daemon failed to start: RuntimeError: the fake source cannot start"
    ):
        ensure_daemon(APP)
    assert time.monotonic() - started < 8
    log = daemon_env.files.log.read_text()
    # One attempt, not a respawn loop until the start timeout.
    assert "Traceback" in log and log.count("RuntimeError: the fake source cannot start") == 1
