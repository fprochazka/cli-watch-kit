"""The watcher: its own event numbering across daemon restarts, the ends, the threshold timers, the in-process
backend."""

from __future__ import annotations

import io
import json
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from cli_watch_kit import cli
from cli_watch_kit.app import WatchApp
from cli_watch_kit.changelog import Changelog
from cli_watch_kit.cli import WatchOptions
from cli_watch_kit.client import BackendError, InProcessBackend, Watcher, WatchSettings, WriterContext
from cli_watch_kit.events import Event
from cli_watch_kit.locks import FileLock, LockTimeout
from cli_watch_kit.sinks import StatusOutput, StdoutSink
from cli_watch_kit.source import Params, Target
from fakeapp.source import FakeSource, World
from fakeapp.timers import StaleTimer

TARGET = Target("item", "a")
ALL = frozenset({"watch_started", "watch_degraded", "watch_ended", "status_changed", "note_added"})


def daemon_event(kind: str, seq: int, **data: Any) -> dict[str, Any]:
    return Event(kind, TARGET.key, seq, "2026-11-09T10:00:00.000Z", data).to_json()


class ScriptedBackend:
    """Answers polls from a script. `"restart"` makes the current subscription unknown, like a new daemon. A dict
    step with `snapshot` sends that snapshot too."""

    def __init__(self, script: list[Any]) -> None:
        self.script: Iterator[Any] = iter(script)
        self.subscriptions: list[tuple[Any, Params]] = []
        self.cursors: list[int] = []

    def describe(self) -> str:
        return "scripted"

    def subscribe(
        self,
        client_id: str,
        target: Target,
        events: frozenset[str],
        baseline: Any,
        params: Params,
        source_options: Mapping[str, Any],
    ) -> tuple[str, int]:
        self.subscriptions.append((baseline, params))
        return f"sub-{len(self.subscriptions)}", 0

    def poll(self, client_id: str, cursors: dict[str, int], acks: dict[str, int]) -> dict[str, Any]:
        ((sid, cursor),) = cursors.items()
        self.cursors.append(cursor)
        step: Any = next(self.script, [])
        if step == "restart":
            return {"unknown": [sid], "next_poll_ms": 0}
        if isinstance(step, dict):
            return {
                "events": {sid: step.get("events", [])},
                "snapshots": {sid: step["snapshot"]},
                "next_poll_ms": 0,
            }
        return {"events": {sid: step} if step else {}, "next_poll_ms": 0}

    def unsubscribe(self, client_id: str, subscription_id: str) -> None:
        pass

    def call(self, op: str, payload: Mapping[str, Any], params: Params | None = None) -> dict[str, Any]:
        raise AssertionError("no op expected")

    def begin_shutdown(self) -> None:
        pass

    def close(self) -> None:
        pass


class Ticks:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        self.now += 1.0
        return self.now


def run(backend: Any, changelog: Changelog, out: io.StringIO, settings: WatchSettings | None = None, **kw: Any) -> int:
    watcher = Watcher(
        settings or WatchSettings(TARGET, ALL, timeout=40),
        backend,
        StdoutSink(out),
        changelog,
        StatusOutput("test: ", io.StringIO()),
        clock=Ticks(),
        **kw,
    )
    return watcher.run()


def lines_of(out: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in out.getvalue().splitlines()]


def test_seq_is_strictly_increasing_across_a_daemon_restart(tmp_path: Path) -> None:
    backend = ScriptedBackend(
        [
            [daemon_event("watch_started", 0, summary={}, snapshot_hash="h1")],
            [daemon_event("note_added", 1), daemon_event("status_changed", 2)],
            "restart",
            # The new daemon numbers from scratch; its watch_started changed snapshot, so it is reported.
            [daemon_event("watch_started", 0, summary={}, snapshot_hash="h2"), daemon_event("note_added", 1)],
        ]
    )
    changelog = Changelog(tmp_path, TARGET.key)
    out = io.StringIO()
    assert run(backend, changelog, out) == 0
    events = lines_of(out)
    kinds = [e["kind"] for e in events]
    assert kinds == ["watch_started", "note_added", "status_changed", "watch_started", "note_added", "watch_ended"]
    assert [e["seq"] for e in events] == [1, 2, 3, 4, 5, 6]
    assert changelog.events_path.read_text().splitlines() == out.getvalue().splitlines()
    assert len(backend.subscriptions) == 2
    # The cursor still follows the daemon's own numbering, and resets with the new daemon.
    assert backend.cursors[:4] == [0, 0, 2, 0]


def test_a_restart_resubscribes_with_the_baseline_and_drops_an_unchanged_watch_started(tmp_path: Path) -> None:
    snapshot = {"hash": "h1", "data": {"status": "open", "notes": 0}}
    backend = ScriptedBackend(
        [
            {"events": [daemon_event("watch_started", 0, summary={}, snapshot_hash="h1")], "snapshot": snapshot},
            "restart",
            [daemon_event("watch_started", 0, summary={}, snapshot_hash="h1")],
        ]
    )
    changelog = Changelog(tmp_path, TARGET.key)
    out = io.StringIO()
    settings = WatchSettings(TARGET, ALL, timeout=10, params={"token": "t"})
    assert run(backend, changelog, out, settings) == 0
    assert [e["kind"] for e in lines_of(out)] == ["watch_started", "watch_ended"]
    assert backend.subscriptions == [(None, {"token": "t"}), ({"status": "open", "notes": 0}, {"token": "t"})]
    assert changelog.load_baseline() is not None


def test_a_new_watcher_continues_the_changelog_numbering(tmp_path: Path) -> None:
    changelog = Changelog(tmp_path, TARGET.key, readme="hello\n")
    settings = WatchSettings(TARGET, ALL, timeout=3)
    run(ScriptedBackend([[daemon_event("watch_started", 0, summary={})]]), changelog, io.StringIO(), settings)
    second = io.StringIO()
    run(ScriptedBackend([[daemon_event("watch_started", 0, summary={})]]), changelog, second, settings)
    assert [e["seq"] for e in lines_of(second)] == [3, 4]
    assert changelog.last_seq() == 4
    assert (tmp_path / "README.txt").read_text() == "hello\n"


def test_until_ends_after_the_first_event_of_its_kind(tmp_path: Path) -> None:
    backend = ScriptedBackend(
        [[daemon_event("watch_started", 0, summary={})], [daemon_event("status_changed", 1, to="done")]]
    )
    out = io.StringIO()
    settings = WatchSettings(TARGET, ALL, until="status_changed")
    assert run(backend, Changelog(tmp_path, TARGET.key), out, settings) == 0
    assert lines_of(out)[-1]["data"] == {"reason": "until", "event": "status_changed", "seq": 2}


def test_not_found_exits_with_an_error(tmp_path: Path) -> None:
    backend = ScriptedBackend([[daemon_event("watch_ended", 1, reason="not_found", message="gone")]])
    out = io.StringIO()
    assert run(backend, Changelog(tmp_path, TARGET.key), out, WatchSettings(TARGET, ALL)) == 1


def test_idle_after_ends_a_watch_without_events_and_thresholds_do_not_count(tmp_path: Path) -> None:
    snapshot = {"hash": "h1", "data": {"status": "open", "notes": 0}}
    backend = ScriptedBackend(
        [{"events": [daemon_event("watch_started", 0, summary={}, snapshot_hash="h1")], "snapshot": snapshot}]
    )
    out = io.StringIO()
    settings = WatchSettings(TARGET, ALL | {"stale"}, idle_after=10, threshold_kinds=frozenset({"stale"}))
    assert settings.subscribed_events == ALL
    code = run(backend, Changelog(tmp_path, TARGET.key), out, settings, timers=[StaleTimer(3, 0)])
    assert code == 0
    events = lines_of(out)
    assert [e["kind"] for e in events] == ["watch_started", "stale", "watch_ended"]
    assert events[1]["data"] == {"status": "open", "after": "3s"}
    assert events[-1]["data"] == {"reason": "idle", "idle_after": "10s"}


def test_the_in_process_backend_runs_the_engine_and_the_ops(tmp_path: Path) -> None:
    world = World(items={"a": {"status": "open"}})
    source = FakeSource(world)
    app = WatchApp("inproc", source, "unused.daemon", 1, "0", "inproc: ", "inproc-watcher")
    backend = InProcessBackend(app)
    try:
        sid, cursor = backend.subscribe("c", TARGET, frozenset({"status_changed"}), None, {"user": "u"}, {})
        response: dict[str, Any] = {}
        for _ in range(200):
            response = backend.poll("c", {sid: cursor}, {})
            if response["events"]:
                break
            time.sleep(0.01)
        assert [e["kind"] for e in response["events"][sid]] == ["watch_started"]
        echoed = backend.call("echo", {"target": TARGET.to_wire()}, {"user": "u"})
        assert echoed["account"] == "u" and echoed["snapshot"] == {"status": "open", "notes": 0}
        assert backend.call("stop", {})["ok"]
        assert backend.poll("c", {sid: cursor}, {})["ok"]
    finally:
        backend.close()


class ArtifactWriter:
    """Attaches a file to watch_started, like an app that writes the state of the resource next to its changelog."""

    def __init__(self, context: WriterContext) -> None:
        self.context = context

    @classmethod
    def make(cls, contexts: list[WriterContext]) -> Callable[[WriterContext], ArtifactWriter]:
        def factory(context: WriterContext) -> ArtifactWriter:
            contexts.append(context)
            return cls(context)

        return factory

    def snapshot(self, data: Any) -> None:
        pass

    def attach(self, events: list[Event]) -> list[Event]:
        path = str(self.context.resource_dir / "started.txt")
        return [replace(e, artifacts=(path,)) if e.kind == "watch_started" else e for e in events]


def test_run_watch_subscribes_to_the_extra_kinds_without_reporting_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    backend = ScriptedBackend([[daemon_event("watch_started", 0, summary={}), daemon_event("note_added", 1)]])
    subscribed: list[frozenset[str]] = []
    original = backend.subscribe

    def recording(client_id: str, target: Target, events: frozenset[str], *rest: Any) -> tuple[str, int]:
        subscribed.append(events)
        return original(client_id, target, events, *rest)

    monkeypatch.setattr(backend, "subscribe", recording)
    monkeypatch.setattr(cli, "default_backend", lambda app, no_daemon: backend)
    app = WatchApp("inproc", FakeSource(), "unused.daemon", 1, "0", "inproc: ", "inproc-watcher")
    options = WatchOptions(events=("status",), timeout="1s", events_stream="stdout", base_dir=tmp_path)
    contexts: list[WriterContext] = []
    assert cli.run_watch(app, TARGET, options, also_subscribe=["note_added"], writer=ArtifactWriter.make(contexts)) == 0
    assert [(c.base_dir, c.resource_dir, c.target, c.backend) for c in contexts] == [
        (tmp_path, tmp_path / "item" / "a", TARGET, backend)
    ]
    assert subscribed == [frozenset({"status_changed", "note_added", "watch_started", "watch_degraded", "watch_ended"})]
    reported = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [e["kind"] for e in reported] == ["watch_started", "watch_ended"]
    assert reported[0]["artifacts"] == [str(tmp_path / "item" / "a" / "started.txt")]
    assert cli.run_watch(app, TARGET, options, also_subscribe=["box_changed"]) == 2
    assert "cannot also subscribe" in capsys.readouterr().err


class LockedOutBackend(ScriptedBackend):
    """A daemon that cannot be reached because another watcher holds the spawn lock for too long."""

    def poll(self, client_id: str, cursors: dict[str, int], acks: dict[str, int]) -> dict[str, Any]:
        raise LockTimeout("could not lock spawn.lock within 10 s")


def test_a_spawn_lock_timeout_ends_the_watch_with_an_error_line(tmp_path: Path) -> None:
    status = io.StringIO()
    watcher = Watcher(
        WatchSettings(TARGET, ALL),
        LockedOutBackend([]),
        StdoutSink(io.StringIO()),
        Changelog(tmp_path, TARGET.key),
        StatusOutput("test: ", status),
        clock=Ticks(),
    )
    assert watcher.run() == 1
    assert "test: error: could not lock spawn.lock within 10 s" in status.getvalue()


class FlakySubscribeBackend(ScriptedBackend):
    """Its second subscribe fails once, as when the API is unreachable after a suspend."""

    failed = False

    def subscribe(self, client_id: str, target: Target, events: frozenset[str], *rest: Any) -> tuple[str, int]:
        if self.subscriptions and not self.failed:
            self.failed = True
            raise BackendError("the API is unreachable", "error")
        return super().subscribe(client_id, target, events, *rest)


def test_a_failed_resubscribe_is_retried_with_a_backoff(tmp_path: Path) -> None:
    backend = FlakySubscribeBackend(
        [[daemon_event("watch_started", 0, summary={}, snapshot_hash="h1")], "restart", [daemon_event("note_added", 1)]]
    )
    status = io.StringIO()
    out = io.StringIO()
    watcher = Watcher(
        WatchSettings(TARGET, ALL, timeout=60),
        backend,
        StdoutSink(out),
        Changelog(tmp_path, TARGET.key),
        StatusOutput("test: ", status),
        clock=Ticks(),
        retry_delays=(0.01, 0.02),
    )
    assert watcher.run() == 0
    assert status.getvalue().count("subscribing again failed: the API is unreachable") == 1
    assert [e["kind"] for e in lines_of(out)] == ["watch_started", "note_added", "watch_ended"]


def test_a_refused_resubscribe_that_cannot_pass_ends_the_watch(tmp_path: Path) -> None:
    class Refusing(ScriptedBackend):
        def subscribe(self, client_id: str, target: Target, events: frozenset[str], *rest: Any) -> tuple[str, int]:
            if self.subscriptions:
                raise BackendError("the token was refused", "auth")
            return super().subscribe(client_id, target, events, *rest)

    status = io.StringIO()
    watcher = Watcher(
        WatchSettings(TARGET, ALL, timeout=60),
        Refusing([[daemon_event("watch_started", 0, summary={})], "restart"]),
        StdoutSink(io.StringIO()),
        Changelog(tmp_path, TARGET.key),
        StatusOutput("test: ", status),
        clock=Ticks(),
    )
    assert watcher.run() == 1
    assert "test: error: the token was refused" in status.getvalue()


def test_a_second_watcher_of_the_same_resource_dir_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "default_backend", lambda app, no_daemon: ScriptedBackend([]))
    app = WatchApp("inproc", FakeSource(), "unused.daemon", 1, "0", "inproc: ", "inproc-watcher")
    options = WatchOptions(timeout="1s", events_stream="stdout", base_dir=tmp_path)
    (tmp_path / "item" / "a").mkdir(parents=True)
    with FileLock(tmp_path / "item" / "a" / cli.DIR_LOCK) as first:
        assert first.try_acquire()
        assert cli.run_watch(app, TARGET, options) == 1
        assert "another watcher already watches item a into" in capsys.readouterr().err
    assert not (tmp_path / "item" / "a" / "events.jsonl").exists()
    assert cli.run_watch(app, TARGET, options) == 0
