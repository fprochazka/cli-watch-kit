"""The watcher: its own event numbering across daemon restarts, the ends, the threshold timers, the in-process
backend."""

from __future__ import annotations

import io
import json
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from cli_watch_kit import cli
from cli_watch_kit.app import WatchApp
from cli_watch_kit.changelog import Changelog
from cli_watch_kit.cli import WatchOptions
from cli_watch_kit.client import (
    RETRYABLE_REASONS,
    BackendError,
    InProcessBackend,
    Watcher,
    WatchSettings,
    WriterContext,
)
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
    monkeypatch.setattr(cli, "DIR_LOCK_WAIT", 0.2)
    app = WatchApp("inproc", FakeSource(), "unused.daemon", 1, "0", "inproc: ", "inproc-watcher")
    options = WatchOptions(timeout="1s", events_stream="stdout", base_dir=tmp_path)
    (tmp_path / "item" / "a").mkdir(parents=True)
    with FileLock(tmp_path / "item" / "a" / cli.DIR_LOCK) as first:
        assert first.try_acquire()
        assert cli.run_watch(app, TARGET, options) == 1
        assert "another watcher already watches item a into" in capsys.readouterr().err
    assert not (tmp_path / "item" / "a" / "events.jsonl").exists()
    assert cli.run_watch(app, TARGET, options) == 0


def test_a_watcher_waits_for_a_stopped_one_that_is_still_finishing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cli, "default_backend", lambda app, no_daemon: ScriptedBackend([]))
    app = WatchApp("inproc", FakeSource(), "unused.daemon", 1, "0", "inproc: ", "inproc-watcher")
    options = WatchOptions(timeout="1s", events_stream="stdout", base_dir=tmp_path)
    (tmp_path / "item" / "a").mkdir(parents=True)
    old = FileLock(tmp_path / "item" / "a" / cli.DIR_LOCK)
    assert old.try_acquire()
    finishing = threading.Timer(1.0, old.release)
    finishing.start()
    try:
        assert cli.run_watch(app, TARGET, options) == 0
    finally:
        finishing.cancel()
        old.release()
    assert (tmp_path / "item" / "a" / "events.jsonl").is_file()


def test_a_stdout_watcher_under_a_session_writes_into_its_scratchpad(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cli, "default_backend", lambda app, no_daemon: ScriptedBackend([]))
    monkeypatch.setattr(cli, "find_session", lambda: SimpleNamespace(scratchpad_dir=tmp_path / "scratchpad"))
    app = WatchApp("inproc", FakeSource(), "unused.daemon", 1, "0", "inproc: ", "inproc-watcher")
    monkeypatch.delenv(app.paths.base_dir_env, raising=False)
    assert cli.run_watch(app, TARGET, WatchOptions(timeout="1s", events_stream="stdout")) == 0
    assert (tmp_path / "scratchpad" / "inproc" / "item" / "a" / "events.jsonl").is_file()


class StuckSink(StdoutSink):
    """A sink whose reader stopped reading: every write blocks."""

    name = "stuck"

    def emit(self, events: Any) -> None:
        threading.Event().wait(30)


@pytest.mark.skipif(not hasattr(signal, "SIGTERM") or os.name == "nt", reason="sends itself SIGTERM")
def test_a_stop_signal_ends_a_watch_whose_writes_block(tmp_path: Path) -> None:
    before = signal.getsignal(signal.SIGTERM)
    changelog = Changelog(tmp_path, TARGET.key)
    watcher = Watcher(
        WatchSettings(TARGET, ALL),
        ScriptedBackend([[daemon_event("watch_started", 0, summary={})]]),
        StuckSink(),
        changelog,
        StatusOutput("test: ", io.StringIO()),
        clock=Ticks(),
    )
    threading.Timer(0.3, os.kill, (os.getpid(), signal.SIGTERM)).start()
    started = time.monotonic()
    assert watcher.run() == 0
    assert time.monotonic() - started < 5
    ended = json.loads(changelog.events_path.read_text().splitlines()[-1])
    assert ended["kind"] == "watch_ended" and ended["data"]["reason"] == "signal SIGTERM"
    assert signal.getsignal(signal.SIGTERM) == before


def test_both_backends_take_the_engine_settings_from_the_app_and_its_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = FakeSource(World(items={"a": {"status": "open"}}))
    app = WatchApp("inproc", lambda: FakeSource(), "unused.daemon", 1, "0", "inproc: ", "inproc-watcher")
    overridden = app.with_overrides(source=source, client_poll_ms=250, lease_timeout=3)
    assert overridden.make_source() is source and app.make_source() is not source
    assert overridden.engine_settings() == {"client_poll_ms": 250, "lease_timeout": 3}
    monkeypatch.setenv("INPROC_CLIENT_POLL_MS", "100")
    backend = InProcessBackend(overridden)
    try:
        assert backend.engine.client_poll_ms == 100 and backend.engine.lease_timeout == 3
        assert backend.engine.source is source
        sid, cursor = backend.subscribe("c", TARGET, frozenset({"status_changed"}), None, {}, {})
        assert backend.poll("c", {sid: cursor}, {})["next_poll_ms"] == 100
    finally:
        backend.close()


def test_a_resubscribe_that_meets_a_bug_in_the_daemon_is_retried(tmp_path: Path) -> None:
    engine_backend = InProcessBackend(
        WatchApp("inproc", FakeSource(World(items={"a": {"status": "open"}})), "unused", 1, "0", "inproc: ", "x")
    )
    try:
        with pytest.raises(BackendError) as raised:
            engine_backend.subscribe("c", TARGET, frozenset({"status_changed"}), None, {"user": "boom"}, {})
        assert raised.value.reason in RETRYABLE_REASONS
    finally:
        engine_backend.close()


class SlowSink(StdoutSink):
    """A healthy sink that takes a moment per write, like a message to a Claude Code session."""

    name = "slow"

    def __init__(self) -> None:
        super().__init__(io.StringIO())
        self.kinds: list[str] = []

    def emit(self, events: Any) -> None:
        time.sleep(0.4)
        self.kinds.extend(e.kind for e in events)


@pytest.mark.skipif(os.name == "nt", reason="sends itself SIGTERM")
def test_a_stop_signal_lets_a_healthy_write_finish(tmp_path: Path) -> None:
    sink = SlowSink()
    watcher = Watcher(
        WatchSettings(TARGET, ALL),
        ScriptedBackend([[daemon_event("watch_started", 0, summary={})]]),
        sink,
        Changelog(tmp_path, TARGET.key),
        StatusOutput("test: ", io.StringIO()),
        clock=Ticks(),
    )
    threading.Timer(0.2, os.kill, (os.getpid(), signal.SIGTERM)).start()
    assert watcher.run() == 0
    assert sink.kinds == ["watch_started", "watch_ended"]


@pytest.mark.skipif(os.name == "nt", reason="raises SIGTERM in a child process")
def test_a_stop_signal_that_arrives_while_the_first_one_is_handled_still_ends_the_watch(tmp_path: Path) -> None:
    script = Path(__file__).with_name("signal_burst_watcher.py")
    # The child ends itself after 20 s when its handler deadlocks, so the timeout here is never the one that fires.
    result = subprocess.run(
        [sys.executable, str(script), str(tmp_path), "20"], capture_output=True, text=True, timeout=60, check=False
    )
    assert result.returncode == 0, result.stderr
    assert int(result.stderr.split()[-1]) >= 5, "the burst never reached the handler"
    (ended,) = [json.loads(line) for line in result.stdout.splitlines()]
    assert ended["kind"] == "watch_ended" and ended["data"]["reason"] == "signal SIGTERM"


def test_handlers_not_set_from_python_are_put_back_as_the_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    saved = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    original = signal.signal
    restored: list[Any] = []

    def fake_signal(sig: int, handler: Any) -> Any:
        previous = original(sig, handler)
        if handler is signal.SIG_DFL or not callable(handler):
            restored.append(handler)
            return previous
        return None  # as for a handler installed by C code

    monkeypatch.setattr(signal, "signal", fake_signal)
    try:
        watcher = Watcher(
            WatchSettings(TARGET, ALL, timeout=3),
            ScriptedBackend([[daemon_event("watch_started", 0, summary={})]]),
            StdoutSink(io.StringIO()),
            Changelog(tmp_path, TARGET.key),
            StatusOutput("test: ", io.StringIO()),
            clock=Ticks(),
        )
        assert watcher.run() == 0
        assert restored == [signal.SIG_DFL, signal.SIG_DFL]
    finally:
        for sig, handler in saved.items():
            original(sig, handler)


def multi_app(world: World) -> WatchApp:
    app = WatchApp(
        "inproc", FakeSource(), "unused.daemon", 1, "0", "inproc: ", "inproc-watcher", command="inproc watch"
    )
    return app.with_overrides(source=FakeSource(world, default_interval=0.5), client_poll_ms=20)


def once(ready: Callable[[], bool], then: Callable[[], None]) -> None:
    """Call `then` from a thread as soon as `ready()` holds, while the watch runs in the main thread."""

    def wait() -> None:
        for _ in range(1000):
            if ready():
                then()
                return
            time.sleep(0.01)

    threading.Thread(target=wait, daemon=True).start()


def logged(path: Path) -> str:
    try:
        return path.read_text()
    except OSError:
        return ""


def test_one_watch_of_several_targets_streams_them_all_and_ends_when_each_ended(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    world = World(items={"a": {"status": "done"}, "b": {"status": "done"}})
    options = WatchOptions(timeout="20s", events_stream="stdout", base_dir=tmp_path, no_daemon=True)
    targets = [Target("item", "a"), Target("item", "b"), Target("item", "a")]
    assert cli.run_watch(multi_app(world), targets, options) == 0
    captured = capsys.readouterr()
    events = [json.loads(line) for line in captured.out.splitlines()]
    assert sorted((e["resource"], e["seq"], e["kind"]) for e in events) == [
        ("item a", 1, "watch_started"),
        ("item a", 2, "watch_ended"),
        ("item b", 1, "watch_started"),
        ("item b", 2, "watch_ended"),
    ]
    for ident in "ab":
        changelog = [json.loads(line) for line in (tmp_path / "item" / ident / "events.jsonl").read_text().splitlines()]
        assert [e["resource"] for e in changelog] == [f"item {ident}"] * 2
    header = [line for line in captured.err.splitlines() if line.startswith("inproc: ")]
    assert header[:6] == [
        "inproc: resource: item a",
        f"inproc: dir: {tmp_path / 'item' / 'a'}",
        f"inproc: changelog: {tmp_path / 'item' / 'a' / 'events.jsonl'}",
        "inproc: resource: item b",
        f"inproc: dir: {tmp_path / 'item' / 'b'}",
        f"inproc: changelog: {tmp_path / 'item' / 'b' / 'events.jsonl'}",
    ]
    assert "inproc: ends: when every resource ended or after 20s" in header
    assert any(line.startswith("inproc: restart with: inproc watch item a b ") for line in header)
    assert "inproc: ended: item a: done (already done when the watch started)" in header


def test_until_of_one_target_ends_them_all_and_a_failed_target_sets_the_exit_code(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    world = World(items={"a": {"status": "open"}})
    options = WatchOptions(
        until="status_changed", timeout="20s", events_stream="stdout", base_dir=tmp_path, no_daemon=True
    )

    def started_and_b_gone() -> bool:
        a, b = (logged(tmp_path / "item" / ident / "events.jsonl") for ident in "ab")
        return "watch_started" in a and "watch_ended" in b

    once(started_and_b_gone, lambda: world.items.update(a={"status": "review"}))
    assert cli.run_watch(multi_app(world), [Target("item", "a"), Target("item", "b")], options) == 1
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    def of(resource: str) -> list[tuple[str, Any]]:
        return [(e["kind"], e["data"].get("reason")) for e in events if e["resource"] == resource]

    # b does not exist: it ends with an error, and a goes on alone until its first status change.
    assert of("item b") == [("watch_ended", "not_found")]
    assert of("item a") == [("watch_started", None), ("status_changed", None), ("watch_ended", "until")]
    assert events[-1]["data"] == {"reason": "until", "event": "status_changed", "seq": 2, "resource": "item a"}


def test_every_target_of_a_watch_resubscribes_after_its_lease_expired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    world = World(items={"a": {"status": "open"}, "b": {"status": "open"}})
    app = multi_app(world)
    backends: list[InProcessBackend] = []

    def backend_of(app: WatchApp, no_daemon: bool) -> InProcessBackend:
        backends.append(InProcessBackend(app))
        return backends[-1]

    monkeypatch.setattr(cli, "default_backend", backend_of)
    options = WatchOptions(timeout="20s", until="status_changed", events_stream="stdout", base_dir=tmp_path)

    def both_started() -> bool:
        return all((tmp_path / "item" / ident / "baseline.json").is_file() for ident in "ab")

    def expire_and_change() -> None:
        engine = backends[0].engine
        done = threading.Event()

        def expire() -> None:
            engine.reap(time.monotonic() + 3600)
            world.items["b"] = {"status": "review"}
            done.set()

        backends[0]._loop.call_soon_threadsafe(expire)
        done.wait(5)

    once(both_started, expire_and_change)
    assert cli.run_watch(app, [Target("item", "a"), Target("item", "b")], options) == 0
    captured = capsys.readouterr()
    events = [(e["resource"], e["kind"]) for e in map(json.loads, captured.out.splitlines())]
    assert "daemon restarted, subscribing again" in captured.err
    # a's new watch_started repeats a known snapshot and is dropped; b's names a new one, and the change in the gap
    # is reported as well.
    assert [kind for resource, kind in events if resource == "item a"] == ["watch_started", "watch_ended"]
    b = [kind for resource, kind in events if resource == "item b"]
    assert b[0] == "watch_started" and b[-2:] == ["status_changed", "watch_ended"]
    assert len(backends[0].engine.subscriptions) == 0
