"""The watcher: subscribe through a backend, poll it for events, write the changelog, deliver to a sink.

Two backends speak the same requests. :class:`DaemonBackend` talks to the shared per-user daemon and starts a new one
when it disappears. :class:`InProcessBackend` (`--no-daemon`) runs the same engine and request handler in a thread of
this process, with a private budget.

The watcher numbers the events it reports itself (`seq`), strictly increasing across daemon restarts and across
watcher runs on the same changelog. After a daemon restart it subscribes again with its baseline, so the new daemon
reports only what changed, and it drops the new `watch_started` when the snapshot did not move. One watcher can
watch several targets: each keeps its own numbering, changelog and baseline, and they share one stream.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import os
import signal
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import FrameType
from typing import Any, Protocol

from cli_watch_kit.app import WatchApp
from cli_watch_kit.changelog import Changelog
from cli_watch_kit.daemon.engine import Engine
from cli_watch_kit.daemon.lifecycle import DEFAULT_REQUEST_TIMEOUT, HUNG_AFTER, ensure_daemon, kill_hung_daemon
from cli_watch_kit.daemon.server import handle
from cli_watch_kit.daemon.transport import (
    DaemonFiles,
    DaemonHung,
    DaemonMismatch,
    DaemonNotTrusted,
    DaemonStartFailed,
    DaemonTimeout,
    DaemonUnavailable,
    request,
)
from cli_watch_kit.durations import format_duration
from cli_watch_kit.events import WATCH_ENDED, WATCH_STARTED, Event, utc_now_iso
from cli_watch_kit.sinks import Sink, StatusOutput
from cli_watch_kit.source import NotFound, Params, Target
from cli_watch_kit.timers import ClientTimer, IdleTimer

OP_TIMEOUT = 660.0
"""An extra op may wait up to `Engine.await_budget`'s 600 s for the account's budget, then download something large."""
RETRYABLE_REASONS = frozenset({"error", "rate_limited"})
"""Reasons of a refused subscription that may pass by themselves: the source could not reach its API."""
EXIT_OK = 0
EXIT_ERROR = 1


STUCK_WRITE = 1.0
"""Seconds a write to the sink may take before a stop signal breaks it as stuck."""


class _Interrupted(Exception):
    """Raised by the signal handler out of a write that blocks, so a stop is not stuck behind it."""


class BackendError(RuntimeError):
    """The daemon refused a request. `reason` is the source's error reason, when the source raised it."""

    def __init__(self, message: str, reason: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason


class Backend(Protocol):
    def describe(self) -> str: ...

    def subscribe(
        self,
        client_id: str,
        target: Target,
        events: frozenset[str],
        baseline: Any,
        params: Params,
        source_options: Mapping[str, Any],
    ) -> tuple[str, int]: ...

    def poll(self, client_id: str, cursors: dict[str, int], acks: dict[str, int]) -> dict[str, Any]:
        """New events after the cursors. `acks` passes back the `acks` of the last answer that arrived."""
        ...

    def unsubscribe(self, client_id: str, subscription_id: str) -> None: ...

    def call(self, op: str, payload: Mapping[str, Any], params: Params | None = None) -> dict[str, Any]:
        """An extra op of the source, run by the daemon within its budget."""
        ...

    def begin_shutdown(self) -> None:
        """The watcher is stopping: from now on, never start a daemon."""
        ...

    def close(self) -> None: ...


def _check(response: dict[str, Any]) -> dict[str, Any]:
    if not response.get("ok"):
        raise BackendError(str(response.get("error", "the daemon refused the request")), response.get("reason"))
    return response


def _subscribe_payload(
    client_id: str,
    target: Target,
    events: frozenset[str],
    baseline: Any,
    params: Params,
    source_options: Mapping[str, Any],
) -> dict[str, Any]:
    options: dict[str, Any] = {"events": sorted(events)}
    if baseline is not None:
        options["baseline"] = baseline
    payload: dict[str, Any] = {
        "op": "subscribe",
        "client_id": client_id,
        "target": target.to_wire(),
        "options": options,
    }
    if params:
        payload["params"] = dict(params)
    if source_options:
        payload["source_options"] = dict(source_options)
    return payload


class DaemonBackend:
    def __init__(self, app: WatchApp, files: DaemonFiles | None = None) -> None:
        self.app = app
        self.files = files or DaemonFiles.for_app(app)
        self.hello = ensure_daemon(app, self.files)
        self._shutting_down = False
        self.timeout = app.env_float("REQUEST_TIMEOUT", DEFAULT_REQUEST_TIMEOUT)
        self._timeouts = 0

    def describe(self) -> str:
        return f"pid {self.hello.get('pid')}, endpoint {self.files.describe_endpoint()}, log {self.files.log}"

    def _call(self, payload: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        timeout = self.timeout if timeout is None else timeout
        secret = bool(payload.get("params"))
        try:
            if secret:
                self.files.check_trusted()
            response = request(self.files, payload, timeout=timeout)
        except DaemonNotTrusted:
            raise
        except DaemonUnavailable:
            if self._shutting_down:
                raise
            # The daemon exited (idle, crash, drained or stopped by hand). Start a new one; subscriptions then come
            # back as unknown and the watcher subscribes again.
            self.hello = ensure_daemon(self.app, self.files)
            if secret:
                self.files.check_trusted()
            response = request(self.files, payload, timeout=timeout)
        error = str(response.get("error", ""))
        if not response.get("ok") and error.startswith("unknown op"):
            raise DaemonMismatch(
                f"the daemon (pid {self.hello.get('pid')}, {self.app.name} {self.hello.get('app_version')}) answered"
                f" {error}: it runs other code than this watcher. Run `{self.app.command} daemon stop`; the running"
                " watchers then start a current daemon and subscribe again"
            )
        return _check(response)

    def subscribe(
        self,
        client_id: str,
        target: Target,
        events: frozenset[str],
        baseline: Any,
        params: Params,
        source_options: Mapping[str, Any],
    ) -> tuple[str, int]:
        response = self._call(_subscribe_payload(client_id, target, events, baseline, params, source_options))
        return str(response["subscription_id"]), int(response["cursor"])

    def poll(self, client_id: str, cursors: dict[str, int], acks: dict[str, int]) -> dict[str, Any]:
        """A poll; a daemon that let `HUNG_AFTER` polls in a row time out is taken for hung and replaced.

        It is killed only when its pid is verified (see `kill_hung_daemon`), else this raises `DaemonHung`. The
        answer of the new daemon names the subscription unknown, and the watcher subscribes again."""
        payload = {"op": "poll", "client_id": client_id, "cursors": cursors, "acks": acks}
        try:
            response = self._call(payload)
        except DaemonTimeout:
            self._timeouts += 1
            if self._timeouts < HUNG_AFTER or self._shutting_down:
                raise
            self._timeouts = 0
            if kill_hung_daemon(self.app, self.files) is None:
                pid = self.files.owner_pid()
                raise DaemonHung(
                    f"the daemon (pid {pid if pid is not None else 'unknown'}) did not answer {HUNG_AFTER} polls in a"
                    f" row and could not be verified and killed; run `{self.app.command} daemon stop`"
                ) from None
            self.hello = ensure_daemon(self.app, self.files)
            response = self._call(payload)
        self._timeouts = 0
        return response

    def call(self, op: str, payload: Mapping[str, Any], params: Params | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {**payload, "op": op}
        if params:
            body["params"] = dict(params)
        return self._call(body, OP_TIMEOUT)

    def unsubscribe(self, client_id: str, subscription_id: str) -> None:
        """Best effort: one short attempt, never a spawn. If the daemon is gone, the lease expiry cleans up."""
        payload = {"op": "unsubscribe", "client_id": client_id, "subscription_id": subscription_id}
        with contextlib.suppress(DaemonUnavailable, OSError, ValueError):
            request(self.files, payload, timeout=1.0)

    def begin_shutdown(self) -> None:
        self._shutting_down = True

    def close(self) -> None:
        pass


class InProcessBackend:
    """The engine and the daemon's request handler on their own event loop in a daemon thread."""

    def __init__(self, app: WatchApp, engine: Engine | None = None) -> None:
        self.app = app
        self.engine = engine or Engine.for_app(app)
        self._identity = app.identity()
        self._loop = asyncio.new_event_loop()
        self._stop: asyncio.Event | None = None
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"{app.name}-engine", daemon=True)
        self._thread.start()
        self._ready.wait()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)

        async def main() -> None:
            self._stop = asyncio.Event()
            self._ready.set()
            await self.engine.run(self._stop)

        self._loop.run_until_complete(main())

    def _call(self, payload: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        # A private stop event: `stop` and `drain` from inside the process must not end the engine under the watcher.
        async def invoke() -> dict[str, Any]:
            return await handle(self.engine, payload, asyncio.Event(), self._identity)

        future: concurrent.futures.Future[dict[str, Any]] = asyncio.run_coroutine_threadsafe(invoke(), self._loop)
        return _check(future.result(timeout=timeout))

    def describe(self) -> str:
        return "in-process (--no-daemon), private budget"

    def subscribe(
        self,
        client_id: str,
        target: Target,
        events: frozenset[str],
        baseline: Any,
        params: Params,
        source_options: Mapping[str, Any],
    ) -> tuple[str, int]:
        response = self._call(_subscribe_payload(client_id, target, events, baseline, params, source_options))
        return str(response["subscription_id"]), int(response["cursor"])

    def poll(self, client_id: str, cursors: dict[str, int], acks: dict[str, int]) -> dict[str, Any]:
        return self._call({"op": "poll", "client_id": client_id, "cursors": cursors, "acks": acks})

    def call(self, op: str, payload: Mapping[str, Any], params: Params | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {**payload, "op": op}
        if params:
            body["params"] = dict(params)
        return self._call(body, OP_TIMEOUT)

    def unsubscribe(self, client_id: str, subscription_id: str) -> None:
        self._call({"op": "unsubscribe", "client_id": client_id, "subscription_id": subscription_id})

    def begin_shutdown(self) -> None:
        pass

    def close(self) -> None:
        stop = self._stop
        if stop is not None:
            self._loop.call_soon_threadsafe(stop.set)
        self._thread.join(timeout=10)


def default_backend(app: WatchApp, no_daemon: bool) -> Backend:
    if no_daemon:
        return InProcessBackend(app)
    return DaemonBackend(app)


class Writer(Protocol):
    """An app's writer of the resource dir: the state file from each snapshot, artifacts attached to events."""

    def snapshot(self, data: Any) -> None: ...

    def attach(self, events: list[Event]) -> list[Event]: ...


@dataclass(frozen=True)
class WriterContext:
    """What an app's writer is made from: the base dir and the resource dir of the watch, its target, the backend
    (for the source's extra ops, such as fetching a log the daemon caches) and the status output."""

    base_dir: Path
    resource_dir: Path
    target: Target
    backend: Backend
    status: StatusOutput


@dataclass(frozen=True)
class WatchSettings:
    """What one watch reports and when it ends.

    `events` are the kinds reported, lifecycle and threshold kinds included; `subscribed` are the kinds asked of the
    daemon (default: `events` without the thresholds, which the caller passes via `threshold_kinds`). `params` (secrets
    such as an API key) and `source_options` (what the source should do for this watcher) go with the subscription
    to the daemon. A `watch_ended` whose reason is in `error_reasons` exits with 1.
    """

    target: Target
    events: frozenset[str]
    until: str | None = None
    timeout: float | None = None
    idle_after: float | None = None
    params: Mapping[str, Any] = field(default_factory=dict)
    source_options: Mapping[str, Any] = field(default_factory=dict)
    threshold_kinds: frozenset[str] = frozenset()
    subscribed: frozenset[str] | None = None
    error_reasons: frozenset[str] = frozenset({NotFound.reason})

    @property
    def subscribed_events(self) -> frozenset[str]:
        if self.subscribed is not None:
            return self.subscribed
        return self.events - self.threshold_kinds


@dataclass(frozen=True)
class WatchedTarget:
    """One more target of a :class:`Watcher` that watches several: its changelog, and the app's writer and threshold
    timers for it. It is watched with the same :class:`WatchSettings` as the first target, so it should be of the same
    kind."""

    target: Target
    changelog: Changelog
    writer: Writer | None = None
    timers: Sequence[ClientTimer] = ()


@dataclass
class _Track:
    """What the watcher keeps per target."""

    target: Target
    changelog: Changelog
    writer: Writer | None
    timers: list[ClientTimer]
    subscription: str | None = None
    cursor: int = 0
    """The daemon's sequence number of the last event received. It restarts when the daemon restarts, so it is only
    the poll cursor. Reported events carry the watcher's own `seq`, which never goes back."""
    seq: int = 0
    snapshot_hash: str | None = None
    resubscribed: bool = False
    ended: bool = False


class Watcher:
    """One watch: subscribes, polls the backend, numbers and delivers the events, and ends.

    With `more`, one watcher watches several targets in one stream: each event carries the key of its resource in
    `resource`, each target keeps its own changelog, baseline and numbering, and all of them share the settings, the
    sink, one subscription lease and one poll of the backend. A target that ends (`watch_ended` from the daemon) drops
    out; the watch ends when every target ended, or for all of them at once on `until` (the first event of that kind
    of any target), the timeout, `idle_after` (no event of any target for that long), a stop or a gone session. It
    exits with 1 when any target ended for one of the `error_reasons`.

    It does not lock its resource dirs. `run_watch` does, so that a second watcher of the same resource is refused;
    a CLI that builds Watcher objects itself must make sure no two of them share a changelog.
    """

    def __init__(
        self,
        settings: WatchSettings,
        backend: Backend,
        sink: Sink,
        changelog: Changelog,
        status: StatusOutput,
        *,
        writer: Writer | None = None,
        timers: Sequence[ClientTimer] = (),
        clock: Callable[[], float] = time.monotonic,
        retry_delays: tuple[float, float] = (1.0, 60.0),
        more: Sequence[WatchedTarget] = (),
    ) -> None:
        self.settings = settings
        self.backend = backend
        self.sink = sink
        self.changelog = changelog
        self.status = status
        self.writer = writer
        self.timers = list(timers)
        self.clock = clock
        self.retry_delays = retry_delays
        """The first and the longest wait between attempts to subscribe again."""
        self.client_id = uuid.uuid4().hex
        self._stop_reason: str | None = None
        self._emitting_since: float | None = None
        self._stuck = False
        self._wakeup = threading.Event()
        self._acks: dict[str, int] = {}
        self._tracks = [_Track(settings.target, changelog, writer, self.timers)]
        self._tracks += [_Track(m.target, m.changelog, m.writer, list(m.timers)) for m in more]
        keys = [t.target.key for t in self._tracks]
        if len(set(keys)) != len(keys):
            raise ValueError(f"a watcher watches each target once, got {', '.join(keys)}")

    def request_stop(self, reason: str = "stopped") -> None:
        self._stop_reason = reason
        self.backend.begin_shutdown()
        self._wakeup.set()

    def _install_signal_handlers(self) -> dict[int, Any]:
        """Turn SIGTERM and SIGINT into a stop with `watch_ended`. Return the handlers to put back afterwards."""
        if threading.current_thread() is not threading.main_thread():
            return {}

        def handler(signum: int, frame: FrameType | None) -> None:
            self.request_stop(f"signal {signal.Signals(signum).name}")
            started = self._emitting_since
            if started is None:
                return
            if time.monotonic() - started >= STUCK_WRITE:
                # A write to a reader that stopped reading (a full pipe) would block the stop forever.
                raise _Interrupted

            # A write in progress is let finish; if it still runs when this fires, it is stuck, and the signal
            # comes again to break it.
            def again() -> None:
                if self._emitting_since == started:
                    os.kill(os.getpid(), signum)

            timer = threading.Timer(STUCK_WRITE, again)
            timer.daemon = True
            timer.start()

        return {sig: signal.signal(sig, handler) for sig in (signal.SIGTERM, signal.SIGINT)}

    def _subscribe(self, track: _Track, resubscribe: bool) -> None:
        baseline = None
        if resubscribe:
            stored = track.changelog.load_baseline()
            baseline = stored.snapshot if stored else None
        s = self.settings
        track.subscription, track.cursor = self.backend.subscribe(
            self.client_id, track.target, s.subscribed_events, baseline, s.params, s.source_options
        )

    def _resubscribe(self, track: _Track, deadline: float | None) -> bool:
        """Subscribe again with the baseline, retrying with a backoff while the trouble may pass (a daemon that is
        just restarting, an API that is unreachable after a suspend). Return whether it worked; on a stop or the
        deadline it gives up and the loop ends the watch."""
        delay, longest = self.retry_delays
        while True:
            try:
                self._subscribe(track, resubscribe=True)
                return True
            except BackendError as exc:
                if exc.reason not in RETRYABLE_REASONS:
                    raise
                error: Exception = exc
            except (DaemonStartFailed, DaemonNotTrusted, DaemonHung):
                raise
            except (DaemonUnavailable, TimeoutError) as exc:
                error = exc
                if isinstance(exc, DaemonTimeout):
                    # The daemon may have created the subscription after all. A new client id lets that one expire
                    # with its lease, instead of being renewed by this watcher's polls for nothing. The subscriptions
                    # of the other targets then come back as unknown, and they subscribe again too.
                    self.client_id = uuid.uuid4().hex
            if deadline is not None and self.clock() + delay >= deadline:
                self.request_stop("timeout")
                return False
            self.status.line(f"subscribing again failed: {error}; trying again in {format_duration(delay)}")
            self._wakeup.wait(delay)
            if self._stop_reason is not None:
                return False
            delay = min(delay * 2, longest)

    def _label(self, track: _Track) -> str:
        """What a status line about one target starts with: nothing when the watch has only that one."""
        return f"{track.target.key}: " if len(self._tracks) > 1 else ""

    def _end_event(self, track: _Track, reason: str, **data: Any) -> Event:
        track.seq += 1
        return Event(WATCH_ENDED, track.target.key, track.seq, utc_now_iso(), {"reason": reason, **data})

    def _deliver(self, track: _Track, events: list[Event]) -> None:
        if not events:
            return
        track.changelog.append(events)
        self._to_sink(lambda: self.sink.emit(events))

    def _to_sink(self, write: Callable[[], None]) -> None:
        """Run a write to the sink (an emit, a tick that sends a buffered batch) that a stop signal may break when it
        blocks. A sink that blocked once gets nothing more; the changelog keeps every event."""
        if self._stuck:
            return
        self._emitting_since = time.monotonic()
        try:
            write()
        except _Interrupted:
            self._stuck = True
        finally:
            self._emitting_since = None

    def _abandon_stdout(self) -> None:
        """The reader of stdout stopped reading. Python flushes stdout at exit, which would block on the same full
        pipe, so point this process's stdout at /dev/null. Only this process's fd changes: the pipe, which other
        processes may share, keeps its mode."""
        self.status.line("the event stream stopped taking events; they are in the changelog")
        if self.sink.name != "stdout":
            return
        with contextlib.suppress(OSError, ValueError):
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, sys.stdout.fileno())
            os.close(devnull)

    def _finish(self, reason: str, **data: Any) -> None:
        """End every target still watched with `watch_ended` for `reason`."""
        for track in self._tracks:
            if not track.ended:
                track.ended = True
                self._deliver(track, [self._end_event(track, reason, **data)])
        self.status.line(f"ended: {reason}")

    def _exit_code(self, failed: bool) -> int:
        return EXIT_ERROR if failed else EXIT_OK

    def _receive(self, track: _Track, response: Mapping[str, Any], idle: IdleTimer) -> list[Event]:
        """The events of one target in a poll's answer, filtered, attached and numbered; its snapshot taken in."""
        s = self.settings
        sid = track.subscription
        previous_hash = track.snapshot_hash
        snapshot = (response.get("snapshots") or {}).get(sid)
        if snapshot:
            track.snapshot_hash = str(snapshot["hash"])
            track.changelog.save_baseline(track.snapshot_hash, snapshot.get("data"))
            if track.writer is not None:
                track.writer.snapshot(snapshot.get("data"))
            for timer in track.timers:
                timer.snapshot(snapshot.get("data"), self.clock())
        raw = [Event.from_json(e) for e in (response.get("events") or {}).get(sid) or []]
        for event in raw:
            track.cursor = max(track.cursor, event.seq)
        events = [e for e in raw if e.kind in s.events]
        if track.resubscribed and any(e.kind == WATCH_STARTED for e in events):
            # After a daemon restart, the new watch_started repeats what we already know unless the snapshot
            # moved; changes in between arrive as normal events, diffed against our stored snapshot.
            events = [
                e for e in events if not (e.kind == WATCH_STARTED and e.data.get("snapshot_hash") == previous_hash)
            ]
            track.resubscribed = False
        if track.writer is not None:
            events = track.writer.attach(events)
        now = self.clock()
        for timer in track.timers:
            timer.reported(events, now)
            events += [Event(kind, track.target.key, 0, utc_now_iso(), dict(data)) for kind, data in timer.due(now)]
        idle.reported(events, now)
        numbered = []
        for event in events:
            track.seq += 1
            numbered.append(replace(event, seq=track.seq))
        return numbered

    def _drop(self, track: _Track) -> None:
        """Unsubscribe a target that ended while others are still watched."""
        sid, track.subscription = track.subscription, None
        if sid is not None:
            with contextlib.suppress(BackendError, DaemonUnavailable, OSError, TimeoutError):
                self.backend.unsubscribe(self.client_id, sid)

    def run(self) -> int:
        previous_handlers = self._install_signal_handlers()
        s = self.settings
        deadline = self.clock() + s.timeout if s.timeout is not None else None
        for track in self._tracks:
            stored = track.changelog.load_baseline()
            track.snapshot_hash = stored.snapshot_hash if stored else None
            # Continue the changelog's numbering, so events.jsonl stays strictly increasing across watcher runs too.
            track.seq = track.changelog.last_seq()
        idle = IdleTimer(s.idle_after, s.threshold_kinds, self.clock())
        failed = False
        try:
            for track in self._tracks:
                self._subscribe(track, resubscribe=False)
            while True:
                if self._stop_reason is not None:
                    self._finish(self._stop_reason)
                    return self._exit_code(failed)
                active = [t for t in self._tracks if not t.ended]
                self._to_sink(self.sink.tick)
                cursors = {t.subscription: t.cursor for t in active if t.subscription is not None}
                try:
                    response = self.backend.poll(self.client_id, cursors, self._acks)
                except DaemonTimeout as exc:
                    # A busy daemon: polling is safe to repeat, so ask again rather than give up the watch.
                    if deadline is not None and self.clock() >= deadline:
                        self._finish("timeout")
                        return self._exit_code(failed)
                    self.status.line(f"{exc}; polling again ({HUNG_AFTER} in a row and the daemon is replaced)")
                    self._wakeup.wait(1.0)
                    continue
                self._acks = {k: int(v) for k, v in (response.get("acks") or {}).items()}
                unknown = set(response.get("unknown", []))
                restarted = [t for t in active if t.subscription in unknown]
                hit: Event | None = None
                for track in active:
                    if track.subscription in unknown:
                        continue
                    events = self._receive(track, response, idle)
                    ended = next((e for e in events if e.kind == WATCH_ENDED), None)
                    if hit is None:
                        hit = next((e for e in events if s.until and e.kind == s.until), None)
                    self._deliver(track, events)
                    if ended is not None:
                        track.ended = True
                        reason = str(ended.data.get("reason"))
                        already = (
                            f" (already {reason} when the watch started)" if ended.data.get("already_ended") else ""
                        )
                        self.status.line(f"ended: {self._label(track)}{reason}{already}")
                        failed = failed or reason in s.error_reasons
                        self._drop(track)
                if all(t.ended for t in self._tracks):
                    return self._exit_code(failed)
                if hit is not None:
                    data: dict[str, Any] = {"event": hit.kind, "seq": hit.seq}
                    if len(self._tracks) > 1:
                        data["resource"] = hit.resource
                    self._finish("until", **data)
                    return self._exit_code(failed)
                if restarted:
                    self.status.line("daemon restarted, subscribing again")
                    for track in restarted:
                        if not self._resubscribe(track, deadline):
                            break
                        track.resubscribed = True
                    continue
                if not self.sink.alive():
                    self._finish("session_gone")
                    return self._exit_code(failed)
                if deadline is not None and self.clock() >= deadline:
                    self._finish("timeout")
                    return self._exit_code(failed)
                if idle.idle(self.clock()):
                    self._finish("idle", idle_after=format_duration(s.idle_after or 0.0))
                    return self._exit_code(failed)
                self._wakeup.wait(self._wait(response, deadline, idle))
        except (BackendError, DaemonMismatch, DaemonUnavailable, TimeoutError) as exc:
            if self._stop_reason is not None:
                # Stopped while a request was in flight, and the daemon is gone: end like any other stop.
                self._finish(self._stop_reason)
                return self._exit_code(failed)
            self.status.line(f"error: {exc}")
            return EXIT_ERROR
        finally:
            self.backend.begin_shutdown()
            for track in self._tracks:
                if track.subscription is not None:
                    with contextlib.suppress(BackendError, DaemonUnavailable, OSError, TimeoutError):
                        self.backend.unsubscribe(self.client_id, track.subscription)
            try:
                if self._stuck:
                    self._abandon_stdout()
                else:
                    self.sink.close()
            finally:
                for sig, previous in previous_handlers.items():
                    # `None` means a handler not installed from Python; the default is the closest one to put back.
                    signal.signal(sig, previous if previous is not None else signal.SIG_DFL)

    def _wait(self, response: Mapping[str, Any], deadline: float | None, idle: IdleTimer) -> float:
        now = self.clock()
        wait = int(response.get("next_poll_ms", 5000)) / 1000.0
        if deadline is not None:
            wait = max(0.0, min(wait, deadline - now))
        candidates = [timer.seconds_until_next(now) for t in self._tracks if not t.ended for timer in t.timers]
        candidates += [idle.seconds_until_next(now), self.sink.seconds_until_flush()]
        for candidate in candidates:
            if candidate is not None:
                wait = min(wait, candidate)
        return max(0.0, wait)
