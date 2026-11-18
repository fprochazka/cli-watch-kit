"""The watcher: subscribe through a backend, poll it for events, write the changelog, deliver to a sink.

Two backends speak the same requests. :class:`DaemonBackend` talks to the shared per-user daemon and starts a new one
when it disappears. :class:`InProcessBackend` (`--no-daemon`) runs the same engine and request handler in a thread of
this process, with a private budget.

The watcher numbers the events it reports itself (`seq`), strictly increasing across daemon restarts and across
watcher runs on the same changelog. After a daemon restart it subscribes again with its baseline, so the new daemon
reports only what changed, and it drops the new `watch_started` when the snapshot did not move.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import signal
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
from cli_watch_kit.daemon.lifecycle import ensure_daemon
from cli_watch_kit.daemon.server import handle
from cli_watch_kit.daemon.transport import (
    DaemonFiles,
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

    def describe(self) -> str:
        return f"pid {self.hello.get('pid')}, endpoint {self.files.describe_endpoint()}, log {self.files.log}"

    def _call(self, payload: dict[str, Any], timeout: float = 10.0) -> dict[str, Any]:
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
        return self._call({"op": "poll", "client_id": client_id, "cursors": cursors, "acks": acks})

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
        self.engine = engine or Engine(app.make_source())
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


class Watcher:
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
        self._wakeup = threading.Event()
        self._subscription: str | None = None
        # The daemon's sequence number of the last event received. It restarts when the daemon restarts, so it is
        # only the poll cursor. Reported events carry the watcher's own `_seq`, which never goes back.
        self._cursor = 0
        self._acks: dict[str, int] = {}
        self._seq = 0
        self._snapshot_hash: str | None = None

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

        return {sig: signal.signal(sig, handler) for sig in (signal.SIGTERM, signal.SIGINT)}

    def _subscribe(self, resubscribe: bool) -> None:
        baseline = None
        if resubscribe:
            stored = self.changelog.load_baseline()
            baseline = stored.snapshot if stored else None
        s = self.settings
        self._subscription, self._cursor = self.backend.subscribe(
            self.client_id, s.target, s.subscribed_events, baseline, s.params, s.source_options
        )

    def _resubscribe(self, deadline: float | None) -> bool:
        """Subscribe again with the baseline, retrying with a backoff while the trouble may pass (a daemon that is
        just restarting, an API that is unreachable after a suspend). Return whether it worked; on a stop or the
        deadline it gives up and the loop ends the watch."""
        delay, longest = self.retry_delays
        while True:
            try:
                self._subscribe(resubscribe=True)
                return True
            except BackendError as exc:
                if exc.reason not in RETRYABLE_REASONS:
                    raise
                error: Exception = exc
            except (DaemonStartFailed, DaemonNotTrusted):
                raise
            except (DaemonUnavailable, TimeoutError) as exc:
                error = exc
                if isinstance(exc, DaemonTimeout):
                    # The daemon may have created the subscription after all. A new client id lets that one expire
                    # with its lease, instead of being renewed by this watcher's polls for nothing.
                    self.client_id = uuid.uuid4().hex
            if deadline is not None and self.clock() + delay >= deadline:
                self.request_stop("timeout")
                return False
            self.status.line(f"subscribing again failed: {error}; trying again in {format_duration(delay)}")
            self._wakeup.wait(delay)
            if self._stop_reason is not None:
                return False
            delay = min(delay * 2, longest)

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _end_event(self, reason: str, **data: Any) -> Event:
        key = self.settings.target.key
        return Event(WATCH_ENDED, key, self._next_seq(), utc_now_iso(), {"reason": reason, **data})

    def _deliver(self, events: list[Event]) -> None:
        if not events:
            return
        self.changelog.append(events)
        self.sink.emit(events)

    def _finish(self, event: Event) -> None:
        self._deliver([event])
        self.status.line(f"ended: {event.data.get('reason')}")

    def run(self) -> int:
        previous_handlers = self._install_signal_handlers()
        s = self.settings
        deadline = self.clock() + s.timeout if s.timeout is not None else None
        stored = self.changelog.load_baseline()
        self._snapshot_hash = stored.snapshot_hash if stored else None
        # Continue the changelog's numbering, so events.jsonl stays strictly increasing across watcher runs too.
        self._seq = self.changelog.last_seq()
        resubscribed = False
        idle = IdleTimer(s.idle_after, s.threshold_kinds, self.clock())
        try:
            self._subscribe(resubscribe=False)
            while True:
                if self._stop_reason is not None:
                    self._finish(self._end_event(self._stop_reason))
                    return EXIT_OK
                assert self._subscription is not None
                self.sink.tick()
                try:
                    response = self.backend.poll(self.client_id, {self._subscription: self._cursor}, self._acks)
                except DaemonTimeout as exc:
                    # A busy daemon: polling is safe to repeat, so ask again rather than give up the watch.
                    if deadline is not None and self.clock() >= deadline:
                        self._finish(self._end_event("timeout"))
                        return EXIT_OK
                    self.status.line(f"{exc}; polling again")
                    self._wakeup.wait(1.0)
                    continue
                self._acks = {k: int(v) for k, v in (response.get("acks") or {}).items()}
                if self._subscription in response.get("unknown", []):
                    self.status.line("daemon restarted, subscribing again")
                    if self._resubscribe(deadline):
                        resubscribed = True
                    continue
                previous_hash = self._snapshot_hash
                snapshot = (response.get("snapshots") or {}).get(self._subscription)
                if snapshot:
                    self._snapshot_hash = str(snapshot["hash"])
                    self.changelog.save_baseline(self._snapshot_hash, snapshot.get("data"))
                    if self.writer is not None:
                        self.writer.snapshot(snapshot.get("data"))
                    for timer in self.timers:
                        timer.snapshot(snapshot.get("data"), self.clock())
                raw = [Event.from_json(e) for e in (response.get("events") or {}).get(self._subscription) or []]
                for event in raw:
                    self._cursor = max(self._cursor, event.seq)
                events = [e for e in raw if e.kind in s.events]
                if resubscribed and any(e.kind == WATCH_STARTED for e in events):
                    # After a daemon restart, the new watch_started repeats what we already know unless the snapshot
                    # moved; changes in between arrive as normal events, diffed against our stored snapshot.
                    events = [
                        e
                        for e in events
                        if not (e.kind == WATCH_STARTED and e.data.get("snapshot_hash") == previous_hash)
                    ]
                    resubscribed = False
                if self.writer is not None:
                    events = self.writer.attach(events)
                now = self.clock()
                for timer in self.timers:
                    timer.reported(events, now)
                    events += [Event(kind, s.target.key, 0, utc_now_iso(), dict(data)) for kind, data in timer.due(now)]
                idle.reported(events, now)
                events = [replace(e, seq=self._next_seq()) for e in events]
                ended = next((e for e in events if e.kind == WATCH_ENDED), None)
                hit = next((e for e in events if s.until and e.kind == s.until), None)
                self._deliver(events)
                if ended is not None:
                    reason = str(ended.data.get("reason"))
                    already = f" (already {reason} when the watch started)" if ended.data.get("already_ended") else ""
                    self.status.line(f"ended: {reason}{already}")
                    return EXIT_ERROR if reason in s.error_reasons else EXIT_OK
                if hit is not None:
                    self._finish(self._end_event("until", event=hit.kind, seq=hit.seq))
                    return EXIT_OK
                if not self.sink.alive():
                    self._finish(self._end_event("session_gone"))
                    return EXIT_OK
                if deadline is not None and self.clock() >= deadline:
                    self._finish(self._end_event("timeout"))
                    return EXIT_OK
                if idle.idle(self.clock()):
                    self._finish(self._end_event("idle", idle_after=format_duration(s.idle_after or 0.0)))
                    return EXIT_OK
                self._wakeup.wait(self._wait(response, deadline, idle))
        except (BackendError, DaemonMismatch, DaemonUnavailable, TimeoutError) as exc:
            if self._stop_reason is not None:
                # Stopped while a request was in flight, and the daemon is gone: end like any other stop.
                self._finish(self._end_event(self._stop_reason))
                return EXIT_OK
            self.status.line(f"error: {exc}")
            return EXIT_ERROR
        finally:
            self.backend.begin_shutdown()
            if self._subscription is not None:
                with contextlib.suppress(BackendError, DaemonUnavailable, OSError, TimeoutError):
                    self.backend.unsubscribe(self.client_id, self._subscription)
            self.sink.close()
            for sig, previous in previous_handlers.items():
                signal.signal(sig, previous)

    def _wait(self, response: Mapping[str, Any], deadline: float | None, idle: IdleTimer) -> float:
        now = self.clock()
        wait = int(response.get("next_poll_ms", 5000)) / 1000.0
        if deadline is not None:
            wait = max(0.0, min(wait, deadline - now))
        candidates = [t.seconds_until_next(now) for t in self.timers]
        candidates += [idle.seconds_until_next(now), self.sink.seconds_until_flush()]
        for candidate in candidates:
            if candidate is not None:
                wait = min(wait, candidate)
        return max(0.0, wait)
