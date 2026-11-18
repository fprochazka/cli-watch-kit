"""Choosing the events stream: stdout, or messages into the Claude Code session that started the watcher.

The `claude-message` stream needs cc-blackbox, which this module imports only when it is used, from the
`claude-code` extra (`pip install 'cli-watch-kit[claude-code]'`). cc-blackbox finds the session, checks that its
messaging protocol is one it speaks, batches the events that arrive within 3 s into one message, caps a message at
64 KB, keeps below the session's rate limit, notices when the session is gone, and moves the events to stdout when
messaging stops working. Without the extra, `auto` falls back to stdout and says which extra to install.

Each message is a `<{envelope_tag} resource="..." seq="...">` block for one resource, sent as `<app>:<pid>` under a
sender name the app chooses. Only the message with `watch_started` names the resource dir and the changelog.
Delivery is best effort: the receiver may rate-limit a message, or hold it until its user approves it. The changelog
always has every event.
"""

from __future__ import annotations

import importlib.util
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from cli_watch_kit.events import WATCH_ENDED, WATCH_STARTED, Event
from cli_watch_kit.sinks import Sink, StatusOutput, StdoutSink

if TYPE_CHECKING:
    from cc_blackbox import ActiveSession, EventEnvelope, RateLimitedSender

    from cli_watch_kit.app import WatchApp

EXTRA = "claude-code"
INSTALL_HINT = "install the claude-code extra of cli-watch-kit (cc-blackbox) to message the session"
STREAM_MODES: tuple[str, ...] = ("auto", "stdout", "claude-message")
MESSAGE_PRIORITIES: tuple[str, ...] = ("now", "next", "later")


class StreamError(RuntimeError):
    pass


def claude_code_available() -> bool:
    """Whether the `claude-code` extra is installed."""
    return importlib.util.find_spec("cc_blackbox") is not None


def find_session() -> ActiveSession | None:
    """The Claude Code session this process runs under, or `None`, also when the extra is not installed."""
    if not claude_code_available():
        return None
    from cc_blackbox import current_session_or_none

    return current_session_or_none()


def _under_claude_code() -> bool:
    return bool(os.environ.get("CLAUDE_CODE_MESSAGING_SOCKET") or os.environ.get("CLAUDECODE"))


@dataclass
class Stream:
    sink: Sink
    fallback: str | None = None
    """Why `auto` picked stdout although the watcher runs under a Claude Code session; printed in the header."""


def open_stream(
    app: WatchApp,
    mode: str,
    *,
    session: ActiveSession | None,
    resource: str,
    sender_name: str,
    changelog: Path,
    priority: str,
    status: StatusOutput,
) -> Stream:
    """The sink for `mode`. `session` is :func:`find_session`'s answer; `changelog` the watcher's events file."""
    if mode not in STREAM_MODES:
        raise StreamError(f"--events-stream must be one of {', '.join(STREAM_MODES)}")
    if priority not in MESSAGE_PRIORITIES:
        raise StreamError(f"--message-priority must be one of {', '.join(MESSAGE_PRIORITIES)}")
    if mode == "stdout":
        return Stream(StdoutSink())
    if not claude_code_available():
        if mode == "claude-message":
            raise StreamError(f"--events-stream=claude-message is not possible: {INSTALL_HINT}")
        return Stream(StdoutSink(), INSTALL_HINT if _under_claude_code() else None)
    from cc_blackbox import EventStreamMode, MessagingUnavailable, choose_event_stream

    try:
        choice = choose_event_stream(cast(EventStreamMode, mode), session)
    except MessagingUnavailable as exc:
        raise StreamError(f"--events-stream=claude-message is not possible: {exc}; use --events-stream=stdout") from exc
    if choice.mode == "claude-message" and choice.session is not None:
        sink = claude_message_sink(app, choice.session, resource, sender_name, changelog, priority, status)
        return Stream(sink)
    return Stream(StdoutSink(), choice.fallback)


def _event_payload(event: Event) -> Mapping[str, Any]:
    """An event as a message line: the envelope already names the resource, and the time is of no use there."""
    payload: dict[str, object] = {"kind": event.kind, "seq": event.seq, "data": event.data}
    if event.artifacts:
        payload["artifacts"] = list(event.artifacts)
    return payload


def _without_announced_dir(event: Event, directory: Path) -> Event:
    """`watch_started` as the announcing message shows it: the head names the dir, so the summary does not repeat it."""
    summary = event.data.get("summary")
    if event.kind != WATCH_STARTED or not isinstance(summary, dict):
        return event
    summary = {k: v for k, v in summary.items() if not (k == "dir" and v == str(directory))}
    return Event(event.kind, event.resource, event.seq, event.observed_at, {**event.data, "summary": summary})


def message_envelope(app: WatchApp, resource: str, changelog: Path) -> EventEnvelope[Event]:
    """`<tag resource="…" seq="…">`. The message with `watch_started` also names the resource dir and the changelog.
    Above 64 KB the body becomes a count per kind that points at the changelog."""
    from cc_blackbox import EventEnvelope

    return EventEnvelope(
        app.envelope_tag,
        {"resource": resource},
        payload=_event_payload,
        announce_on=WATCH_STARTED,
        announce_attributes={"dir": str(changelog.parent), "changelog": str(changelog)},
        announced=lambda event: _without_announced_dir(event, changelog.parent),
        record=str(changelog),
    )


def claude_message_sink(
    app: WatchApp,
    session: ActiveSession,
    resource: str,
    sender_name: str,
    changelog: Path,
    priority: str,
    status: StatusOutput,
    *,
    rate_limiter: RateLimitedSender | None = None,
    clock: Callable[[], float] = time.monotonic,
    session_alive: Callable[[], bool] | None = None,
) -> Sink:
    """The sink for one watched resource. `watch_ended` goes out at once, so the session learns the end without the
    3 s burst wait."""
    from cc_blackbox import MessageEventSink, Priority

    return MessageEventSink(
        session,
        message_envelope(app, resource, changelog),
        sender=f"{app.name}:{os.getpid()}",
        sender_name=sender_name,
        status=status.line,
        priority=cast(Priority, priority),
        flush_on={WATCH_ENDED},
        rate_limiter=rate_limiter,
        clock=clock,
        session_alive=session_alive,
    )
