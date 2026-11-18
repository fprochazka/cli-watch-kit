"""Where a watcher delivers its events, and where its own status lines go.

Status lines (the startup header, the end line, errors) go to stderr, so stdout carries only events. Claude Code puts
a background Bash task's stderr into the task output next to its stdout, so an agent reads both. Status lines start
with the app's prefix and never with `{`, so a reader of the merged output can still tell them from events.

:class:`EventSink` has the shape of cc-blackbox's event sinks, so its `MessageEventSink` fits without an adapter;
see :mod:`cli_watch_kit.claude`.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from typing import Any, Protocol, TextIO, TypeVar

from cli_watch_kit.events import Event


class StreamEvent(Protocol):
    @property
    def kind(self) -> str: ...

    @property
    def seq(self) -> int: ...

    def to_json(self) -> Mapping[str, Any]: ...


E_contra = TypeVar("E_contra", bound=StreamEvent, contravariant=True)


class EventSink(Protocol[E_contra]):
    """Where a watcher delivers its events. The watcher calls :meth:`tick` when :meth:`seconds_until_flush` says so."""

    name: str

    def describe(self) -> str:
        """One line for the startup header: the stream mode and its target."""
        ...

    def emit(self, events: Sequence[E_contra]) -> None: ...

    def seconds_until_flush(self) -> float | None:
        """When a buffered batch is due, so the watcher wakes up for it; `None` when nothing is buffered."""
        ...

    def tick(self) -> None:
        """Deliver a buffered batch whose time has come."""
        ...

    def alive(self) -> bool:
        """`False` once the target is gone, which ends the watch."""
        ...

    def close(self) -> None: ...


type Sink = EventSink[Event]


class StdoutSink:
    """One compact JSON object per line, flushed after every batch."""

    name = "stdout"

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream

    def describe(self) -> str:
        return "stdout (one JSON event per line)"

    def emit(self, events: Sequence[StreamEvent]) -> None:
        stream = self._stream or sys.stdout
        for event in events:
            stream.write(json.dumps(event.to_json(), ensure_ascii=False, separators=(",", ":")) + "\n")
        stream.flush()

    def seconds_until_flush(self) -> float | None:
        return None

    def tick(self) -> None:
        pass

    def alive(self) -> bool:
        return True

    def close(self) -> None:
        pass


class StatusOutput:
    """Status lines with the app's prefix, on stderr unless a stream is given."""

    def __init__(self, prefix: str, stream: TextIO | None = None) -> None:
        self.prefix = prefix
        self._stream = stream

    @property
    def stream(self) -> TextIO:
        return self._stream if self._stream is not None else sys.stderr

    def line(self, text: str) -> None:
        self.stream.write(f"{self.prefix}{text}\n")
        self.stream.flush()
