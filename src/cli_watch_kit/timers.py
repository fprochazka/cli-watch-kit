"""What a watcher derives from time: the threshold events of the app, and the idle end of a watch.

Both come from the snapshots and events the watcher already receives, so they cost no request. The daemon never sees
them: they depend on what this watcher reported, which differs per watcher of the same resource.

An app adds its own threshold kinds (glab's `quiet` and `pipeline_stuck`, for example) as :class:`ClientTimer` objects
and names their options in the catalog's `thresholds`. :class:`IdleTimer` ends a watch when nothing was reported for
`--idle-after`; threshold events do not count as activity, so a quiet resource stays idle after `quiet`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol

from cli_watch_kit.durations import format_duration
from cli_watch_kit.events import Event


class ClientTimer(Protocol):
    """An app's threshold event source inside the watcher. The watcher passes a monotonic `now` everywhere."""

    def snapshot(self, data: Any, now: float) -> None:
        """Take in a snapshot the daemon sent, as the source encoded it."""
        ...

    def reported(self, events: Iterable[Event], now: float) -> None:
        """The events the watcher just reported, threshold events included."""
        ...

    def due(self, now: float) -> list[tuple[str, dict[str, Any]]]:
        """The threshold events due now, as (kind, data). Each should be returned once."""
        ...

    def seconds_until_next(self, now: float) -> float | None:
        """How long until the next threshold can fire, so the watcher does not sleep past it."""
        ...

    def describe(self) -> str | None:
        """A part of the header's `thresholds:` line, such as `quiet after 2m`, or `None`."""
        ...


class IdleTimer:
    """Ends a watch when nothing but threshold events was reported for `idle_after` seconds."""

    def __init__(self, idle_after: float | None, threshold_kinds: Iterable[str], now: float) -> None:
        self.idle_after = idle_after
        self.threshold_kinds = frozenset(threshold_kinds)
        self._last_activity = now

    def reported(self, events: Iterable[Event], now: float) -> None:
        if any(e.kind not in self.threshold_kinds for e in events):
            self._last_activity = now

    def idle(self, now: float) -> bool:
        return self.idle_after is not None and now - self._last_activity >= self.idle_after

    def seconds_until_next(self, now: float) -> float | None:
        if self.idle_after is None:
            return None
        return max(0.0, self._last_activity + self.idle_after - now)

    def describe(self) -> str | None:
        if self.idle_after is None:
            return None
        return f"after {format_duration(self.idle_after)} without an event"
