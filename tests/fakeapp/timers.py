"""`stale`: an item whose status did not change for a threshold's length, once per status."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from cli_watch_kit.durations import format_duration
from cli_watch_kit.events import Event


class StaleTimer:
    def __init__(self, after: float, now: float) -> None:
        self.after = after
        self._status: str | None = None
        self._since = now
        self._sent: set[str] = set()

    def snapshot(self, data: Any, now: float) -> None:
        status = data.get("status") if isinstance(data, dict) else None
        if isinstance(status, str) and status != self._status:
            self._status = status
            self._since = now

    def reported(self, events: Iterable[Event], now: float) -> None:
        pass

    def due(self, now: float) -> list[tuple[str, dict[str, Any]]]:
        status = self._status
        if status is None or status in self._sent or now - self._since < self.after:
            return []
        self._sent.add(status)
        return [("stale", {"status": status, "after": format_duration(self.after)})]

    def seconds_until_next(self, now: float) -> float | None:
        if self._status is None or self._status in self._sent:
            return None
        return max(0.0, self._since + self.after - now)

    def describe(self) -> str | None:
        return f"stale after {format_duration(self.after)}"
