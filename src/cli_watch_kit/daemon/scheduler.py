"""Which resources poll next, and which of them poll together.

Resources wait in due-time order. Among those that are due, urgent ones go first (a new subscription, a filter that
grew), then the one served longest ago. That round-robin keeps one busy resource from starving a quiet one when the
budget is short. The scheduler holds no clock and does no I/O; the engine passes the time in.

Resources of one batch group (the same account and batch key) that are due go into one batch, together with those of
the group that become due within the coalescing window. A resource pulled in early is then rescheduled from the
batch's time, so the schedules of one group converge and stay aligned.
"""

from __future__ import annotations

from collections.abc import Callable, Container, Hashable
from dataclasses import dataclass


@dataclass
class _Entry:
    due: float
    urgent: bool


class Scheduler:
    def __init__(self) -> None:
        self._entries: dict[str, _Entry] = {}
        self._last_served: dict[str, float] = {}

    def schedule(self, key: str, due: float, *, urgent: bool = False) -> None:
        self._entries[key] = _Entry(due, urgent)

    def remove(self, key: str) -> None:
        self._entries.pop(key, None)

    def forget(self, key: str) -> None:
        self._entries.pop(key, None)
        self._last_served.pop(key, None)

    def __contains__(self, key: str) -> bool:
        return key in self._entries

    def due_at(self, key: str) -> float | None:
        entry = self._entries.get(key)
        return entry.due if entry else None

    def next_due(self, exclude: Container[str] = ()) -> float | None:
        """The earliest due time, leaving out `exclude` (keys that are due but wait for something else)."""
        return min((e.due for k, e in self._entries.items() if k not in exclude), default=None)

    def ready(self, now: float) -> list[str]:
        """Due keys in serving order: urgent first, then least recently served, then earliest due."""
        due = [(key, entry) for key, entry in self._entries.items() if entry.due <= now]
        due.sort(key=lambda item: (not item[1].urgent, self._last_served.get(item[0], float("-inf")), item[1].due))
        return [key for key, _ in due]

    def ready_batches(
        self, now: float, group_of: Callable[[str], Hashable | None], window: float = 0.0
    ) -> list[list[str]]:
        """Due keys grouped into batches, in serving order of each batch's first key.

        `group_of` returns a key's batch group, or `None` for a key that polls alone. Keys of a group that is due also
        join when they are due within `window` seconds, in due order.
        """
        batches: list[list[str]] = []
        by_group: dict[Hashable, list[str]] = {}
        for key in self.ready(now):
            group = group_of(key)
            if group is None:
                batches.append([key])
            elif group in by_group:
                by_group[group].append(key)
            else:
                by_group[group] = [key]
                batches.append(by_group[group])
        if window > 0 and by_group:
            early = sorted((entry.due, key) for key, entry in self._entries.items() if now < entry.due <= now + window)
            for _, key in early:
                group = group_of(key)
                if group is not None and group in by_group:
                    by_group[group].append(key)
        return batches

    def mark_served(self, key: str, now: float) -> None:
        self._entries.pop(key, None)
        self._last_served[key] = now
