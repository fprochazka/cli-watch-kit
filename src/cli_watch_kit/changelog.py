"""A watcher's changelog in its resource dir: `events.jsonl` (append-only) and `baseline.json` (its last snapshot).

The baseline lets a watcher resubscribe after a daemon restart without a fake "something changed": the new daemon
diffs against it, and the watcher drops the new `watch_started` when the snapshot hash did not move. It is what this
watcher saw, which is why it should not be a state file other writers move forward: a watcher that resubscribed from
such a file would skip the events in between.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cli_watch_kit.atomic import atomic_write_json, atomic_write_text
from cli_watch_kit.events import Event, utc_now_iso


@dataclass(frozen=True)
class StoredState:
    snapshot_hash: str | None
    snapshot: Any


class Changelog:
    """The changelog of the resource `resource` in `directory`. `readme`, when given, is written next to it once."""

    def __init__(self, directory: Path, resource: str, readme: str | None = None) -> None:
        self.dir = directory
        self.resource = resource
        self.readme = readme
        self.events_path = directory / "events.jsonl"
        self.baseline_path = directory / "baseline.json"

    def _ensure_dir(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        if self.readme is not None:
            readme = self.dir / "README.txt"
            if not readme.exists():
                atomic_write_text(readme, self.readme)

    def append(self, events: Iterable[Event]) -> None:
        lines = [json.dumps(e.to_json(), ensure_ascii=False, separators=(",", ":")) + "\n" for e in events]
        if not lines:
            return
        self._ensure_dir()
        with open(self.events_path, "a", encoding="utf-8") as fh:
            fh.writelines(lines)

    def last_seq(self) -> int:
        """The `seq` of the last event in events.jsonl, 0 when there is none."""
        try:
            with open(self.events_path, "rb") as fh:
                fh.seek(0, os.SEEK_END)
                size = fh.tell()
                fh.seek(max(0, size - 256 * 1024))
                tail = fh.read()
        except OSError:
            return 0
        for line in reversed(tail.splitlines()):
            try:
                return int(json.loads(line)["seq"])
            except (ValueError, KeyError, TypeError):
                continue
        return 0

    def load_baseline(self) -> StoredState | None:
        try:
            raw = json.loads(self.baseline_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(raw, dict) or raw.get("resource") != self.resource:
            return None
        return StoredState(raw.get("snapshot_hash"), raw.get("snapshot"))

    def save_baseline(self, snapshot_hash: str, snapshot: Any) -> None:
        self._ensure_dir()
        payload = {
            "resource": self.resource,
            "snapshot_hash": snapshot_hash,
            "snapshot": snapshot,
            "written_at": utc_now_iso(),
        }
        atomic_write_json(self.baseline_path, payload)
