"""The event kinds of the fake app."""

from __future__ import annotations

from cli_watch_kit.events import KindCatalog

CATALOG = KindCatalog(
    kinds_by_resource={
        "item": ("status_changed", "note_added", "stale"),
        "box": ("box_changed", "status_changed", "note_added"),
    },
    groups={"status": ("status_changed", "box_changed", "stale"), "notes": ("note_added",)},
    thresholds={"stale": "--stale-after"},
)
