"""A fake source over an in-memory world of items and boxes, which a test (or a JSON spec file, for the daemon running
in its own process) changes between polls.

- An item has a `status` and a count of `notes`. A status change reports `status_changed`, more notes `note_added`.
  The status `done` ends the watch.
- A box lists item ids and relays the events of its items, which it polls as children.
- `stale` is a threshold kind the watcher computes; the source never produces it.
"""

from __future__ import annotations

import json
import os
from collections.abc import Hashable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from cli_watch_kit.budget import DimensionSpec
from cli_watch_kit.events import RawEvent
from cli_watch_kit.source import (
    NotFound,
    OpContext,
    OpHandler,
    Params,
    PollContext,
    PollItem,
    PollOutcome,
    RateLimited,
    Source,
    Target,
    Transient,
    Unauthorized,
    Wants,
)
from fakeapp.catalog import CATALOG

SPEC_ENV = "FAKE_SOURCE_SPEC"
LOG_ENV = "FAKE_SOURCE_LOG"

type Snapshot = dict[str, Any]


@dataclass
class World:
    """What the fake API returns. `readings` are rate-limit readings per dimension, as `observe` takes them."""

    items: dict[str, dict[str, Any]] = field(default_factory=dict)
    boxes: dict[str, list[str]] = field(default_factory=dict)
    rate_limited: float | None = None
    failing: str | None = None
    readings: dict[str, dict[str, float]] = field(default_factory=dict)
    actual_cost: dict[str, float] | None = None

    def load(self) -> World:
        return self

    def to_json(self) -> dict[str, Any]:
        return {
            "items": self.items,
            "boxes": self.boxes,
            "rate_limited": self.rate_limited,
            "failing": self.failing,
            "readings": self.readings,
        }


@dataclass
class FileWorld(World):
    """The world as a JSON file, read on every poll, so a test can change it under a daemon in another process."""

    path: Path = Path()

    def load(self) -> World:
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        return World(
            items=raw.get("items", {}),
            boxes=raw.get("boxes", {}),
            rate_limited=raw.get("rate_limited"),
            failing=raw.get("failing"),
            readings=raw.get("readings", {}),
        )


def write_world(path: Path, world: World) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(world.to_json()), encoding="utf-8")
    os.replace(tmp, path)


@dataclass
class Call:
    keys: list[str]
    account: str
    params: dict[str, Any]
    options: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


async def _echo(ctx: OpContext, request: dict[str, Any]) -> dict[str, Any]:
    """An extra op: what the daemon knows about a target, for the tests of app ops."""
    target = Target.from_wire(request.get("target"))
    account = await ctx.account(target)
    await ctx.engine.await_budget(account, {"requests": 1})
    res = ctx.engine.find(account, target)
    return {"account": account, "snapshot": res.snapshot_json if res is not None else None}


class FakeSource(Source[Snapshot]):
    catalog = CATALOG
    dimensions = {
        "requests": DimensionSpec(per_minute=600, burst=10),
        "complexity": DimensionSpec(per_minute=6000, burst=100),
    }

    def __init__(
        self,
        world: World | None = None,
        *,
        batching: bool = False,
        complexity_per_item: float = 10.0,
        cap: Mapping[str, float] | None = None,
        coalesce_window: float = 5.0,
        default_interval: float = 10.0,
        log_path: Path | None = None,
    ) -> None:
        self.world = world or World()
        self.batching = batching
        self.complexity_per_item = complexity_per_item
        self.batch_cost_cap = cap
        self.coalesce_window = coalesce_window
        self.default_interval = default_interval
        self.calls: list[Call] = []
        self.log_path = log_path
        self.ops: Mapping[str, OpHandler] = {"echo": _echo}

    async def account(self, target: Target, params: Params) -> str:
        if params.get("token") == "bad":
            raise Unauthorized("the token was refused")
        return str(params.get("user", ""))

    def batch_key(self, target: Target) -> Hashable | None:
        if not self.batching:
            return None
        return str(target.data.get("batch", target.kind))

    def interval(self, target: Target, snapshot: Snapshot | None) -> float:
        return float(target.data.get("interval", self.default_interval))

    def cost(self, batch: Sequence[PollItem[Snapshot]]) -> Mapping[str, float]:
        return {"requests": 1.0, "complexity": self.complexity_per_item * len(batch)}

    async def poll(self, batch: Sequence[PollItem[Snapshot]], ctx: PollContext) -> Mapping[str, PollOutcome[Snapshot]]:
        options = {item.key: [dict(o) for o in item.options] for item in batch}
        call = Call([item.key for item in batch], ctx.account, dict(ctx.params), options)
        self.calls.append(call)
        if self.log_path is not None:
            with open(self.log_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"keys": call.keys, "account": call.account}) + "\n")
        world = self.world.load()
        now = ctx.clock()
        for dim, reading in world.readings.items():
            ctx.budget.observe(dim, now, remaining=reading.get("remaining"), reset_at=reading.get("reset_at"))
        if self.world.actual_cost is not None:
            ctx.spend(self.world.actual_cost)
        if world.rate_limited is not None:
            raise RateLimited("slow down", world.rate_limited)
        if world.failing is not None:
            raise Transient(world.failing)
        return {item.key: self._poll_one(world, item) for item in batch}

    def _poll_one(self, world: World, item: PollItem[Snapshot]) -> PollOutcome[Snapshot]:
        target = item.target
        item.state["polls"] = item.state.get("polls", 0) + 1
        if target.kind == "box":
            if target.id not in world.boxes:
                return PollOutcome(error=NotFound(f"no box {target.id}"))
            box: Snapshot = {"items": list(world.boxes[target.id])}
            events = []
            if item.snapshot is not None and item.snapshot != box and "box_changed" in item.events:
                events.append(RawEvent("box_changed", {"items": box["items"]}))
            return PollOutcome(box, events)
        state = world.items.get(target.id)
        if state is None:
            return PollOutcome(error=NotFound(f"no item {target.id}"))
        snapshot: Snapshot = {"status": state.get("status", "open"), "notes": int(state.get("notes", 0))}
        raw: list[RawEvent] = []
        previous = item.snapshot
        if previous is not None:
            if previous["status"] != snapshot["status"] and "status_changed" in item.events:
                raw.append(
                    RawEvent("status_changed", {"id": target.id, "from": previous["status"], "to": snapshot["status"]})
                )
            if snapshot["notes"] > previous["notes"] and "note_added" in item.events:
                raw.append(RawEvent("note_added", {"id": target.id, "notes": snapshot["notes"]}))
        if snapshot["status"] == "done":
            return PollOutcome(snapshot, raw, ended="done", end_data={"status": "done"})
        return PollOutcome(snapshot, raw)

    def summary(self, target: Target, snapshot: Snapshot | None) -> dict[str, Any]:
        return dict(snapshot or {})

    def decode(self, target: Target, data: Any) -> Snapshot:
        if not isinstance(data, dict) or ("status" not in data and "items" not in data):
            raise ValueError("not a snapshot of the fake source")
        return dict(data)

    def check_options(self, target: Target, options: Mapping[str, Any]) -> Mapping[str, Any]:
        if set(options) - {"detail"} or options.get("detail", "full") not in ("full", "brief"):
            raise ValueError(f"unsupported options {dict(options)}")
        return dict(options)

    def children(
        self, target: Target, snapshot: Snapshot, item: PollItem[Snapshot]
    ) -> Mapping[Target, frozenset[str] | Wants]:
        if target.kind != "box":
            return {}
        wanted = Wants(item.events & {"status_changed", "note_added"}, {"detail": "brief"})
        return {Target("item", i): wanted for i in snapshot.get("items", [])}


def source_from_env() -> FakeSource:
    """The source of the daemon process: the world in the spec file, the calls logged to the log file."""
    spec = os.environ[SPEC_ENV]
    log = os.environ.get(LOG_ENV)
    return FakeSource(
        FileWorld(path=Path(spec)),
        batching=os.environ.get("FAKE_SOURCE_BATCHING") == "1",
        default_interval=float(os.environ.get("FAKE_SOURCE_INTERVAL", "10")),
        log_path=Path(log) if log else None,
    )
