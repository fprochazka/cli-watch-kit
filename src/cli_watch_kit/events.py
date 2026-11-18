"""The event model, the catalog of event kinds a CLI reports, and the `--events`/`--exclude-events` filter.

Every watch reports the three lifecycle kinds, whatever the filter says: `watch_started` once the first snapshot is
known, `watch_degraded` once per kind of trouble, and `watch_ended` as the last event. The CLI names everything else
in a :class:`KindCatalog`: which kinds each resource kind produces, the groups the filter options accept, and the
threshold kinds that the watcher computes from time and that are reported only when their option is given.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

WATCH_STARTED = "watch_started"
WATCH_DEGRADED = "watch_degraded"
WATCH_ENDED = "watch_ended"
LIFECYCLE_KINDS: tuple[str, ...] = (WATCH_STARTED, WATCH_DEGRADED, WATCH_ENDED)
LIFECYCLE_GROUP = "lifecycle"


class FilterError(ValueError):
    """An `--events`/`--exclude-events` value or a threshold option that cannot work; the message names the option."""


def split_values(values: Iterable[str] | None) -> list[str]:
    """Flatten repeated and comma-separated option values."""
    return [part.strip() for value in values or () for part in value.split(",") if part.strip()]


@dataclass(frozen=True)
class KindCatalog:
    """The event kinds of one CLI.

    - `kinds_by_resource`: for each resource kind (`issue`, `mr`, ...), the kinds a watch of it can report, threshold
      kinds included. The lifecycle kinds are implied.
    - `groups`: names that `--events`/`--exclude-events` expand to several kinds. A group may list kinds of several
      resource kinds; it expands to those the watched resource produces. `lifecycle` is added.
    - `thresholds`: kinds the watcher derives from time (see :mod:`cli_watch_kit.timers`), each with the option that
      switches it on, such as `{"quiet": "--quiet-after"}`. They are never sent to the daemon.
    """

    kinds_by_resource: Mapping[str, tuple[str, ...]]
    groups: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    thresholds: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if LIFECYCLE_GROUP in self.groups:
            raise ValueError(f"the group name {LIFECYCLE_GROUP!r} is reserved")
        unknown = set(self.thresholds) - self.all_kinds
        if unknown:
            raise ValueError(f"threshold kinds that no resource produces: {', '.join(sorted(unknown))}")

    @property
    def all_groups(self) -> Mapping[str, tuple[str, ...]]:
        return {**self.groups, LIFECYCLE_GROUP: LIFECYCLE_KINDS}

    @property
    def all_kinds(self) -> frozenset[str]:
        return frozenset(k for kinds in self.kinds_by_resource.values() for k in kinds) | frozenset(LIFECYCLE_KINDS)

    def produced(self, resource_kind: str) -> tuple[str, ...]:
        """The kinds a watch of `resource_kind` can report, without the lifecycle kinds."""
        try:
            return self.kinds_by_resource[resource_kind]
        except KeyError:
            raise FilterError(f"unknown resource kind {resource_kind!r}") from None

    def daemon_kinds(self, kinds: Iterable[str]) -> frozenset[str]:
        """`kinds` without the threshold kinds, which only the watcher computes."""
        return frozenset(kinds) - frozenset(self.thresholds)

    def groups_for(self, resource_kind: str) -> list[str]:
        """The groups that mean something for a watch of `resource_kind`, lifecycle left out."""
        produced = set(self.produced(resource_kind))
        return [name for name, kinds in self.groups.items() if set(kinds) & produced]

    def _expand(self, resource_kind: str, values: Iterable[str], option: str, enabled: frozenset[str]) -> set[str]:
        produced = set(self.produced(resource_kind)) | set(LIFECYCLE_KINDS)
        switched_off = set(self.thresholds) - enabled
        groups = self.all_groups
        expanded: set[str] = set()
        for name in values:
            if name in groups:
                # A group brings a threshold kind only when its option is given: `--events=status` stays usable
                # without the threshold option.
                members = set(groups[name]) & produced - switched_off
                if not members:
                    raise FilterError(f"{option}: the group {name!r} has no events for a {resource_kind} watch")
                expanded |= members
            elif name in self.all_kinds:
                if name not in produced:
                    raise FilterError(f"{option}: a {resource_kind} watch never produces {name!r}")
                if name in switched_off and option == "--events":
                    raise FilterError(f"{option}: {name!r} is reported only with {self.thresholds[name]}")
                expanded.add(name)
            else:
                known = ", ".join(sorted(groups))
                raise FilterError(f"{option}: unknown event kind or group {name!r} (groups: {known})")
        return expanded

    def resolve(
        self,
        resource_kind: str,
        include: Iterable[str] | None,
        exclude: Iterable[str] | None,
        enabled: Iterable[str] = (),
    ) -> frozenset[str]:
        """The event kinds a watch reports. Lifecycle events are always in, and cannot be excluded.

        `enabled` names the threshold kinds whose option was given. Without its option a threshold kind is never
        reported, and with it the kind must survive the filter, so an option never silently does nothing.
        """
        enabled_set = frozenset(enabled)
        produced = self.produced(resource_kind)
        include_list = split_values(include)
        exclude_list = split_values(exclude)
        if include_list:
            selected = self._expand(resource_kind, include_list, "--events", enabled_set)
        else:
            selected = set(produced) - (set(self.thresholds) - enabled_set)
        if exclude_list:
            excluded = self._expand(resource_kind, exclude_list, "--exclude-events", enabled_set)
            if excluded & set(LIFECYCLE_KINDS):
                raise FilterError("--exclude-events: lifecycle events are always reported and cannot be excluded")
            selected -= excluded
        for kind in sorted(enabled_set):
            option = self.thresholds.get(kind, kind)
            if kind not in produced:
                raise FilterError(f"{option}: a {resource_kind} watch never produces {kind!r}")
            if kind not in selected:
                raise FilterError(f"{option}: {kind!r} is filtered out by --events/--exclude-events")
        return frozenset(selected | set(LIFECYCLE_KINDS))


def utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass(frozen=True, slots=True)
class RawEvent:
    """An event as a source produces it, before the engine assigns its sequence number."""

    kind: str
    data: dict[str, Any] = field(default_factory=dict)
    artifacts: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Event:
    """One reported change. `resource` is the watched resource's key, `seq` its number in the stream."""

    kind: str
    resource: str
    seq: int
    observed_at: str
    data: dict[str, Any]
    artifacts: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "resource": self.resource,
            "seq": self.seq,
            "observed_at": self.observed_at,
            "data": self.data,
        }
        if self.artifacts:
            payload["artifacts"] = list(self.artifacts)
        return payload

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> Event:
        return cls(
            kind=str(data["kind"]),
            resource=str(data["resource"]),
            seq=int(data["seq"]),
            observed_at=str(data["observed_at"]),
            data=dict(data.get("data") or {}),
            artifacts=tuple(str(a) for a in data.get("artifacts") or ()),
        )
