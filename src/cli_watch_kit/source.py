"""What a CLI implements to be watched: a :class:`Source` that polls targets and diffs them into events.

The engine owns the scheduling, the rate budget, the sharing between watchers and the event log. The source owns the
API: it fetches the current state of a batch of targets, diffs each against the previous snapshot with a pure
function, and returns one :class:`PollOutcome` per target. It never sleeps and never retries; the engine does both.

- A **target** is what a watcher asks for (`issue ABC-123`). Its `key` names it in events and logs.
- An **account** is whose rate limit a poll spends. Targets of different accounts never share a poll or a budget,
  so two watchers with different API keys of the same user share one budget, and two users never share one.
- Targets with the same **batch key** that are due at about the same time go into one :meth:`Source.poll` call, split
  so that no call costs more than :attr:`Source.batch_cost_cap`.
- **Params** are what a watcher sends along with its subscription, such as an API key. They reach the daemon over its
  private socket and are given to the source per account, never logged or shown in the status.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Hashable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cli_watch_kit.budget import DEFAULT_DIMENSIONS, REQUESTS, Budget, DimensionSpec
from cli_watch_kit.daemon.cache import ByteBoundedCache
from cli_watch_kit.events import KindCatalog, RawEvent

if TYPE_CHECKING:
    from cli_watch_kit.daemon.engine import Engine

Params = Mapping[str, Any]


class SourceError(Exception):
    """A poll of a target failed. Raise a subclass from :meth:`Source.poll` for the whole batch, or put one in a
    :class:`PollOutcome` for one target. The engine reports each kind once as `watch_degraded` and backs off."""

    reason = "error"


class NotFound(SourceError):
    """The target does not exist (any more). Before the first successful poll this ends the watch."""

    reason = "not_found"


class Unauthorized(SourceError):
    reason = "auth"


class RateLimited(SourceError):
    """The API refused the request for its rate limit. Blocks the whole account for `retry_after` seconds or more."""

    reason = "rate_limited"

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class Transient(SourceError):
    """A network error, a 5xx, an unexpected response: worth trying again later."""

    reason = "error"


@dataclass(frozen=True)
class Target:
    """A watched resource: its `kind` (a key of the catalog's `kinds_by_resource`), an `id` unique within the kind,
    and whatever else the source needs to poll it, as JSON in `data`. Two targets are equal when their keys are."""

    kind: str
    id: str
    data: Mapping[str, Any] = field(default_factory=dict, compare=False, hash=False)

    @property
    def key(self) -> str:
        return f"{self.kind} {self.id}"

    def to_wire(self) -> dict[str, Any]:
        return {"kind": self.kind, "id": self.id, "data": dict(self.data)}

    @classmethod
    def from_wire(cls, value: Any) -> Target:
        if not isinstance(value, Mapping):
            raise ValueError("target must be an object")
        kind, ident, data = value.get("kind"), value.get("id"), value.get("data") or {}
        if not isinstance(kind, str) or not kind or not isinstance(ident, str) or not ident:
            raise ValueError("target.kind and target.id must be non-empty strings")
        if not isinstance(data, Mapping):
            raise ValueError("target.data must be an object")
        return cls(kind, ident, dict(data))


@dataclass(frozen=True)
class PollItem[S]:
    """One target in a poll: the snapshot to diff against (`None` on the first poll without a baseline) and the event
    kinds any subscriber wants, which a source may use to skip work nobody needs.

    `state` is the resource's private scratch space for what is not part of the snapshot (an ETag, work held back
    for a later poll, a stateful poller object). It is the same dict on every poll of the resource, starts empty
    (also after a daemon restart), and goes away with the resource.
    """

    target: Target
    snapshot: S | None
    events: frozenset[str]
    state: dict[str, Any] = field(default_factory=dict, compare=False)
    options: tuple[Mapping[str, Any], ...] = ()
    """The source options of every subscriber and parent that sent any, each once, for the source to merge (glab's
    `--logs` modes, for example). Empty when nobody sent options."""

    @property
    def key(self) -> str:
        return self.target.key


@dataclass(frozen=True)
class Wants:
    """What a parent wants from a child target: event kinds, and source options like a subscriber's."""

    events: frozenset[str]
    options: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PollOutcome[S]:
    """What a poll found for one target.

    - On success, `snapshot` is the new state and `events` what changed since the previous one. A poll without a
      previous snapshot is the baseline and should report nothing.
    - `ended` ends the watch with that reason (`completed`, `merged`, ...), `end_data` goes into `watch_ended`.
    - `next_delay` overrides :meth:`Source.interval` for this target once.
    - With `error`, the rest is ignored.
    """

    snapshot: S | None = None
    events: Sequence[RawEvent] = ()
    ended: str | None = None
    end_data: Mapping[str, Any] = field(default_factory=dict)
    next_delay: float | None = None
    error: SourceError | None = None


@dataclass
class PollContext:
    """What a poll gets from the engine besides its targets.

    `budget` is the account's: call :meth:`Budget.observe` with the rate-limit readings of each response.

    The engine charges the account :meth:`Source.cost` before the call. A source that knows what each response really
    cost (an ETag 304 at a tenth of a request, a complexity header) calls :meth:`spend` with it, as often as it sends
    requests; the engine then gives the estimate back after the call, so the account pays what was spent. `cache` is
    shared by the whole daemon; put the version of the value into its key, so a hit is never stale.
    """

    account: str
    params: Params
    budget: Budget
    cache: ByteBoundedCache[Any]
    clock: Callable[[], float] = time.time
    estimate: Mapping[str, float] = field(default_factory=dict)
    """What the engine charged before the call."""
    spent: dict[str, float] = field(default_factory=dict)
    """What :meth:`spend` charged so far."""

    def spend(self, cost: Mapping[str, float]) -> None:
        """Charge what a request really cost. Once a poll spent anything, the estimate is refunded after it."""
        self.budget.charge(cost, self.clock())
        for dim, amount in cost.items():
            self.spent[dim] = self.spent.get(dim, 0.0) + amount

    @staticmethod
    def current() -> PollContext | None:
        """The context of the poll the calling task runs in, or `None` outside a poll (in an extra op, say).

        For an API client the source shares between the polls of an account, which can then charge each request to
        the poll that sent it without the context being passed down to it. Concurrent polls each see their own."""
        return _CURRENT_POLL.get()


_CURRENT_POLL: ContextVar[PollContext | None] = ContextVar("cli_watch_kit_current_poll", default=None)


@dataclass
class OpContext:
    """What an extra op of the daemon gets: the engine and the params the request carried."""

    engine: Engine
    params: Params

    async def account(self, target: Target) -> str:
        return await self.engine.source.account(target, self.params)


OpHandler = Callable[[OpContext, dict[str, Any]], Awaitable[dict[str, Any]]]
"""An extra daemon op: gets the request (without `op` and `params`) and returns the fields of the `ok` response."""


class Source[S](ABC):
    """The base class of a CLI's source. `S` is the source's snapshot type, which never leaves the daemon as is:
    :meth:`encode` turns it into JSON for the watchers and their baselines, :meth:`decode` reads it back."""

    catalog: KindCatalog
    dimensions: Mapping[str, DimensionSpec] = DEFAULT_DIMENSIONS
    """The budget dimensions, with their rates before the API reports any."""
    batch_cost_cap: Mapping[str, float] | None = None
    """The most one poll call may cost per dimension. Batches above it are split; one target above it polls alone."""
    coalesce_window: float = 5.0
    """A target due within this many seconds joins a batch that polls now, so close schedules merge."""
    max_inflight: int = 4
    """Concurrent poll calls per account."""
    ops: Mapping[str, OpHandler] = {}
    """Extra daemon ops, by name. They must not shadow the built-in ones."""

    async def account(self, target: Target, params: Params) -> str:
        """Whose budget polls of `target` spend. Called on subscribe; raise :class:`Unauthorized` for bad params.

        The default is one account for everything."""
        return ""

    def batch_key(self, target: Target) -> Hashable | None:
        """Targets with an equal key (within one account) may be polled in one call. `None` polls alone."""
        return None

    @abstractmethod
    def interval(self, target: Target, snapshot: S | None) -> float:
        """Base seconds until the next poll, before the engine stretches it under budget pressure."""

    def cost(self, batch: Sequence[PollItem[S]]) -> Mapping[str, float]:
        """The estimated cost of one poll call for `batch`, per dimension. Default: one request."""
        return {REQUESTS: 1.0}

    @abstractmethod
    async def poll(self, batch: Sequence[PollItem[S]], ctx: PollContext) -> Mapping[str, PollOutcome[S]]:
        """Poll every target of `batch` and return an outcome per target key. A target left out counts as a
        transient error. Raise a :class:`SourceError` when the whole call failed."""

    def summary(self, target: Target, snapshot: S | None) -> dict[str, Any]:
        """A compact description of the snapshot, sent in `watch_started`."""
        return {}

    def encode(self, snapshot: S) -> Any:
        """The snapshot as JSON. Default: the snapshot is already JSON."""
        return snapshot

    def decode(self, target: Target, data: Any) -> S:
        """A snapshot from :meth:`encode` (a baseline a watcher kept). Raise `ValueError` when it does not fit."""
        return data  # type: ignore[no-any-return]

    def status(self) -> dict[str, Any]:
        """What `daemon status` shows about the source, such as request counts per host. Never put params here."""
        return {}

    def check_options(self, target: Target, options: Mapping[str, Any]) -> Mapping[str, Any]:
        """Validate the source options a watcher subscribes with, and return them normalized. Raise `ValueError`
        with a message for the watcher when they cannot work. Default: any options are taken as they are."""
        return dict(options)

    def children(self, target: Target, snapshot: S, item: PollItem[S]) -> Mapping[Target, frozenset[str] | Wants]:
        """Other targets whose events this one relays, each with the kinds (and options) it wants from them, such as
        an MR that relays its head pipeline. `item` says what the subscribers of `target` want. A child is polled
        as a target of its own, in the parent's account, and its events other than the lifecycle ones go into the
        parent's log too."""
        return {}
