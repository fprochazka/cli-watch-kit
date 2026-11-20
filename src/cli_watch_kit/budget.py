"""The rate budget of one account, with one token bucket per dimension (requests, complexity points, ...).

An API usually reports what is left of a limit and when the limit resets. A source passes those readings to
:meth:`Budget.observe`; the budget then paces on `remaining / (reset - now)` and takes `share` of it, leaving the rest
to the user's other tools. Before a reading, or for an API that sends none, each dimension has a fixed rate.

A poll may run only when every dimension has the tokens for its cost. A 429 blocks the whole account for its
`Retry-After`, and repeated 429s back off exponentially. An API that limits each endpoint on its own (Slack limits
every method separately) declares a dimension per endpoint and names it with the 429: then only that dimension is
blocked and backs off, and polls whose cost does not use it go on. When the polls the watchers want would exceed the
rate of any dimension, :meth:`Budget.stretch_factor` says how much every interval of the account must grow.

Time is passed in by the caller (wall-clock seconds, because reset times are usually Unix timestamps), which keeps the
class deterministic under test.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

DEFAULT_SHARE = 0.5
MIN_BACKOFF = 5.0
MAX_BACKOFF = 300.0
MIN_RATE = 0.0001
"""Units per second a dimension keeps even with nothing left, so a wait time stays finite."""


@dataclass(frozen=True, slots=True)
class DimensionSpec:
    """How one dimension is paced before the API reports anything about it.

    `per_minute` is the fixed rate in units per minute, `burst` the bucket size. `share` is the part of a reported
    allowance this tool may use.
    """

    per_minute: float = 60.0
    burst: float = 10.0
    share: float = DEFAULT_SHARE


REQUESTS = "requests"
DEFAULT_DIMENSIONS: Mapping[str, DimensionSpec] = {REQUESTS: DimensionSpec()}


@dataclass
class Dimension:
    name: str
    spec: DimensionSpec
    tokens: float
    last_refill: float | None = None
    remaining: float | None = None
    reset_at: float | None = None
    ceiling_per_second: float | None = None
    """An upper bound on the rate the API states separately, such as GitLab's per-minute limit."""
    spent: float = 0.0
    headers_seen: bool = False
    blocked_until: float = 0.0
    """Until when a 429 that named this dimension blocks it."""
    backoff: float = MIN_BACKOFF
    """The least this dimension is blocked for at its next 429; it doubles with each one in a row."""

    @classmethod
    def from_spec(cls, name: str, spec: DimensionSpec) -> Dimension:
        return cls(name, spec, spec.burst)

    def rate(self, now: float) -> float:
        """Units per second this tool may spend right now."""
        share = self.spec.share
        if self.remaining is not None and self.reset_at is not None and self.reset_at > now:
            rate = self.remaining * share / max(1.0, self.reset_at - now)
            if self.ceiling_per_second:
                rate = min(rate, self.ceiling_per_second * share)
            return max(rate, MIN_RATE)
        if self.ceiling_per_second:
            return self.ceiling_per_second * share
        return max(self.spec.per_minute / 60.0, MIN_RATE)

    def refill(self, now: float) -> None:
        if self.last_refill is None:
            self.last_refill = now
            return
        elapsed = max(0.0, now - self.last_refill)
        self.tokens = min(self.spec.burst, self.tokens + elapsed * self.rate(now))
        self.last_refill = now

    def missing(self, cost: float) -> float:
        """Tokens short of `cost`. A cost above the bucket size needs a full bucket, else it could never run."""
        return max(0.0, min(cost, self.spec.burst) - self.tokens)

    def describe(self, now: float) -> dict[str, Any]:
        return {
            "rate_per_minute": round(self.rate(now) * 60, 2),
            "tokens": round(self.tokens, 2),
            "burst": self.spec.burst,
            "remaining": self.remaining,
            "reset_in_s": round(self.reset_at - now) if self.reset_at else None,
            "spent": round(self.spent, 2),
            "headers_seen": self.headers_seen,
            "blocked_for_s": round(max(0.0, self.blocked_until - now), 1),
        }


@dataclass
class Budget:
    """The budget of one account. Costs and readings for a dimension not declared up front use `DimensionSpec()`."""

    specs: Mapping[str, DimensionSpec] = field(default_factory=lambda: dict(DEFAULT_DIMENSIONS))
    dimensions: dict[str, Dimension] = field(default_factory=dict)
    blocked_until: float = 0.0
    backoff: float = MIN_BACKOFF
    rate_limited_count: int = 0

    def __post_init__(self) -> None:
        for name, spec in self.specs.items():
            self.dimensions.setdefault(name, Dimension.from_spec(name, spec))

    def dimension(self, name: str) -> Dimension:
        dim = self.dimensions.get(name)
        if dim is None:
            dim = Dimension.from_spec(name, self.specs.get(name, DimensionSpec()))
            self.dimensions[name] = dim
        return dim

    def _refill(self, now: float) -> None:
        for dim in self.dimensions.values():
            dim.refill(now)

    def can_spend(self, cost: Mapping[str, float], now: float) -> bool:
        self._refill(now)
        if self.blocked_for(cost, now) > 0:
            return False
        return all(self.dimension(name).missing(amount) <= 0 for name, amount in cost.items())

    def blocked_for(self, dimensions: Iterable[str], now: float) -> float:
        """Seconds a 429 still blocks a poll that spends on `dimensions` (a cost's keys): the account's block, or that
        of one of those dimensions."""
        until = max([self.blocked_until, *(self.dimension(name).blocked_until for name in dimensions)])
        return max(0.0, until - now)

    def wait_time(self, cost: Mapping[str, float], now: float) -> float:
        """Seconds until `can_spend(cost)` could become true: the slowest dimension, or the 429 block."""
        self._refill(now)
        wait = self.blocked_for(cost, now)
        for name, amount in cost.items():
            dim = self.dimension(name)
            wait = max(wait, dim.missing(amount) / dim.rate(now))
        return wait

    def charge(self, cost: Mapping[str, float], now: float) -> None:
        """Take `cost` from the buckets. A negative amount gives back an estimate that was too high."""
        self._refill(now)
        for name, amount in cost.items():
            dim = self.dimension(name)
            dim.tokens = min(dim.spec.burst, dim.tokens - amount)
            dim.spent += amount

    def observe(
        self,
        name: str,
        now: float,
        *,
        remaining: float | None = None,
        reset_at: float | None = None,
        ceiling_per_second: float | None = None,
    ) -> None:
        """Take in what the API reported about one dimension. `reset_at` is in the same clock as `now`."""
        if remaining is None and reset_at is None and ceiling_per_second is None:
            return
        dim = self.dimension(name)
        dim.headers_seen = True
        if remaining is not None:
            dim.remaining = remaining
        if reset_at is not None:
            dim.reset_at = reset_at
        if ceiling_per_second is not None:
            dim.ceiling_per_second = ceiling_per_second

    def on_success(self, dimensions: Iterable[str] = ()) -> None:
        """A poll went through: reset the account's backoff, and that of `dimensions`, the ones the poll spent on."""
        self.backoff = MIN_BACKOFF
        for name in dimensions:
            self.dimension(name).backoff = MIN_BACKOFF

    def check_dimension(self, name: str) -> None:
        """Raise `ValueError` unless `name` is a declared dimension or one a cost already used."""
        if name not in self.specs and name not in self.dimensions:
            known = ", ".join(sorted({*self.specs, *self.dimensions})) or "none"
            raise ValueError(f"a 429 names the unknown budget dimension {name!r} (known: {known})")

    def on_rate_limited(self, retry_after: float | None, now: float, dimension: str | None = None) -> None:
        """Honor a 429's Retry-After, and back off exponentially on repeated 429s.

        Without `dimension` the whole account is blocked and every dimension emptied. With it, only that dimension is
        blocked, emptied and backs off, so polls whose cost does not use it go on. A `dimension` that is neither
        declared nor used by any cost so far raises `ValueError`, because it is almost surely a typo."""
        if dimension is None:
            wait = max(retry_after or 0.0, self.backoff)
            self.blocked_until = max(self.blocked_until, now + wait)
            self.backoff = min(self.backoff * 2, MAX_BACKOFF)
            self.rate_limited_count += 1
            for dim in self.dimensions.values():
                dim.tokens = min(dim.tokens, 0.0)
            return
        self.check_dimension(dimension)
        dim = self.dimension(dimension)
        wait = max(retry_after or 0.0, dim.backoff)
        dim.blocked_until = max(dim.blocked_until, now + wait)
        dim.backoff = min(dim.backoff * 2, MAX_BACKOFF)
        dim.tokens = min(dim.tokens, 0.0)
        self.rate_limited_count += 1

    def stretch_factor(self, demand_per_second: Mapping[str, float], now: float) -> float:
        """How much every poll interval of the account must grow so the demand of each dimension fits its rate."""
        return self.stretch(demand_per_second, now)[0]

    def stretch(self, demand_per_second: Mapping[str, float], now: float) -> tuple[float, str | None]:
        """:meth:`stretch_factor`, and the dimension that drives it (`None` when nothing needs stretching)."""
        factor, driver = 1.0, None
        for name, demand in demand_per_second.items():
            rate = self.dimension(name).rate(now)
            if demand > rate and demand / rate > factor:
                factor, driver = demand / rate, name
        return factor, driver

    def reset_in(self, now: float, dimension: str | None = None) -> float | None:
        """Seconds until the reported window of `dimension` resets (of any dimension, the latest, when `None`), or
        `None` when that is not known."""
        dims = [self.dimension(dimension)] if dimension is not None else list(self.dimensions.values())
        resets = [d.reset_at - now for d in dims if d.reset_at is not None and d.reset_at > now]
        return max(resets) if resets else None

    def windows(self) -> tuple[tuple[str, float | None], ...]:
        """The reported reset times; a change means a new window began."""
        return tuple((name, d.reset_at) for name, d in sorted(self.dimensions.items()))

    def describe(self, now: float) -> dict[str, Any]:
        self._refill(now)
        return {
            "dimensions": {name: dim.describe(now) for name, dim in sorted(self.dimensions.items())},
            "blocked_for_s": round(max(0.0, self.blocked_until - now), 1),
            "rate_limited_count": self.rate_limited_count,
        }
