"""The polling engine: subscriptions with leases, shared resources, per-resource event logs, and the loop that polls
the due resources in batches under each account's budget.

The daemon serves the engine over its socket. `--no-daemon` runs the same engine inside the watcher's process.

- **Resources** are shared: two subscriptions to the same target in the same account use one resource, polled for
  the union of the event kinds their filters want. A resource can also be held by a parent resource (an MR holds its
  head pipeline); the parent then receives the child's events in its own log.
- **Event log:** each resource keeps its last 500 events with sequence numbers. A new subscriber starts at the head of
  the log and gets a private `watch_started` with the current summary, never the backlog.
- **Leases:** every watcher poll renews all of that watcher's subscriptions. A subscription not renewed within the
  lease timeout is dropped, so a killed watcher never keeps a resource polled.
- **Batches:** due resources of one account with the same batch key are polled in one call of the source, split so
  that no call costs more than the source's cap. Each call runs only when the account's budget has its cost.
- **Errors:** each kind of error is reported once as `watch_degraded` until a poll succeeds again, and the resource
  backs off exponentially. Not finding the target before its first successful poll ends the watch.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import time
import uuid
from collections import deque
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from cli_watch_kit.budget import Budget
from cli_watch_kit.daemon.cache import ByteBoundedCache
from cli_watch_kit.daemon.scheduler import Scheduler
from cli_watch_kit.events import (
    LIFECYCLE_KINDS,
    WATCH_DEGRADED,
    WATCH_ENDED,
    WATCH_STARTED,
    Event,
    RawEvent,
    utc_now_iso,
)
from cli_watch_kit.source import (
    NotFound,
    Params,
    PollContext,
    PollItem,
    PollOutcome,
    RateLimited,
    Source,
    SourceError,
    Target,
    Transient,
    Wants,
)

log = logging.getLogger(__name__)

EVENT_LOG_SIZE = 500
DEFAULT_LEASE_TIMEOUT = 20.0
DEFAULT_CLIENT_POLL_MS = 5000
MAX_ERROR_BACKOFF = 300.0
MIN_INTERVAL = 0.5
RECENT_BATCHES = 20


class EngineError(Exception):
    pass


def stable_hash(value: Any) -> str:
    """A short hash of a JSON value that does not depend on key order."""
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


@dataclass
class Subscription:
    id: str
    client_id: str
    rid: str
    events: frozenset[str]
    renewed_at: float
    options: Mapping[str, Any] = field(default_factory=dict)
    started: bool = False
    private: list[Event] = field(default_factory=list)
    sent_snapshot_hash: str | None = None


@dataclass
class Resource:
    rid: str
    account: str
    target: Target
    batch_key: Hashable | None
    snapshot: Any = None
    snapshot_json: Any = None
    snapshot_hash: str | None = None
    state: dict[str, Any] = field(default_factory=dict)
    log: deque[Event] = field(default_factory=lambda: deque(maxlen=EVENT_LOG_SIZE))
    seq: int = 0
    events: frozenset[str] = frozenset()
    options: tuple[Mapping[str, Any], ...] = ()
    subscriptions: set[str] = field(default_factory=set)
    parents: dict[str, Wants] = field(default_factory=dict)
    """Parent rid -> what that parent wants from this resource."""
    children: set[str] = field(default_factory=set)
    polled: bool = False
    polling: bool = False
    ended: bool = False
    end_event: Event | None = None
    degraded: str | None = None
    failures: int = 0
    polls: int = 0
    interval: float | None = None
    last_poll_at: float | None = None
    last_error: str | None = None

    @property
    def key(self) -> str:
        return self.target.key

    @property
    def held(self) -> bool:
        return bool(self.subscriptions or self.parents)


@dataclass
class Account:
    """What the engine keeps per account: its budget, the params of its latest subscription, and counters."""

    budget: Budget
    params: Params
    inflight: int = 0
    calls: int = 0
    items: int = 0
    recent_batches: deque[list[str]] = field(default_factory=lambda: deque(maxlen=RECENT_BATCHES))


class Engine:
    def __init__(
        self,
        source: Source[Any],
        *,
        clock: Callable[[], float] = time.time,
        lease_timeout: float = DEFAULT_LEASE_TIMEOUT,
        client_poll_ms: int = DEFAULT_CLIENT_POLL_MS,
    ) -> None:
        self.source = source
        self.clock = clock
        self.lease_timeout = lease_timeout
        self.client_poll_ms = client_poll_ms
        self.started_at = clock()
        self.resources: dict[str, Resource] = {}
        self.subscriptions: dict[str, Subscription] = {}
        self.accounts: dict[str, Account] = {}
        self.scheduler = Scheduler()
        self.cache: ByteBoundedCache[Any] = ByteBoundedCache()
        self._tasks: set[asyncio.Task[None]] = set()
        self._wake: asyncio.Event | None = None
        self._idle_since: float | None = clock()

    # ----------------------------------------------------------------------------------------------------------
    # Watcher-facing operations

    async def subscribe(
        self,
        client_id: str,
        target: Target,
        events: frozenset[str],
        baseline: Any = None,
        params: Params | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if target.kind not in self.source.catalog.kinds_by_resource:
            raise EngineError(f"unknown resource kind {target.kind!r}")
        unknown = set(events) - self.source.catalog.all_kinds
        if unknown:
            raise EngineError(f"unknown event kinds: {', '.join(sorted(unknown))}")
        try:
            checked = self.source.check_options(target, dict(options or {}))
        except ValueError as exc:
            raise EngineError(str(exc)) from exc
        params = dict(params or {})
        account_name = await self.source.account(target, params)
        account = self.account(account_name)
        if params:
            # The latest key of an account wins: a rotated key replaces the old one for every watcher of the account.
            account.params = params
        now = self.clock()
        res = self._acquire(account_name, target)
        if baseline is not None and not res.polled and res.snapshot is None:
            self._seed(res, baseline)
        sub = Subscription(uuid.uuid4().hex, client_id, res.rid, events | frozenset(LIFECYCLE_KINDS), now, checked)
        self.subscriptions[sub.id] = sub
        res.subscriptions.add(sub.id)
        self._idle_since = None
        cursor = res.seq
        if res.polled:
            sub.private.append(self._started_event(res))
            sub.started = True
        if res.end_event is not None:
            sub.private.append(res.end_event)
        self._recompute_wants(res)
        log.info("subscribe %s account=%r client=%s sub=%s", target.key, account_name, client_id[:8], sub.id[:8])
        self._kick()
        return {"subscription_id": sub.id, "cursor": cursor}

    def poll(self, client_id: str, cursors: Mapping[str, int]) -> dict[str, Any]:
        now = self.clock()
        for owned in self.subscriptions.values():
            if owned.client_id == client_id:
                owned.renewed_at = now
        events: dict[str, list[dict[str, Any]]] = {}
        snapshots: dict[str, dict[str, Any]] = {}
        unknown: list[str] = []
        for sid, cursor in cursors.items():
            sub = self.subscriptions.get(sid)
            res = self.resources.get(sub.rid) if sub is not None and sub.client_id == client_id else None
            if sub is None or res is None:
                unknown.append(sid)
                continue
            batch = list(sub.private)
            sub.private.clear()
            batch.extend(e for e in res.log if e.seq > cursor and e.kind in sub.events)
            if batch:
                events[sid] = [e.to_json() for e in batch]
            if res.snapshot_hash is not None and res.snapshot_hash != sub.sent_snapshot_hash:
                snapshots[sid] = {"hash": res.snapshot_hash, "data": res.snapshot_json}
                sub.sent_snapshot_hash = res.snapshot_hash
        return {"events": events, "snapshots": snapshots, "unknown": unknown, "next_poll_ms": self.client_poll_ms}

    def unsubscribe(self, client_id: str, subscription_id: str) -> bool:
        sub = self.subscriptions.get(subscription_id)
        if sub is None or sub.client_id != client_id:
            return False
        self._drop_subscription(sub, "unsubscribed")
        return True

    def idle_for(self, now: float) -> float:
        """Seconds without any subscription, 0 while there is one."""
        if self.subscriptions or self._idle_since is None:
            return 0.0
        return now - self._idle_since

    def reap(self, now: float) -> None:
        expired = [s for s in self.subscriptions.values() if now - s.renewed_at > self.lease_timeout]
        for sub in expired:
            self._drop_subscription(sub, "lease expired")

    def status(self) -> dict[str, Any]:
        now = self.clock()
        return {
            "subscriptions": [
                {
                    "id": s.id,
                    "client_id": s.client_id,
                    "resource": self.resources[s.rid].key if s.rid in self.resources else None,
                    "account": self.resources[s.rid].account if s.rid in self.resources else None,
                    "idle_s": round(now - s.renewed_at, 1),
                    "events": sorted(s.events - frozenset(LIFECYCLE_KINDS)),
                    "options": dict(s.options),
                }
                for s in self.subscriptions.values()
            ],
            "resources": [
                {
                    "resource": r.key,
                    "account": r.account,
                    "batch_key": None if r.batch_key is None else str(r.batch_key),
                    "subscriptions": len(r.subscriptions),
                    "parents": sorted(self.resources[p].key for p in r.parents if p in self.resources),
                    "children": sorted(self.resources[c].key for c in r.children if c in self.resources),
                    "seq": r.seq,
                    "polls": r.polls,
                    "ended": r.ended,
                    "degraded": r.degraded,
                    "last_error": r.last_error,
                    "interval_s": r.interval,
                    "next_poll_in_s": round(d - now, 1) if (d := self.scheduler.due_at(r.rid)) is not None else None,
                }
                for r in self.resources.values()
            ],
            "accounts": {
                name: {
                    "budget": a.budget.describe(now),
                    "calls": a.calls,
                    "items": a.items,
                    "inflight": a.inflight,
                    "stretch": round(self._stretch(name, now), 2),
                    "recent_batches": list(a.recent_batches),
                }
                for name, a in sorted(self.accounts.items())
            },
            "cache": self.cache.describe(),
        }

    # ----------------------------------------------------------------------------------------------------------
    # Helpers for the source's extra ops

    def account(self, name: str) -> Account:
        account = self.accounts.get(name)
        if account is None:
            account = Account(Budget(self.source.dimensions), {})
            self.accounts[name] = account
        return account

    def find(self, account: str, target: Target) -> Resource | None:
        """The resource of `target` in `account`, when some watcher holds it."""
        return self.resources.get(self._rid(account, target))

    async def await_budget(self, account: str, cost: Mapping[str, float], timeout: float = 600.0) -> None:
        """Wait until the account's budget has `cost`, then charge it, so an on-demand fetch stays as gentle as the
        polls. Raises `TimeoutError` after `timeout` seconds."""
        budget = self.account(account).budget
        deadline = time.monotonic() + timeout
        while True:
            now = self.clock()
            if budget.can_spend(cost, now):
                budget.charge(cost, now)
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(f"the budget of account {account!r} had no room within {timeout:.0f} s")
            await asyncio.sleep(min(5.0, max(0.05, budget.wait_time(cost, now))))

    # ----------------------------------------------------------------------------------------------------------
    # Resources

    @staticmethod
    def _rid(account: str, target: Target) -> str:
        return f"{account}|{target.key}" if account else target.key

    def _acquire(self, account: str, target: Target) -> Resource:
        rid = self._rid(account, target)
        res = self.resources.get(rid)
        if res is None:
            res = Resource(rid, account, target, self.source.batch_key(target))
            self.resources[rid] = res
            self.scheduler.schedule(rid, self.clock(), urgent=True)
            log.info("start polling %s", rid)
        return res

    def _release(self, res: Resource) -> None:
        if res.held or self.resources.get(res.rid) is not res:
            return
        del self.resources[res.rid]
        self.scheduler.forget(res.rid)
        log.info("stop polling %s", res.rid)
        for child_rid in list(res.children):
            self._unlink_child(res, child_rid)

    def _unlink_child(self, parent: Resource, child_rid: str) -> None:
        parent.children.discard(child_rid)
        child = self.resources.get(child_rid)
        if child is None:
            return
        child.parents.pop(parent.rid, None)
        self._recompute_wants(child)
        self._release(child)

    def _drop_subscription(self, sub: Subscription, why: str) -> None:
        self.subscriptions.pop(sub.id, None)
        log.info("drop subscription %s on %s: %s", sub.id[:8], sub.rid, why)
        res = self.resources.get(sub.rid)
        if res is not None:
            res.subscriptions.discard(sub.id)
            self._recompute_wants(res)
            self._release(res)
        if not self.subscriptions:
            self._idle_since = self.clock()

    def _recompute_wants(self, res: Resource) -> None:
        """The union of what the subscribers and parents of `res` want. Poll again at once when that grew."""
        events: set[str] = set()
        options: list[Mapping[str, Any]] = []
        wants = [(s.events, s.options) for s in (self.subscriptions[sid] for sid in sorted(res.subscriptions))]
        wants += [(w.events, w.options) for _, w in sorted(res.parents.items())]
        for kinds, opts in wants:
            events |= kinds
            if opts and opts not in options:
                options.append(opts)
        new, new_options = frozenset(events), tuple(options)
        if new == res.events and new_options == res.options:
            return
        grew = not new <= res.events or any(o not in res.options for o in new_options)
        res.events, res.options = new, new_options
        if grew and res.polled and not res.ended and not res.polling:
            self.scheduler.schedule(res.rid, self.clock(), urgent=True)
            self._kick()
        if res.polled:
            self._reconcile_children(res)

    def _reconcile_children(self, res: Resource) -> None:
        wanted: dict[str, tuple[Target, Wants]] = {}
        if not res.ended and res.snapshot is not None:
            for target, kinds in self.source.children(res.target, res.snapshot, self._item(res)).items():
                rid = self._rid(res.account, target)
                if rid != res.rid:
                    wanted[rid] = (target, kinds if isinstance(kinds, Wants) else Wants(frozenset(kinds)))
        for child_rid in list(res.children):
            if child_rid not in wanted:
                self._unlink_child(res, child_rid)
        for rid, (target, kinds) in wanted.items():
            child = self._acquire(res.account, target)
            res.children.add(rid)
            if child.parents.get(res.rid) != kinds:
                child.parents[res.rid] = kinds
                self._recompute_wants(child)

    def _seed(self, res: Resource, baseline: Any) -> None:
        """Start from a snapshot a watcher kept from an earlier daemon, so the first poll reports what changed."""
        try:
            snapshot = self.source.decode(res.target, baseline)
        except (ValueError, TypeError, KeyError) as exc:
            log.info("ignoring the baseline of %s: %s", res.rid, exc)
            return
        self._set_snapshot(res, snapshot)

    def _set_snapshot(self, res: Resource, snapshot: Any) -> None:
        res.snapshot = snapshot
        res.snapshot_json = self.source.encode(snapshot)
        res.snapshot_hash = stable_hash(res.snapshot_json)

    # ----------------------------------------------------------------------------------------------------------
    # Events

    def _started_event(self, res: Resource) -> Event:
        data = {"summary": self.source.summary(res.target, res.snapshot), "snapshot_hash": res.snapshot_hash}
        return Event(WATCH_STARTED, res.key, res.seq, utc_now_iso(), data)

    def _append(self, res: Resource, raw: RawEvent) -> None:
        res.seq += 1
        event = Event(raw.kind, res.key, res.seq, utc_now_iso(), dict(raw.data), raw.artifacts)
        res.log.append(event)
        if raw.kind in LIFECYCLE_KINDS:
            return
        for parent_rid in res.parents:
            parent = self.resources.get(parent_rid)
            if parent is not None and not parent.ended:
                self._append(parent, raw)

    def _end(self, res: Resource, reason: str, data: Mapping[str, Any]) -> None:
        self._append(res, RawEvent(WATCH_ENDED, {"reason": reason, **data}))
        res.ended = True
        res.end_event = res.log[-1]
        self.scheduler.remove(res.rid)
        for child_rid in list(res.children):
            self._unlink_child(res, child_rid)
        log.info("ended %s: %s", res.rid, reason)

    def _degrade(self, res: Resource, reason: str, message: str) -> None:
        res.last_error = message
        if res.degraded == reason:
            return
        res.degraded = reason
        self._append(res, RawEvent(WATCH_DEGRADED, {"reason": reason, "message": message}))
        log.warning("degraded %s: %s: %s", res.rid, reason, message)

    # ----------------------------------------------------------------------------------------------------------
    # Polling

    def _kick(self) -> None:
        if self._wake is not None:
            self._wake.set()

    def _item(self, res: Resource) -> PollItem[Any]:
        return PollItem(res.target, res.snapshot, res.events, res.state, res.options)

    def _interval(self, res: Resource) -> float:
        if res.interval is None:
            res.interval = self.source.interval(res.target, res.snapshot)
        return max(res.interval, MIN_INTERVAL)

    def _stretch(self, account: str, now: float) -> float:
        """The factor all intervals of the account grow by so that the polls the watchers want fit the budget.

        The demand counts one call per batch group at the group's shortest interval, which is what batching makes of
        it once the schedules align.
        """
        groups: dict[Hashable, list[Resource]] = {}
        for res in self.resources.values():
            if res.account == account and not res.ended:
                key = ("solo", res.rid) if res.batch_key is None else ("batch", res.batch_key)
                groups.setdefault(key, []).append(res)
        demand: dict[str, float] = {}
        for members in groups.values():
            interval = min(self._interval(r) for r in members)
            for chunk in self._split(members):
                for dim, amount in self.source.cost([self._item(r) for r in chunk]).items():
                    demand[dim] = demand.get(dim, 0.0) + amount / interval
        return self.account(account).budget.stretch_factor(demand, now)

    def _exceeds(self, cost: Mapping[str, float]) -> bool:
        cap = self.source.batch_cost_cap or {}
        return any(cost.get(dim, 0.0) > limit for dim, limit in cap.items())

    def _split(self, batch: Sequence[Resource]) -> list[list[Resource]]:
        """Split a batch greedily, in order, so that no call costs more than the source's cap."""
        if not self.source.batch_cost_cap or len(batch) <= 1:
            return [list(batch)]
        chunks: list[list[Resource]] = []
        current: list[Resource] = []
        for res in batch:
            candidate = [*current, res]
            if current and self._exceeds(self.source.cost([self._item(r) for r in candidate])):
                chunks.append(current)
                current = [res]
            else:
                current = candidate
        if current:
            chunks.append(current)
        return chunks

    def _group_of(self, rid: str) -> Hashable | None:
        res = self.resources.get(rid)
        if res is None or res.batch_key is None or res.polling or res.ended:
            return None
        return (res.account, res.batch_key)

    async def _poll(self, account_name: str, chunk: list[Resource], items: list[PollItem[Any]]) -> None:
        account = self.account(account_name)
        ctx = PollContext(account_name, account.params, account.budget, self.cache, self.clock)
        outcomes: Mapping[str, PollOutcome[Any]] = {}
        try:
            outcomes = await self.source.poll(items, ctx)
        except SourceError as exc:
            outcomes = {item.key: PollOutcome(error=exc) for item in items}
        except Exception as exc:
            log.exception("poll of %s failed", ", ".join(item.key for item in items))
            error = Transient(f"{type(exc).__name__}: {exc}")
            outcomes = {item.key: PollOutcome(error=error) for item in items}
        finally:
            account.inflight -= 1
        now = self.clock()
        limited = next((o.error for o in outcomes.values() if isinstance(o.error, RateLimited)), None)
        if limited is not None:
            account.budget.on_rate_limited(limited.retry_after, now)
        elif any(o.error is None for o in outcomes.values()):
            account.budget.on_success()
        for res, item in zip(chunk, items, strict=True):
            outcome = outcomes.get(item.key)
            if outcome is None:
                outcome = PollOutcome(error=Transient(f"the source returned no outcome for {item.key}"))
            self._apply(res, outcome, now)
        self._kick()

    def _apply(self, res: Resource, outcome: PollOutcome[Any], now: float) -> None:
        res.polling = False
        res.polls += 1
        res.last_poll_at = now
        if self.resources.get(res.rid) is not res:
            return
        delay: float | None = None
        error = outcome.error
        if error is not None:
            if isinstance(error, NotFound) and not res.polled:
                self._end(res, NotFound.reason, {"message": str(error)})
            else:
                self._degrade(res, error.reason, str(error) or type(error).__name__)
                delay = self._error_delay(res)
                if isinstance(error, RateLimited):
                    delay = max(delay, self.account(res.account).budget.blocked_until - now)
        else:
            res.failures = 0
            res.degraded = None
            res.last_error = None
            first = not res.polled
            baseline = res.snapshot is None
            res.polled = True
            if outcome.snapshot is not None:
                self._set_snapshot(res, outcome.snapshot)
            for raw in outcome.events:
                self._append(res, raw)
            if first:
                for sid in res.subscriptions:
                    sub = self.subscriptions[sid]
                    if not sub.started:
                        sub.private.append(self._started_event(res))
                        sub.started = True
            if outcome.ended is not None:
                data = dict(outcome.end_data)
                if first and baseline:
                    data["already_ended"] = True
                self._end(res, outcome.ended, data)
            else:
                self._reconcile_children(res)
                res.interval = self.source.interval(res.target, res.snapshot)
                base = outcome.next_delay if outcome.next_delay is not None else self._interval(res)
                delay = base * self._stretch(res.account, now)
        if delay is not None and not res.ended:
            self.scheduler.schedule(res.rid, now + delay)

    def _error_delay(self, res: Resource) -> float:
        res.failures += 1
        return float(min(self._interval(res) * (2 ** min(res.failures, 8)), MAX_ERROR_BACKOFF))

    def _start_due(self, now: float) -> float:
        """Start every due batch the budgets allow. Return seconds until the loop should look again."""
        wait = 1.0
        for keys in self.scheduler.ready_batches(now, self._group_of, self.source.coalesce_window):
            batch: list[Resource] = []
            for key in keys:
                res = self.resources.get(key)
                if res is None:
                    self.scheduler.forget(key)
                elif not res.polling and not res.ended:
                    batch.append(res)
            if not batch:
                continue
            account_name = batch[0].account
            account = self.account(account_name)
            for chunk in self._split(batch):
                if account.inflight >= self.source.max_inflight:
                    break
                items = [self._item(r) for r in chunk]
                cost = self.source.cost(items)
                if not account.budget.can_spend(cost, now):
                    wait = min(wait, max(0.05, account.budget.wait_time(cost, now)))
                    continue
                account.budget.charge(cost, now)
                account.inflight += 1
                account.calls += 1
                account.items += len(chunk)
                account.recent_batches.append([r.key for r in chunk])
                for res in chunk:
                    self.scheduler.mark_served(res.rid, now)
                    res.polling = True
                task = asyncio.create_task(self._poll(account_name, chunk, items))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
        next_due = self.scheduler.next_due()
        if next_due is not None:
            wait = min(wait, max(0.05, next_due - now))
        return wait

    async def poll_due_once(self) -> None:
        """Start every due batch and wait for all of them: one deterministic step, for tests and debugging."""
        self.reap(self.clock())
        self._start_due(self.clock())
        while self._tasks:
            await asyncio.gather(*list(self._tasks))

    async def run(self, stop: asyncio.Event) -> None:
        self._wake = asyncio.Event()
        try:
            while not stop.is_set():
                now = self.clock()
                self.reap(now)
                wait = self._start_due(now)
                self._wake.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), wait)
        finally:
            for task in list(self._tasks):
                task.cancel()
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
