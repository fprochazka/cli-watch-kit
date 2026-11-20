"""Batch coalescing, splitting by the cost cap, and pacing on two budget dimensions, all on a fake clock."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence

import pytest

from cli_watch_kit.budget import DimensionSpec
from cli_watch_kit.daemon.engine import Engine
from cli_watch_kit.source import PollContext, PollItem, PollOutcome, RateLimited, Target
from conftest import Clock
from fakeapp.source import Call, FakeSource, Snapshot, World

KINDS = frozenset({"status_changed", "note_added"})


def item(ident: str, interval: float = 10.0, batch: str | None = None) -> Target:
    data: dict[str, object] = {"interval": interval}
    if batch is not None:
        data["batch"] = batch
    return Target("item", ident, data)


def world_of(*ids: str) -> World:
    return World(items={i: {"status": "open"} for i in ids})


def test_targets_due_together_poll_in_one_call(clock: Clock) -> None:
    source = FakeSource(world_of("a", "b", "c"), batching=True)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        for ident in "abc":
            await engine.subscribe("client", item(ident), KINDS)
        await engine.poll_due_once()
        assert [sorted(call.keys) for call in source.calls] == [["item a", "item b", "item c"]]
        clock.advance(10)
        await engine.poll_due_once()
        assert len(source.calls) == 2

    asyncio.run(scenario())


def test_different_batch_keys_and_accounts_never_share_a_call(clock: Clock) -> None:
    source = FakeSource(world_of("a", "b", "c"), batching=True)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", item("a", batch="one"), KINDS)
        await engine.subscribe("client", item("b", batch="two"), KINDS)
        await engine.subscribe("client", item("c", batch="one"), KINDS, params={"user": "someone-else"})
        await engine.poll_due_once()
        assert sorted((call.account, tuple(call.keys)) for call in source.calls) == [
            ("", ("item a",)),
            ("", ("item b",)),
            ("someone-else", ("item c",)),
        ]

    asyncio.run(scenario())


def test_a_target_due_within_the_window_joins_the_batch_and_the_schedules_stay_aligned(clock: Clock) -> None:
    source = FakeSource(world_of("a", "b"), batching=True, coalesce_window=5)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client-a", item("a", interval=10), KINDS)
        await engine.poll_due_once()
        clock.advance(7)
        # b is new and due now; a is due in 3 s, within the window, so it comes along.
        await engine.subscribe("client-b", item("b", interval=30), KINDS)
        await engine.poll_due_once()
        assert source.calls[-1].keys == ["item b", "item a"]
        for _ in range(3):
            clock.advance(10)
            engine.poll("client-a", {})
            engine.poll("client-b", {})
            await engine.poll_due_once()
        # a alone twice, then both together again at the 30 s mark of their common start.
        assert [sorted(call.keys) for call in source.calls[2:]] == [["item a"], ["item a"], ["item a", "item b"]]

    asyncio.run(scenario())


def test_a_batch_above_the_cost_cap_is_split_in_order(clock: Clock) -> None:
    source = FakeSource(world_of(*"abcde"), batching=True, cap={"complexity": 30})
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        for ident in "abcde":
            await engine.subscribe("client", item(ident), KINDS)
        await engine.poll_due_once()
        assert [len(call.keys) for call in source.calls] == [3, 2]
        assert sorted(k for call in source.calls for k in call.keys) == [f"item {i}" for i in "abcde"]

    asyncio.run(scenario())


def test_a_target_above_the_cap_on_its_own_polls_alone(clock: Clock) -> None:
    source = FakeSource(world_of("a", "b"), batching=True, complexity_per_item=50, cap={"complexity": 30})
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", item("a"), KINDS)
        await engine.subscribe("client", item("b"), KINDS)
        await engine.poll_due_once()
        assert sorted(call.keys for call in source.calls) == [["item a"], ["item b"]]

    asyncio.run(scenario())


def test_a_scarce_second_dimension_stretches_every_interval(clock: Clock) -> None:
    now = clock()
    # 3600 complexity points left for an hour: half of it is 0.5 points/s for this tool.
    world = world_of("a", "b")
    world.readings = {"complexity": {"remaining": 3600, "reset_at": now + 3600}}
    source = FakeSource(world, batching=True)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", item("a"), KINDS)
        await engine.subscribe("client", item("b"), KINDS)
        await engine.poll_due_once()
        # One call of 20 points every 10 s is 2 points/s, four times the 0.5/s; requests are far below their rate.
        account = engine.status()["accounts"][""]
        assert account["stretch"] == pytest.approx(4.0, rel=0.01)
        assert engine.scheduler.due_at("item a") == pytest.approx(now + 40, rel=1e-6)

    asyncio.run(scenario())


def test_a_poll_waits_until_the_scarcer_dimension_has_its_cost(clock: Clock) -> None:
    now = clock()
    world = world_of("a")
    world.readings = {"complexity": {"remaining": 3600, "reset_at": now + 3600}}
    # The response says the call cost 120 points, not the 10 estimated: the bucket goes 20 into debt.
    world.actual_cost = {"requests": 1, "complexity": 120}
    source = FakeSource(world)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", item("a", interval=1), KINDS)
        await engine.poll_due_once()
        complexity = engine.accounts[""].budget.dimension("complexity")
        assert complexity.tokens == pytest.approx(-20)
        assert complexity.spent == pytest.approx(120)
        # The estimate was given back: the account paid what the poll spent, not both.
        assert engine.accounts[""].budget.dimension("requests").spent == pytest.approx(1)
        clock.advance(20)
        engine.poll("client", {})
        await engine.poll_due_once()
        assert len(source.calls) == 1
        clock.advance(42)
        engine.poll("client", {})
        await engine.poll_due_once()
        assert len(source.calls) == 2

    asyncio.run(scenario())


def test_a_rate_limit_blocks_the_whole_account(clock: Clock) -> None:
    world = world_of("a", "b")
    world.rate_limited = 60.0
    source = FakeSource(world)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", item("a"), KINDS)
        await engine.poll_due_once()
        assert len(source.calls) == 1
        world.rate_limited = None
        await engine.subscribe("client", item("b"), KINDS)
        clock.advance(30)
        engine.poll("client", {})
        await engine.poll_due_once()
        assert len(source.calls) == 1
        clock.advance(31)
        engine.poll("client", {})
        await engine.poll_due_once()
        assert sorted(k for call in source.calls[1:] for k in call.keys) == ["item a", "item b"]

    asyncio.run(scenario())


class PerMethodSource(FakeSource):
    """An API that limits each of its methods on its own, like Slack: a dimension per method, which an item names in
    `method`. `limited` maps an item id to the Retry-After of the 429 its next poll gets, `dimension` to the dimension
    that 429 names (default: the item's method)."""

    dimensions = {"history": DimensionSpec(per_minute=600), "replies": DimensionSpec(per_minute=600)}

    def __init__(self, world: World) -> None:
        super().__init__(world)
        self.limited: dict[str, float] = {}
        self.dimension: str | None = None
        self.side_request: tuple[float, str] | None = None

    def cost(self, batch: Sequence[PollItem[Snapshot]]) -> Mapping[str, float]:
        return {str(batch[0].target.data["method"]): 1.0}

    async def poll(self, batch: Sequence[PollItem[Snapshot]], ctx: PollContext) -> Mapping[str, PollOutcome[Snapshot]]:
        target = batch[0].target
        if target.id in self.limited:
            self.calls.append(Call([target.key], ctx.account, dict(ctx.params)))
            method = self.dimension or str(target.data["method"])
            raise RateLimited("slow down", self.limited.pop(target.id), dimension=method)
        if self.side_request is not None:
            ctx.rate_limited(*self.side_request)
        return await super().poll(batch, ctx)


def method_item(ident: str, method: str) -> Target:
    return Target("item", ident, {"interval": 10.0, "method": method})


def polled(source: FakeSource, since: int = 0) -> list[str]:
    return sorted(k for call in source.calls[since:] for k in call.keys)


def test_a_rate_limit_of_one_dimension_blocks_only_the_targets_that_spend_on_it(clock: Clock) -> None:
    source = PerMethodSource(world_of("a", "b", "c"))
    source.limited = {"b": 60.0}
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", method_item("a", "history"), KINDS)
        await engine.subscribe("client", method_item("b", "replies"), KINDS)
        await engine.subscribe("client", method_item("c", "replies"), KINDS)
        await engine.poll_due_once()
        assert polled(source) == ["item a", "item b", "item c"]
        budget = engine.accounts[""].budget
        assert budget.blocked_until == 0.0
        assert budget.dimension("replies").blocked_until == pytest.approx(clock() + 60)
        clock.advance(10)
        engine.poll("client", {})
        await engine.poll_due_once()
        # history goes on; c spends on replies, so it waits for the block although it is due.
        assert polled(source, 3) == ["item a"]
        clock.advance(51)
        engine.poll("client", {})
        await engine.poll_due_once()
        assert "item b" in polled(source, 4) and "item c" in polled(source, 4)
        assert engine.resources["item b"].degraded is None

    asyncio.run(scenario())


def test_a_429_the_poll_handled_itself_can_name_its_dimension(clock: Clock) -> None:
    source = PerMethodSource(world_of("a", "b"))
    source.side_request = (30.0, "replies")
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", method_item("a", "history"), KINDS)
        await engine.poll_due_once()
        budget = engine.accounts[""].budget
        assert budget.blocked_until == 0.0
        assert budget.dimension("replies").blocked_until == pytest.approx(clock() + 30)
        # The poll spent on history, which the 429 did not name: its backoff stays at the start.
        assert budget.dimension("replies").backoff == 10 and budget.dimension("history").backoff == 5
        await engine.subscribe("client", method_item("b", "replies"), KINDS)
        clock.advance(10)
        engine.poll("client", {})
        await engine.poll_due_once()
        assert polled(source, 1) == ["item a"]

    asyncio.run(scenario())


def test_a_429_that_names_an_unknown_dimension_blocks_the_account_and_says_so(clock: Clock) -> None:
    source = PerMethodSource(world_of("a", "b"))
    source.limited = {"b": 60.0}
    source.dimension = "replys"
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        sub = await engine.subscribe("client", method_item("b", "replies"), KINDS)
        await engine.poll_due_once()
        events = engine.poll("client", {sub["subscription_id"]: 0})["events"][sub["subscription_id"]]
        assert [e["data"]["reason"] for e in events] == ["rate_limited"]
        assert "unknown budget dimension 'replys'" in events[0]["data"]["message"]
        assert engine.accounts[""].budget.blocked_until == pytest.approx(clock() + 60)
        await engine.subscribe("client", method_item("a", "history"), KINDS)
        await engine.poll_due_once()
        assert polled(source) == ["item b"]

    asyncio.run(scenario())


def test_a_side_request_429_that_names_an_unknown_dimension_fails_the_poll(clock: Clock) -> None:
    source = PerMethodSource(world_of("a"))
    source.side_request = (30.0, "replys")
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        sub = await engine.subscribe("client", method_item("a", "history"), KINDS)
        await engine.poll_due_once()
        events = engine.poll("client", {sub["subscription_id"]: 0})["events"][sub["subscription_id"]]
        assert [e["data"]["reason"] for e in events] == ["error"]
        assert "ValueError: a 429 names the unknown budget dimension 'replys'" in events[0]["data"]["message"]

    asyncio.run(scenario())


class SharedClientSource(FakeSource):
    """Charges each request through `PollContext.current()`, like an API client shared by the polls of an account."""

    async def poll(self, batch: Sequence[PollItem[Snapshot]], ctx: PollContext) -> Mapping[str, PollOutcome[Snapshot]]:
        await asyncio.sleep(0)
        current = PollContext.current()
        assert current is ctx
        current.spend({"requests": 0.1, "complexity": 1.0})
        await asyncio.sleep(0)
        return await super().poll(batch, ctx)


def test_concurrent_polls_each_charge_their_own_requests_through_the_current_context(clock: Clock) -> None:
    source = SharedClientSource(world_of("a", "b"))
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", item("a"), KINDS)
        await engine.subscribe("client", item("b"), KINDS)
        await engine.poll_due_once()
        assert len(source.calls) == 2
        budget = engine.accounts[""].budget
        # Both estimates (1 request, 10 points each) were given back: only what the requests cost remains.
        assert budget.dimension("requests").spent == pytest.approx(0.2)
        assert budget.dimension("complexity").spent == pytest.approx(2.0)

    asyncio.run(scenario())
    assert PollContext.current() is None


def test_nothing_left_waits_for_the_window_to_reset_not_for_hours(clock: Clock) -> None:
    now = clock()
    world = world_of("a")
    world.readings = {"complexity": {"remaining": 0, "reset_at": now + 60}}
    engine = Engine(FakeSource(world), clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", item("a"), KINDS)
        await engine.poll_due_once()
        # Without the cap the stretch makes this 10 s interval 100,000 s long.
        due = engine.scheduler.due_at("item a")
        assert due is not None and due - now == pytest.approx(70)

    asyncio.run(scenario())


def test_a_new_window_brings_the_waiting_targets_forward(clock: Clock) -> None:
    now = clock()
    world = world_of("a", "b")
    world.readings = {"complexity": {"remaining": 0, "reset_at": now + 60}}
    engine = Engine(FakeSource(world), clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", item("a"), KINDS)
        await engine.poll_due_once()
        world.readings = {"complexity": {"remaining": 1_000_000, "reset_at": now + 3600}}
        await engine.subscribe("client", item("b"), KINDS)
        await engine.poll_due_once()
        assert engine.scheduler.due_at("item a") == pytest.approx(now + 10)

    asyncio.run(scenario())


def test_a_batch_blocked_by_the_budget_lets_the_loop_sleep_until_the_budget_covers_it(clock: Clock) -> None:
    world = world_of(*[str(i) for i in range(20)])
    engine = Engine(FakeSource(world, batching=True, cap={"complexity": 100}), clock=clock)

    async def scenario() -> None:
        for i in range(20):
            await engine.subscribe("client", item(str(i)), KINDS)
        await engine.poll_due_once()
        engine.accounts[""].budget.charge({"complexity": 1000}, clock())
        clock.advance(10)
        # Everything is due, but the points come back only in about 2 s: the loop sleeps, it does not spin.
        assert engine._start_due(clock()) == pytest.approx(1.0)
        assert len(engine._tasks) == 0

    asyncio.run(scenario())


def test_first_polls_of_targets_subscribed_seconds_apart_share_one_call(clock: Clock) -> None:
    source = FakeSource(world_of("a", "b", "c", "d"), batching=True, baseline_wait=None)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        for ident in "abc":
            await engine.subscribe("client-" + ident, item(ident), KINDS)
            await engine.poll_due_once()
            clock.advance(1)
        assert source.calls == []
        clock.advance(2)
        await engine.poll_due_once()
        assert [sorted(call.keys) for call in source.calls] == [["item a", "item b", "item c"]]
        # A target without a batch key still polls at once.
        source.batching = False
        await engine.subscribe("client-d", item("d"), KINDS)
        await engine.poll_due_once()
        assert source.calls[-1].keys == ["item d"]

    asyncio.run(scenario())


def test_the_cap_follows_the_window_of_the_dimension_that_is_short(clock: Clock) -> None:
    now = clock()
    world = world_of("a")
    # Out of requests until the minute ends; plenty of complexity points in an hour-long window.
    world.readings = {
        "requests": {"remaining": 0, "reset_at": now + 60},
        "complexity": {"remaining": 1_000_000, "reset_at": now + 3600},
    }
    engine = Engine(FakeSource(world), clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", item("a"), KINDS)
        await engine.poll_due_once()
        due = engine.scheduler.due_at("item a")
        assert due is not None and due - now == pytest.approx(70)

    asyncio.run(scenario())


class SlowSource(FakeSource):
    """Asks for its target `slow` to be polled again in 600 s, whatever the budget says."""

    def _poll_one(self, world: World, item: PollItem[Snapshot]) -> PollOutcome[Snapshot]:
        outcome = super()._poll_one(world, item)
        if item.target.id == "slow" and outcome.error is None:
            return PollOutcome(outcome.snapshot, outcome.events, next_delay=600.0)
        return outcome


def test_a_new_window_leaves_next_delay_and_error_backoff_alone(clock: Clock) -> None:
    now = clock()
    world = world_of("slow", "gone", "c")
    world.readings = {"requests": {"remaining": 1000, "reset_at": now + 60}}
    engine = Engine(SlowSource(world), clock=clock, lease_timeout=1e9)

    async def scenario() -> None:
        await engine.subscribe("client", item("slow"), KINDS)
        await engine.subscribe("client", item("gone"), KINDS)
        await engine.poll_due_once()
        del world.items["gone"]
        for _ in range(4):
            clock.advance(10)
            await engine.poll_due_once()
        slow_due, gone_due = engine.scheduler.due_at("item slow"), engine.scheduler.due_at("item gone")
        assert slow_due is not None and gone_due is not None and gone_due - clock() > 20
        world.readings = {"requests": {"remaining": 1000, "reset_at": clock() + 60}}
        await engine.subscribe("client", item("c"), KINDS)
        await engine.poll_due_once()
        assert engine.scheduler.due_at("item slow") == slow_due
        assert engine.scheduler.due_at("item gone") == gone_due

    asyncio.run(scenario())


def test_spending_on_one_dimension_keeps_the_estimate_of_the_others(clock: Clock) -> None:
    world = world_of("a")
    world.actual_cost = {"requests": 1}
    engine = Engine(FakeSource(world), clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client", item("a"), KINDS)
        await engine.poll_due_once()
        budget = engine.accounts[""].budget
        assert budget.dimension("requests").spent == pytest.approx(1)
        assert budget.dimension("complexity").spent == pytest.approx(10)

    asyncio.run(scenario())
