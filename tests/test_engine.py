from __future__ import annotations

import asyncio
import time
from collections.abc import Coroutine, Mapping, Sequence
from typing import Any

import pytest

from cli_watch_kit.daemon.engine import Engine, EngineError
from cli_watch_kit.daemon.scheduler import Scheduler
from cli_watch_kit.daemon.server import handle
from cli_watch_kit.events import LIFECYCLE_KINDS
from cli_watch_kit.source import PollItem, Target, Unauthorized, Wants
from conftest import Clock
from fakeapp.source import FakeSource, World

ITEM = Target("item", "a")
ALL_ITEM = frozenset({"status_changed", "note_added"})


def run[T](coro: Coroutine[Any, Any, T]) -> T:
    return asyncio.run(coro)


def events_of(response: dict[str, Any], sid: str) -> list[dict[str, Any]]:
    return list(response["events"].get(sid, []))


def kinds(response: dict[str, Any], sid: str) -> list[str]:
    return [e["kind"] for e in events_of(response, sid)]


def test_two_subscribers_share_one_resource_and_get_their_own_filter(clock: Clock) -> None:
    world = World(items={"a": {"status": "open"}})
    source = FakeSource(world)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        a = await engine.subscribe("client-a", ITEM, ALL_ITEM)
        b = await engine.subscribe("client-b", ITEM, frozenset({"status_changed"}))
        assert len(engine.resources) == 1
        await engine.poll_due_once()
        poll_a = engine.poll("client-a", {a["subscription_id"]: a["cursor"]})
        started = events_of(poll_a, a["subscription_id"])
        assert [e["kind"] for e in started] == ["watch_started"]
        assert started[0]["data"]["summary"] == {"status": "open", "notes": 0}
        assert poll_a["snapshots"][a["subscription_id"]]["data"] == {"status": "open", "notes": 0}

        world.items["a"] = {"status": "review", "notes": 2}
        clock.advance(10)
        await engine.poll_due_once()
        world.items["a"] = {"status": "done", "notes": 2}
        clock.advance(10)
        await engine.poll_due_once()
        poll_a = engine.poll("client-a", {a["subscription_id"]: 0})
        poll_b = engine.poll("client-b", {b["subscription_id"]: 0})
        assert kinds(poll_a, a["subscription_id"]) == ["status_changed", "note_added", "status_changed", "watch_ended"]
        assert kinds(poll_b, b["subscription_id"]) == [
            "watch_started",
            "status_changed",
            "status_changed",
            "watch_ended",
        ]
        # It ended while watched, so it is not marked as ended before the watch started.
        assert events_of(poll_a, a["subscription_id"])[-1]["data"] == {"reason": "done", "status": "done"}
        assert len(source.calls) == 3

    run(scenario())


def test_late_subscriber_gets_watch_started_and_no_backlog(clock: Clock) -> None:
    world = World(items={"a": {"status": "open"}})
    engine = Engine(FakeSource(world), clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client-a", ITEM, ALL_ITEM)
        await engine.poll_due_once()
        world.items["a"]["status"] = "review"
        clock.advance(10)
        await engine.poll_due_once()
        late = await engine.subscribe("client-c", ITEM, ALL_ITEM)
        assert late["cursor"] == 1
        response = engine.poll("client-c", {late["subscription_id"]: late["cursor"]})
        assert kinds(response, late["subscription_id"]) == ["watch_started"]

    run(scenario())


def test_leases_expire_and_the_engine_goes_idle(clock: Clock) -> None:
    engine = Engine(FakeSource(World(items={"a": {}})), clock=clock)
    sub = run(engine.subscribe("client-a", ITEM, ALL_ITEM))
    assert engine.idle_for(clock()) == 0
    clock.advance(15)
    engine.poll("client-a", {sub["subscription_id"]: 0})
    clock.advance(15)
    engine.reap(clock())
    assert engine.subscriptions
    clock.advance(21)
    engine.reap(clock())
    assert not engine.subscriptions and not engine.resources
    clock.advance(30)
    assert engine.idle_for(clock()) == 30
    response = engine.poll("client-a", {sub["subscription_id"]: 0})
    assert response["unknown"] == [sub["subscription_id"]]


def test_not_found_ends_the_watch_and_rate_limits_degrade_once(clock: Clock) -> None:
    world = World()
    source = FakeSource(world)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        sub = await engine.subscribe("client-a", ITEM, ALL_ITEM)
        await engine.poll_due_once()
        events = events_of(engine.poll("client-a", {sub["subscription_id"]: 0}), sub["subscription_id"])
        assert [e["kind"] for e in events] == ["watch_ended"]
        assert events[0]["data"] == {"reason": "not_found", "message": "no item a"}

        other = Target("item", "b")
        world.rate_limited = 1.0
        sub2 = await engine.subscribe("client-a", other, ALL_ITEM)
        await engine.poll_due_once()
        # The second 429 keeps the same reason, so it is not reported again. Renew the lease while waiting.
        engine.poll("client-a", {sub2["subscription_id"]: 0})
        clock.advance(15)
        engine.poll("client-a", {})
        clock.advance(15)
        engine.poll("client-a", {})
        await engine.poll_due_once()
        assert engine.resources[other.key].failures == 2
        events = events_of(engine.poll("client-a", {sub2["subscription_id"]: 0}), sub2["subscription_id"])
        assert [e["kind"] for e in events] == ["watch_degraded"]
        assert events[0]["data"]["reason"] == "rate_limited"
        assert engine.accounts[""].budget.rate_limited_count == 2

        # Recovery clears the degraded state, so the next trouble is reported again.
        world.rate_limited = None
        world.items["b"] = {"status": "open"}
        clock.advance(300)
        engine.poll("client-a", {})
        await engine.poll_due_once()
        assert engine.resources[other.key].degraded is None
        world.failing = "HTTP 502"
        clock.advance(10)
        engine.poll("client-a", {})
        await engine.poll_due_once()
        response = engine.poll("client-a", {sub2["subscription_id"]: 1})
        # The first successful poll gave the subscription its watch_started; then the new trouble is reported.
        assert [(e["kind"], e["data"].get("reason")) for e in events_of(response, sub2["subscription_id"])] == [
            ("watch_started", None),
            ("watch_degraded", "error"),
        ]

    run(scenario())


def test_seeded_baseline_reports_what_changed_while_the_daemon_was_down(clock: Clock) -> None:
    world = World(items={"a": {"status": "open"}})
    first = Engine(FakeSource(world), clock=clock)

    async def scenario() -> None:
        sub = await first.subscribe("client-a", ITEM, ALL_ITEM)
        await first.poll_due_once()
        stored = first.poll("client-a", {sub["subscription_id"]: 0})["snapshots"][sub["subscription_id"]]["data"]

        world.items["a"] = {"status": "open", "notes": 1}
        second = Engine(FakeSource(world), clock=clock)
        sub2 = await second.subscribe("client-a", ITEM, ALL_ITEM, baseline=stored)
        await second.poll_due_once()
        events = events_of(second.poll("client-a", {sub2["subscription_id"]: 0}), sub2["subscription_id"])
        assert [e["kind"] for e in events] == ["watch_started", "note_added"]

        # A baseline the source cannot read is ignored: the first poll is the baseline again.
        third = Engine(FakeSource(world), clock=clock)
        sub3 = await third.subscribe("client-a", ITEM, ALL_ITEM, baseline={"something": "else"})
        await third.poll_due_once()
        assert kinds(third.poll("client-a", {sub3["subscription_id"]: 0}), sub3["subscription_id"]) == ["watch_started"]

    run(scenario())


def test_end_on_the_first_poll_is_already_ended_unless_a_seed_saw_the_target_open(clock: Clock) -> None:
    world = World(items={"a": {"status": "done"}})

    async def scenario() -> None:
        engine = Engine(FakeSource(world), clock=clock)
        sub = await engine.subscribe("client-a", ITEM, ALL_ITEM)
        await engine.poll_due_once()
        events = events_of(engine.poll("client-a", {sub["subscription_id"]: 0}), sub["subscription_id"])
        assert [e["kind"] for e in events] == ["watch_started", "watch_ended"]
        assert events[-1]["data"] == {"reason": "done", "status": "done", "already_ended": True}

        other = Engine(FakeSource(world), clock=clock)
        sub = await other.subscribe("client-a", ITEM, ALL_ITEM, baseline={"status": "open", "notes": 0})
        await other.poll_due_once()
        events = events_of(other.poll("client-a", {sub["subscription_id"]: 0}), sub["subscription_id"])
        assert [e["kind"] for e in events] == ["watch_started", "status_changed", "watch_ended"]
        assert events[-1]["data"] == {"reason": "done", "status": "done"}

    run(scenario())


def test_a_parent_relays_its_childrens_events_and_lets_them_go(clock: Clock) -> None:
    world = World(items={"a": {"status": "open"}, "b": {"status": "open"}}, boxes={"x": ["a"]})
    engine = Engine(FakeSource(world), clock=clock)
    box = Target("box", "x")

    async def scenario() -> None:
        sub = await engine.subscribe("client-a", box, frozenset({"box_changed", "status_changed"}))
        await engine.poll_due_once()
        assert set(engine.resources) == {"box x", "item a"}
        assert engine.resources["item a"].events == frozenset({"status_changed"})
        await engine.poll_due_once()
        world.items["a"] = {"status": "review", "notes": 5}
        clock.advance(10)
        await engine.poll_due_once()
        world.boxes["x"] = ["b"]
        clock.advance(10)
        await engine.poll_due_once()
        response = engine.poll("client-a", {sub["subscription_id"]: 0})
        relayed = events_of(response, sub["subscription_id"])
        assert [e["kind"] for e in relayed] == ["watch_started", "status_changed", "box_changed"]
        assert relayed[1]["resource"] == "box x" and relayed[1]["data"]["id"] == "a"
        assert set(engine.resources) == {"box x", "item b"}

        engine.unsubscribe("client-a", sub["subscription_id"])
        assert not engine.resources

    run(scenario())


def test_accounts_keep_resources_and_budgets_apart_and_get_their_params(clock: Clock) -> None:
    world = World(items={"a": {"status": "open"}})
    source = FakeSource(world)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client-a", ITEM, ALL_ITEM, params={"user": "u1", "token": "t1"})
        await engine.subscribe("client-b", ITEM, ALL_ITEM, params={"user": "u2", "token": "t2"})
        await engine.subscribe("client-c", ITEM, ALL_ITEM, params={"user": "u1", "token": "t3"})
        assert sorted(engine.resources) == ["u1|item a", "u2|item a"]
        await engine.poll_due_once()
        by_account = {call.account: call.params for call in source.calls}
        # The latest params of an account win.
        assert by_account == {"u1": {"user": "u1", "token": "t3"}, "u2": {"user": "u2", "token": "t2"}}
        status = engine.status()
        assert set(status["accounts"]) == {"u1", "u2"}
        assert "t3" not in repr(status)
        with pytest.raises(Unauthorized):
            await engine.subscribe("client-d", ITEM, ALL_ITEM, params={"token": "bad"})
        with pytest.raises(EngineError, match="unknown event kinds: nope"):
            await engine.subscribe("client-d", ITEM, frozenset({"nope"}))
        with pytest.raises(EngineError, match="unknown resource kind"):
            await engine.subscribe("client-d", Target("nope", "x"), ALL_ITEM)

    run(scenario())


def test_each_resource_keeps_its_private_state_until_it_is_released(clock: Clock) -> None:
    engine = Engine(FakeSource(World(items={"a": {}})), clock=clock)

    async def scenario() -> None:
        sub = await engine.subscribe("client-a", ITEM, ALL_ITEM)
        await engine.poll_due_once()
        clock.advance(10)
        await engine.poll_due_once()
        assert engine.resources[ITEM.key].state == {"polls": 2}
        engine.unsubscribe("client-a", sub["subscription_id"])
        await engine.subscribe("client-a", ITEM, ALL_ITEM)
        assert engine.resources[ITEM.key].state == {}

    run(scenario())


def test_source_options_are_checked_merged_and_passed_on_to_children(clock: Clock) -> None:
    world = World(items={"a": {"status": "open"}}, boxes={"x": ["a"]})
    source = FakeSource(world)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        with pytest.raises(EngineError, match="unsupported options"):
            await engine.subscribe("client-a", ITEM, ALL_ITEM, options={"detail": "everything"})
        await engine.subscribe("client-a", ITEM, ALL_ITEM, options={"detail": "full"})
        await engine.subscribe("client-b", ITEM, ALL_ITEM, options={"detail": "full"})
        await engine.subscribe("client-c", ITEM, ALL_ITEM)
        await engine.poll_due_once()
        assert source.calls[-1].options == {"item a": [{"detail": "full"}]}
        # A box relays item a and asks for its own options; a new option polls the item again at once.
        await engine.subscribe("client-d", Target("box", "x"), frozenset({"status_changed"}))
        await engine.poll_due_once()
        await engine.poll_due_once()
        assert source.calls[-1].keys == ["item a"]
        assert source.calls[-1].options == {"item a": [{"detail": "full"}, {"detail": "brief"}]}
        assert {"detail": "full"} in [s["options"] for s in engine.status()["subscriptions"]]

    run(scenario())


def test_a_filter_that_grows_polls_again_at_once(clock: Clock) -> None:
    world = World(items={"a": {"status": "open"}})
    source = FakeSource(world)
    engine = Engine(source, clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client-a", ITEM, frozenset({"status_changed"}))
        await engine.poll_due_once()
        await engine.subscribe("client-b", ITEM, frozenset({"note_added"}))
        await engine.poll_due_once()
        assert len(source.calls) == 2
        await engine.subscribe("client-c", ITEM, frozenset({"note_added"}))
        await engine.poll_due_once()
        assert len(source.calls) == 2

    run(scenario())


def test_status_reports_calls_and_batches_per_account(clock: Clock) -> None:
    engine = Engine(FakeSource(World(items={"a": {}})), clock=clock)

    async def scenario() -> None:
        await engine.subscribe("client-a", ITEM, ALL_ITEM)
        await engine.poll_due_once()
        clock.advance(10)
        await engine.poll_due_once()

    run(scenario())
    status = engine.status()
    assert status["accounts"][""]["calls"] == 2
    assert status["source"] == {"calls": 2}
    assert status["accounts"][""]["recent_batches"] == [["item a"], ["item a"]]
    assert status["resources"][0]["resource"] == ITEM.key
    assert status["accounts"][""]["budget"]["dimensions"]["complexity"]["spent"] == 20


def test_scheduler_round_robin_and_urgent_first() -> None:
    scheduler = Scheduler()
    scheduler.schedule("busy", 0)
    scheduler.schedule("quiet", 5)
    scheduler.mark_served("busy", 1)
    scheduler.schedule("busy", 2)
    # Both due: the one served longest ago (never) goes first, even though it became due later.
    assert scheduler.ready(10) == ["quiet", "busy"]
    scheduler.schedule("probe", 9, urgent=True)
    assert scheduler.ready(10)[0] == "probe"
    assert scheduler.ready(1) == []
    assert scheduler.next_due() == 2


def test_scheduler_forget_and_remove() -> None:
    scheduler = Scheduler()
    scheduler.schedule("a", 1)
    assert "a" in scheduler
    scheduler.remove("a")
    assert "a" not in scheduler and scheduler.next_due() is None


def test_scheduler_batches_a_due_group_and_pulls_in_its_members_due_soon() -> None:
    scheduler = Scheduler()
    for key, due in [("a1", 0), ("b1", 1), ("a2", 3), ("a3", 30), ("solo", 2), ("b2", 4)]:
        scheduler.schedule(key, due)
    groups = {"a1": "A", "a2": "A", "a3": "A", "b1": "B", "b2": "B"}
    batches = scheduler.ready_batches(2, groups.get, window=5)
    assert batches == [["a1", "a2"], ["b1", "b2"], ["solo"]]
    assert scheduler.ready_batches(2, groups.get) == [["a1"], ["b1"], ["solo"]]


class FaultySource(FakeSource):
    """A source with bugs that can be switched on: its interval raises for some targets, its cost for all."""

    def __init__(self, world: World) -> None:
        super().__init__(world, batching=True)
        self.bad_interval: set[str] = set()
        self.bad_cost = False

    def interval(self, target: Target, snapshot: dict[str, Any] | None) -> float:
        if target.id in self.bad_interval:
            raise KeyError("a bug in the app")
        return super().interval(target, snapshot)

    def cost(self, batch: Sequence[PollItem[dict[str, Any]]]) -> Mapping[str, float]:
        if self.bad_cost:
            raise RuntimeError("a bug in cost")
        return super().cost(batch)


def test_a_bug_in_the_source_degrades_its_target_and_the_others_keep_polling(clock: Clock) -> None:
    source = FaultySource(World(items={i: {"status": "open"} for i in "abc"}))
    engine = Engine(source, clock=clock, lease_timeout=1e9)

    async def scenario() -> None:
        subs = {i: await engine.subscribe("client", Target("item", i), ALL_ITEM) for i in "abc"}
        await engine.poll_due_once()
        source.bad_interval = {"b"}
        clock.advance(10)
        await engine.poll_due_once()
        bad = engine.resources["item b"]
        assert not bad.polling and bad.degraded == "error"
        assert engine.scheduler.due_at("item b") is not None
        response = engine.poll("client", {subs["b"]["subscription_id"]: 0})
        degraded = events_of(response, subs["b"]["subscription_id"])[-1]
        assert degraded["kind"] == "watch_degraded" and "a bug in the app" in degraded["data"]["message"]
        assert engine.resources["item a"].degraded is None
        source.bad_interval = set()
        clock.advance(1000)
        calls = len(source.calls)
        await engine.poll_due_once()
        assert any("item b" in call.keys for call in source.calls[calls:])
        assert engine.resources["item b"].degraded is None

    run(scenario())


def test_a_bug_in_cost_never_stops_the_engine_loop() -> None:
    source = FaultySource(World(items={"a": {"status": "open"}}))
    source.bad_cost = True
    engine = Engine(source)

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(engine.run(stop))
        await engine.subscribe("client", ITEM, ALL_ITEM)
        engine._kick()
        await asyncio.sleep(0.2)
        assert not task.done()
        assert engine.resources[ITEM.key].degraded == "error"
        assert not engine.resources[ITEM.key].polling
        stop.set()
        engine._kick()
        await task

    run(scenario())


def test_every_resubscriber_with_a_baseline_gets_the_changes_from_the_gap(clock: Clock) -> None:
    world = World(items={"a": {"status": "review", "notes": 0}})
    engine = Engine(FakeSource(world), clock=clock)
    baseline = {"status": "open", "notes": 0}

    async def scenario() -> None:
        # After a daemon restart, the first watcher seeds the resource; the second finds it already polled.
        a = await engine.subscribe("A", ITEM, ALL_ITEM, baseline=baseline)
        await engine.poll_due_once()
        b = await engine.subscribe("B", ITEM, ALL_ITEM, baseline=baseline)
        assert kinds(engine.poll("A", {a["subscription_id"]: a["cursor"]}), a["subscription_id"]) == [
            "watch_started",
            "status_changed",
        ]
        gap = events_of(engine.poll("B", {b["subscription_id"]: b["cursor"]}), b["subscription_id"])
        assert [e["kind"] for e in gap] == ["watch_started", "status_changed"]
        assert gap[1]["data"] == {"id": "a", "from": "open", "to": "review"}
        # A baseline equal to the current snapshot brings nothing but watch_started.
        c = await engine.subscribe("C", ITEM, ALL_ITEM, baseline={"status": "review", "notes": 0})
        assert kinds(engine.poll("C", {c["subscription_id"]: c["cursor"]}), c["subscription_id"]) == ["watch_started"]

    run(scenario())


def test_a_watcher_whose_lease_expired_gets_the_gap_when_it_comes_back(clock: Clock) -> None:
    world = World(items={"a": {"status": "open", "notes": 0}})
    engine = Engine(FakeSource(world), clock=clock)

    async def scenario() -> None:
        await engine.subscribe("keeper", ITEM, ALL_ITEM)
        late = await engine.subscribe("sleeper", ITEM, ALL_ITEM)
        await engine.poll_due_once()
        stored = engine.poll("sleeper", {late["subscription_id"]: 0})["snapshots"][late["subscription_id"]]["data"]
        world.items["a"] = {"status": "open", "notes": 3}
        for _ in range(3):
            clock.advance(10)
            engine.poll("keeper", {})
            await engine.poll_due_once()
        assert engine.poll("sleeper", {late["subscription_id"]: 0})["unknown"] == [late["subscription_id"]]
        back = await engine.subscribe("sleeper", ITEM, ALL_ITEM, baseline=stored)
        assert kinds(engine.poll("sleeper", {back["subscription_id"]: back["cursor"]}), back["subscription_id"]) == [
            "watch_started",
            "note_added",
        ]

    run(scenario())


def test_leases_run_on_a_monotonic_clock_by_default() -> None:
    engine = Engine(FakeSource(World()))
    assert engine.lease_clock is time.monotonic


def test_a_bug_in_a_request_is_answered_and_logged(clock: Clock, caplog: pytest.LogCaptureFixture) -> None:
    engine = Engine(FakeSource(World(items={"a": {}})), clock=clock)
    request = {
        "op": "subscribe",
        "client_id": "c",
        "target": ITEM.to_wire(),
        "options": {"events": ["status_changed"]},
        "params": {"user": "boom", "token": "a-secret-token"},
    }
    response = run(handle(engine, request, asyncio.Event(), {}))
    assert response == {"ok": False, "error": "the daemon failed: KeyError: 'a bug in account()'", "reason": "error"}
    assert "the 'subscribe' request failed" in caplog.text and "KeyError" in caplog.text
    assert "a-secret-token" not in caplog.text


def test_private_events_and_the_snapshot_come_again_until_their_answer_is_acknowledged(clock: Clock) -> None:
    engine = Engine(FakeSource(World(items={"a": {"status": "open"}})), clock=clock)

    async def scenario() -> None:
        sub = await engine.subscribe("client", ITEM, ALL_ITEM)
        sid = sub["subscription_id"]
        await engine.poll_due_once()
        first = engine.poll("client", {sid: 0}, {})
        assert kinds(first, sid) == ["watch_started"] and sid in first["snapshots"]
        # That answer was lost: the next poll carries no ack for it, so it comes again.
        again = engine.poll("client", {sid: 0}, {})
        assert kinds(again, sid) == ["watch_started"] and sid in again["snapshots"]
        acked = engine.poll("client", {sid: 0}, again["acks"])
        assert kinds(acked, sid) == [] and acked["snapshots"] == {}
        assert engine.poll("client", {sid: 0}, acked["acks"])["events"] == {}

    run(scenario())


def test_a_404_right_after_a_restart_degrades_a_seeded_watch_instead_of_ending_it(clock: Clock) -> None:
    engine = Engine(FakeSource(World()), clock=clock)

    async def scenario() -> None:
        sub = await engine.subscribe("client", ITEM, ALL_ITEM, baseline={"status": "open", "notes": 0})
        await engine.poll_due_once()
        events = events_of(engine.poll("client", {sub["subscription_id"]: 0}), sub["subscription_id"])
        assert [(e["kind"], e["data"].get("reason")) for e in events] == [("watch_degraded", "not_found")]
        assert not engine.resources[ITEM.key].ended

    run(scenario())


class ChildlessSource(FakeSource):
    """A source whose children() can be made to raise."""

    broken = False

    def children(
        self, target: Target, snapshot: dict[str, Any], item: PollItem[dict[str, Any]]
    ) -> Mapping[Target, frozenset[str] | Wants]:
        if self.broken:
            raise RuntimeError("a bug in children()")
        return super().children(target, snapshot, item)


def test_a_subscribe_whose_children_fail_leaves_no_orphan_behind(clock: Clock) -> None:
    source = ChildlessSource(World(items={"a": {}}, boxes={"x": ["a"]}))
    engine = Engine(source, clock=clock)
    box = Target("box", "x")

    async def scenario() -> None:
        await engine.subscribe("A", box, frozenset({"box_changed"}))
        await engine.poll_due_once()
        source.broken = True
        with pytest.raises(RuntimeError, match="a bug in children"):
            await engine.subscribe("B", box, frozenset({"status_changed"}))
        assert [s.client_id for s in engine.subscriptions.values()] == ["A"]
        assert engine.resources["box x"].subscriptions == {next(iter(engine.subscriptions))}
        assert engine.resources["box x"].events == frozenset({"box_changed", *LIFECYCLE_KINDS})

    run(scenario())
