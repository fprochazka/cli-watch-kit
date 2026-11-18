from __future__ import annotations

import pytest

from cli_watch_kit.durations import format_duration, parse_duration
from cli_watch_kit.events import LIFECYCLE_KINDS, Event, FilterError, KindCatalog
from fakeapp.catalog import CATALOG


def test_default_is_everything_the_resource_tracks_but_the_threshold_kinds() -> None:
    expected = frozenset({"status_changed", "note_added", *LIFECYCLE_KINDS})
    assert CATALOG.resolve("item", None, None) == expected


def test_a_threshold_option_adds_its_kind_to_the_default_and_to_its_group() -> None:
    assert "stale" in CATALOG.resolve("item", None, None, ["stale"])
    assert "stale" not in CATALOG.resolve("item", ["status"], None)
    assert "stale" in CATALOG.resolve("item", ["status"], None, ["stale"])


def test_groups_expand_to_kinds_the_resource_produces() -> None:
    selected = CATALOG.resolve("item", ["status,notes"], None)
    assert {"status_changed", "note_added"} <= selected
    assert "box_changed" not in selected
    assert set(LIFECYCLE_KINDS) <= selected


def test_exclude_removes_a_group() -> None:
    selected = CATALOG.resolve("box", None, ["notes"])
    assert selected == frozenset({"box_changed", "status_changed", *LIFECYCLE_KINDS})


def test_repeated_and_comma_separated_values_combine() -> None:
    assert CATALOG.resolve("box", ["box_changed", "note_added,status_changed"], None) == frozenset(
        {"box_changed", "note_added", "status_changed", *LIFECYCLE_KINDS}
    )


@pytest.mark.parametrize(
    ("kind", "include", "exclude", "enabled", "message"),
    [
        ("item", ["stauts"], None, (), "unknown event kind or group 'stauts'"),
        ("item", ["box_changed"], None, (), "never produces 'box_changed'"),
        ("item", None, ["lifecycle"], (), "cannot be excluded"),
        ("item", None, ["watch_started"], (), "cannot be excluded"),
        ("item", ["stale"], None, (), "only with --stale-after"),
        ("box", None, None, ["stale"], "--stale-after: a box watch never produces 'stale'"),
        ("item", ["notes"], None, ["stale"], "'stale' is filtered out"),
        ("nothing", None, None, (), "unknown resource kind"),
    ],
)
def test_mistakes_fail_at_startup(
    kind: str, include: list[str] | None, exclude: list[str] | None, enabled: tuple[str, ...], message: str
) -> None:
    with pytest.raises(FilterError, match=message):
        CATALOG.resolve(kind, include, exclude, enabled)


def test_a_catalog_rejects_the_reserved_group_and_unknown_thresholds() -> None:
    with pytest.raises(ValueError, match="reserved"):
        KindCatalog({"x": ("a",)}, groups={"lifecycle": ("a",)})
    with pytest.raises(ValueError, match="threshold kinds that no resource produces"):
        KindCatalog({"x": ("a",)}, thresholds={"b": "--b-after"})


def test_daemon_kinds_leave_out_the_thresholds() -> None:
    assert CATALOG.daemon_kinds({"stale", "note_added"}) == frozenset({"note_added"})
    assert CATALOG.groups_for("item") == ["status", "notes"]


def test_event_json_round_trip() -> None:
    event = Event("note_added", "item a", 3, "2026-11-09T10:00:00.000Z", {"notes": 1}, ("/tmp/note.md",))
    assert Event.from_json(event.to_json()) == event
    assert "artifacts" not in Event("x", "r", 1, "t", {}).to_json()


@pytest.mark.parametrize(("text", "seconds"), [("90", 90), ("1.5", 1.5), ("90s", 90), ("30m", 1800), ("1h30m", 5400)])
def test_durations_parse(text: str, seconds: float) -> None:
    assert parse_duration(text) == seconds


@pytest.mark.parametrize("text", ["", "abc", "1d", "m"])
def test_bad_durations_fail(text: str) -> None:
    with pytest.raises(ValueError, match="cannot read the duration"):
        parse_duration(text)


def test_durations_format_back() -> None:
    assert [format_duration(s) for s in (0, 59.6, 90, 7200, 5405)] == ["0s", "1m", "1m30s", "2h", "1h30m5s"]
