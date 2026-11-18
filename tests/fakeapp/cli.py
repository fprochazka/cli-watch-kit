"""The fake app's CLI: `fakeapp watch item <id>`, `fakeapp watch box <id>` and `fakeapp watch daemon ...`."""

from __future__ import annotations

import time

import click

from cli_watch_kit.cli import WatchOptions, daemon_group, run_watch, watch_options
from cli_watch_kit.durations import parse_duration
from cli_watch_kit.source import Target
from fakeapp.app import APP
from fakeapp.timers import StaleTimer


@click.group("watch")
def watch() -> None:
    """Watch fake items."""


@watch.command()
@click.argument("ident")
@click.option("--token", help="Sent to the daemon with the subscription.")
@click.option("--user", help="The account the token belongs to.")
@click.option("--stale-after", help="Report stale when the status did not change for this long.")
@watch_options
def item(ident: str, token: str | None, user: str | None, stale_after: str | None, options: WatchOptions) -> None:
    """Watch one item."""
    params = {k: v for k, v in {"token": token, "user": user}.items() if v}
    timers = [StaleTimer(parse_duration(stale_after), time.monotonic())] if stale_after else []
    code = run_watch(
        APP,
        Target("item", ident),
        options,
        params=params,
        enabled_thresholds=["stale"] if stale_after else [],
        timers=timers,
        readme="The files of one fake item watch.\n",
    )
    raise SystemExit(code)


@watch.command()
@click.argument("ident")
@watch_options
def box(ident: str, options: WatchOptions) -> None:
    """Watch a box and its items."""
    raise SystemExit(run_watch(APP, Target("box", ident), options))


watch.add_command(daemon_group(APP))


def main() -> None:
    watch()
