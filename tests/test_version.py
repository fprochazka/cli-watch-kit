from __future__ import annotations

import cli_watch_kit


def test_the_package_knows_its_version() -> None:
    assert cli_watch_kit.__version__
