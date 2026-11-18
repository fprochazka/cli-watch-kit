"""`python -m fakeapp.daemon`: the fake app's daemon process."""

from __future__ import annotations

from cli_watch_kit.daemon import main
from fakeapp.app import APP

if __name__ == "__main__":
    main(APP)
