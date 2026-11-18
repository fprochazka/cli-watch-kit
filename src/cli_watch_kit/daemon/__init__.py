"""The shared polling daemon. An app's daemon module is two lines:

```python
from cli_watch_kit.daemon import main
from my_cli.watch import APP

if __name__ == "__main__":
    main(APP)
```
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, NoReturn

if TYPE_CHECKING:
    from cli_watch_kit.app import WatchApp


def main(app: WatchApp) -> NoReturn:
    """Run the daemon of `app`. Watchers start it with `python -m <app.daemon_module>`; it is not run by hand."""
    from cli_watch_kit.daemon.lifecycle import run_daemon

    sys.exit(run_daemon(app, sys.argv[1:]))
