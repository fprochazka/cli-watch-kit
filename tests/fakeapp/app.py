"""The fake app's `WatchApp`. The daemon process builds its source from the environment (see `source_from_env`)."""

from __future__ import annotations

import os

from cli_watch_kit.app import WatchApp
from fakeapp.source import source_from_env

APP = WatchApp(
    name="fakeapp",
    source=source_from_env,
    daemon_module="fakeapp.daemon",
    app_protocol=int(os.environ.get("FAKEAPP_APP_PROTOCOL", "1")),
    version="1.2.3",
    status_prefix="fakeapp watch: ",
    envelope_tag="fakeapp-watcher",
    command="fakeapp watch",
)
