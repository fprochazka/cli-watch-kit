# cli-watch-kit

A library for building a `watch` command into a CLI: it polls a source, reports each change as a JSON event, and shares one rate-limited daemon between every watcher of the user.

## Developing

```bash
uv sync
uv run ruff check . && uv run ruff format --check . && uv run mypy --strict src tests && uv run pytest
```

## License

MIT
