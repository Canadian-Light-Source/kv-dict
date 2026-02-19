[![CI](https://github.lightsource.ca/daq/kv-dict/actions/workflows/ci.yml/badge.svg)](https://github.lightsource.ca/daq/kv-dict/actions/workflows/ci.yml)

# kv-dict

KV bacjed dictionary, similar to RedisJSONDict

## Install Package

- pip: `pip install git+https://github.lightsource.ca/daq/kv-dict`
- uv: `uv add git+https://github.lightsource.ca/daq/kv-dict`
  - add `--optional FEATURE` to add as an optional dependency
- pixi: `pixi add --git https://github.lightsource.ca/daq/kv-dict`
  - add `--feature FEATURE` to add as an optional dependency

## Development

This project uses `uv` for managing project dependencies. Installation
instructions may be found
[here](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone https://github.lightsource.ca/daq/kv-dict.git
cd kv-dict
uv sync --all-extras
```

## Async Bridge POC Example

Run the focused bridge proof-of-concept:

```bash
uv run python examples/async_bridge_poc_example.py
```

This example demonstrates:

- `__set_item__` + `__get_time__` aliases
- `__setitem__` + `__getitem__` via `mapping[key] = value` and `mapping[key]`
- explicit `close()` cleanup

## Remote Mapping + In-Memory Backend Example

Run a basic `RemoteKVMapping` flow backed by `InMemoryAsyncBackend`:

```bash
uv run python examples/remote_mapping_in_memory_example.py
```

## Remote Mapping + Redis/Dragonfly Backend Example

Run a basic `RemoteKVMapping` flow backed by a Redis-compatible server:

```bash
uv run python examples/remote_mapping_redis_example.py
```

Note: in the devcontainer compose setup, the Redis-compatible service hostname
is `redis`.
