[![CI](https://github.lightsource.ca/daq/kv-dict/actions/workflows/ci.yml/badge.svg)](https://github.lightsource.ca/daq/kv-dict/actions/workflows/ci.yml)

# kv-dict

KV backed dictionary, similar to RedisJSONDict

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

## Remote Mapping + NATS JetStream KV Backend Example

Run a basic `RemoteKVMapping` flow backed by NATS JetStream KV:

```bash
uv run python examples/remote_mapping_nats_example.py
```

Note: this example uses `create_bucket=False` (production-style). Ensure the
`kv_dict` bucket already exists.

<details>
<summary>Create the <code>kv_dict</code> bucket locally (for testing)</summary>

Run this once before executing the NATS example:

```bash
uv run python - <<'PY'
import asyncio
import nats


async def main() -> None:
  nc = await nats.connect(servers=["nats://nats:4222"])
  try:
    js = nc.jetstream()
    try:
      await js.key_value("kv_dict")
      print("Bucket already exists: kv_dict")
    except Exception:
      await js.create_key_value(bucket="kv_dict")
      print("Created bucket: kv_dict")
  finally:
    await nc.close()


asyncio.run(main())
PY
```

</details>

## Differences from Python `dict`

`RemoteKVMapping` is intentionally `dict`-like, but not a byte-for-byte
replacement of the built-in `dict` behavior.

### Key behavioral differences

- Iteration order is sorted by key, not insertion order.
- Values are persisted through backend round-trips, so operations are not purely
  in-memory.
- Nested `dict` and `list` mutations are write-through.
- Mutable non-dict/non-list values are not automatically write-through.

### Missing / non-parity APIs

- `fromkeys()` is not implemented.

### Practical guidance

- Treat this mapping as a backend-backed structure, not an in-memory object.
- For nested non-dict updates, reassign the modified value to persist changes.
- If strict insertion-order semantics are required, do not rely on iteration
  order from `RemoteKVMapping`.

### Dict parity roadmap

- [x] Implement `copy()` semantics for backend-backed snapshots.
- [x] Implement in-place dict union operator (`|=`).
- [x] Implement dict union operator (`|`).
- [ ] Implement `fromkeys()` with explicit persistence semantics.
- [x] Add write-through wrappers for list operations.
- [ ] Add write-through wrappers for additional mutable container types (if
      required).
- [ ] Decide and document stable ordering strategy (sorted vs insertion-order).
- [ ] Add dedicated parity tests against a reference `dict` behavior matrix.
