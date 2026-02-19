# Design Brief: Async KV-Backed MutableMapping with Nested JSON Support

## Problem Statement

Build a Python `MutableMapping` implementation that provides a dict-like
interface over a generic key-value store backend (supporting Redis and NATS KV
Store). Values in the KV store are JSON objects, and the mapping must
flatten/unflatten nested structures using a configurable entry point and
separator.

**Key Constraint**: The backend uses `asyncio`, but `MutableMapping` methods
(`__getitem__`, `__setitem__`, etc.) must be synchronous.

---

## Design Challenges & Solutions

### 1. Async/Sync Bridge

**Challenge**: Cannot call async operations from synchronous methods without an
existing event loop.

**Solution**: Use a dedicated thread pool with its own event loop. Wrap all
backend calls using `asyncio.to_thread()` or a custom executor pattern.

**Alternative**: Accept that this MutableMapping is "blocking" and document
accordingly. Do not use `asyncio.run()` (fails if loop already exists) or
`nest_asyncio` (not production-safe).

### 2. Nested Key Mapping with Entry Point & Separator

**Challenge**: Map flat KV store keys like `"ep1:main:sup1"` → nested dict
`{"main": {"sup1": value}}`

**Solution**:

- Accept `entry_point` (e.g., `"ep1"`) and `sep` (e.g., `":"`) as parameters
- For KV key `"ep1:main:sup1"`:
  1. Verify it starts with `entry_point + sep`
  2. Strip prefix to get `"main:sup1"`
  3. Split by separator to get path `["main", "sup1"]`
  4. Build/navigate nested dict accordingly

**Example**:

```
KV Store Keys:
  ep1:main:sup1 → '{"key": "value"}'
  ep1:main:sup2 → '[1, 2, 3]'
  ep1:other → '{"nested": true}'

Resulting mapping["main"]:
  {"sup1": {"key": "value"}, "sup2": [1, 2, 3]}

Resulting mapping["other"]:
  {"nested": true}
```

### 3. Edge Case: Arbitrary Nested JSON with Conflicts

**Challenge**: What if KV store contains both:

- `ep1:main:sup1` → `{"key": "value"}` (parent is a dict)
- `ep1:main:sup1:sub` → `[1, 2, 3]` (child exists)

This creates a structural conflict: `sup1` must be both a leaf value AND a
container.

**Solution - Conflict Resolution Strategy**:

1. **Dict + Children**: If a node has dict value AND children → merge
   recursively using `deepmerge`
   - Result: `{"key": "value", "sub": [1, 2, 3]}`
2. **Scalar/Array + Children**: If a node has non-dict value AND children → use
   special `_value` key
   - Result: `{"_value": [1, 2, 3], "sub": {...}}`
3. **No Conflict**: If only leaf OR only container → proceed normally

**Rationale**: Supports arbitrary nested JSON without data loss. Document the
`_value` pattern clearly to users.

---

## Technology Choices

### Libraries

- **`pygtrie`** (v2.5.0+): Internal trie structure for efficient prefix-based
  lookups and nested key management
  - Chosen over alternatives: stable, well-tested, perfect fit for nested prefix
    structures
  - Maintenance: Stable library (no frequent updates needed); not abandoned
- **`deepmerge`**: Recursive dict merging for conflict resolution
- **`aioredis`**: Redis backend (async)
- **`nats.py`**: NATS KV Store backend (async)
- **`orjson`** or `ujson`\*\* (optional): Faster JSON encoding/decoding vs
  stdlib `json`

### Async/Sync Handling

- Use `asyncio.to_thread()` (Python 3.9+) or
  `concurrent.futures.ThreadPoolExecutor` with dedicated event loop
- Do NOT use `asyncio.run()` (breaks with existing loops) or `nest_asyncio` (not
  production-safe)

---

## Architecture Overview

```
MutableMapping (inheritance chain)
  └── RemoteKVMapping
      ├── Backend (ABC)
      │   ├── RedisBackend
      │   └── NatsBackend
      ├── KeyMapper (handles entry_point + sep parsing)
      ├── ValueCodec (JSON encode/decode)
      ├── TrieStructure (pygtrie.StringTrie)
      └── ThreadExecutor (async/sync bridge)
```

### Component Responsibilities

1. **RemoteKVMapping** (main class)

   - Implements `MutableMapping` interface
   - Orchestrates Backend, KeyMapper, Trie, and Executor
   - Synchronous methods: `__getitem__`, `__setitem__`, `__delitem__`,
     `__iter__`, `__len__`
   - Internal async methods for backend operations

2. **Backend** (abstract base)

   - Abstract methods: `async get(key)`, `async set(key, value)`,
     `async delete(key)`, `async list_keys(prefix)`
   - Subclasses implement Redis and NATS specifics

3. **KeyMapper**

   - Parses `ep1:main:sup1` → `["main", "sup1"]`
   - Filters KV keys by entry_point prefix
   - Reconstructs KV keys from nested paths

4. **TrieStructure** (pygtrie.StringTrie)

   - Stores flattened KV keys for efficient prefix lookups
   - Supports building nested dicts on retrieval

5. **ValueCodec**

   - Encodes Python objects → JSON strings (for storage)
   - Decodes JSON strings → Python objects (on retrieval)

6. **ThreadExecutor**
   - Bridge between sync `MutableMapping` calls and async backend operations
   - Runs async operations in dedicated thread pool with event loop

---

## Implementation Requirements

### MutableMapping Interface

Implement all abstract methods:

- `__getitem__(key)` → Returns nested dict/value
- `__setitem__(key, value)` → Stores as JSON in KV
- `__delitem__(key)` → Deletes from KV
- `__iter__()` → Iterates over top-level keys in nested structure
- `__len__()` → Returns count of top-level keys

### Constructor

```python
class Backend():
  ...

RemoteKVMapping(
    backend: Backend,
    entry_point: str,
    sep: str = ":",
    json_encoder=None,  # Custom encoder (default: json.dumps)
    json_decoder=None,  # Custom decoder (default: json.loads)
)
```

### Backend Interface (Protocol/ABC)

```python
class Backend(ABC):
    async def get(self, key: str) -> Optional[str]: pass
    async def set(self, key: str, value: str) -> None: pass
    async def delete(self, key: str) -> None: pass
    async def list_keys(self, prefix: str) -> List[str]: pass
    async def close(self): pass
```

### Nested Dict Reconstruction Logic

1. Fetch all KV keys with entry_point prefix from backend
2. Build trie from flattened keys
3. On `__getitem__`, reconstruct nested dict from trie prefix
4. Apply conflict resolution if needed (deepmerge or `_value` pattern)

### JSON Value Handling

- Store all values as JSON strings in KV store
- Decode on retrieval to Python types (dict, list, str, int, etc.)
- Encode on storage to JSON strings

---

## Usage Example

```python
from your_module import RemoteKVMapping, RedisBackend, NatsBackend

# With Redis backend
redis_backend = RedisBackend(url="redis://localhost:6379")
mapping = RemoteKVMapping(
    backend=redis_backend,
    entry_point="ep1",
    sep=":"
)

# Assuming Redis has: ep1:user:alice → '{"age": 30, "email": "alice@..."}'
user = mapping["user"]  # → {"alice": {"age": 30, "email": "alice@..."}}
alice = mapping["user"]["alice"]  # → {"age": 30, "email": "alice@..."}

# Setting values
mapping["user"]["bob"] = {"age": 25, "email": "bob@..."}
# Stores in Redis as: ep1:user:bob → '{"age": 25, "email": "bob@..."}'

# With NATS backend
nats_backend = NatsBackend(servers=["nats://localhost:4222"])
mapping = RemoteKVMapping(backend=nats_backend, entry_point="app1", sep=".")
```

---

## Key Implementation Decisions

1. **Conflict Resolution**: Use `_value` key for scalar/array nodes with
   children (not a separate data structure)
2. **Lazy Loading**: Fetch from KV on each access (no local cache by default;
   caching is optional)
3. **Separator Handling**: Use user-provided separator; validate that keys don't
   contain unescaped separators
4. **Thread Safety**: Backend operations are thread-safe by design (async); sync
   bridge handles event loop safety
5. **Error Handling**: Propagate backend errors (network issues, missing keys,
   etc.) to caller

---

## Testing Strategy

- Unit tests for KeyMapper (parsing entry_point + sep)
- Unit tests for conflict resolution logic (deepmerge vs `_value`)
- Integration tests with mock backends (in-memory dict)
- Integration tests with Redis and NATS (optional; use containers)
- Edge cases: empty mappings, deeply nested keys, special characters in keys

---

## Notes

- This design supports arbitrary nested JSON without data loss
- The `_value` pattern is transparent to users but should be documented
- Backend implementations can be added incrementally (start with in-memory for
  testing)
