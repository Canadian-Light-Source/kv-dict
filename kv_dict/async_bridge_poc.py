"""Proof-of-concept sync/async bridge for dict-like get/set operations."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable, Iterator, MutableMapping
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from concurrent.futures import Future


class _AsyncLoopThread:
    """Runs a dedicated asyncio event loop in a background thread."""

    def __init__(self) -> None:
        self._loop_ready = threading.Event()
        self._thread = threading.Thread(target=self._run, name="kv-dict-async-bridge", daemon=True)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread.start()
        self._loop_ready.wait()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._loop_ready.set()
        loop.run_forever()

    def run(self, coroutine: asyncio.Future[Any] | asyncio.Task[Any] | Any) -> Any:
        if self._loop is None:
            msg = "Async bridge loop is not initialized"
            raise RuntimeError(msg)

        future: Future[Any] = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result()

    def close(self) -> None:
        if self._loop is None:
            return

        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


class DummyAsyncKVBackend:
    """Simple async key-value backend for bridge tests."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        """Return a stored raw value or None when key is missing."""
        await asyncio.sleep(0)
        return self._store.get(key)

    async def set(self, key: str, value: str) -> None:
        """Store a raw value for a key."""
        await asyncio.sleep(0)
        self._store[key] = value

    async def delete(self, key: str) -> None:
        """Delete a key if present."""
        await asyncio.sleep(0)
        self._store.pop(key, None)

    async def snapshot(self) -> dict[str, str]:
        """Return a shallow copy of the underlying key/value store."""
        await asyncio.sleep(0)
        return dict(self._store)


class _WriteThroughDict(MutableMapping[str, Any]):
    """Dict-like wrapper that calls back on any mutation."""

    def __init__(self, data: dict[str, Any], on_change: Callable[[dict[str, Any]], None]) -> None:
        self._data = data
        self._on_change = on_change

    def _persist(self) -> None:
        self._on_change(self.to_plain_dict())

    def _wrap_if_needed(self, value: Any) -> Any:
        if isinstance(value, dict):
            return _WriteThroughDict(value, lambda _updated: self._persist())
        return value

    def __getitem__(self, key: str) -> Any:
        return self._wrap_if_needed(self._data[key])

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = _to_plain(value)
        self._persist()

    def __delitem__(self, key: str) -> None:
        del self._data[key]
        self._persist()

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def clear(self) -> None:
        self._data.clear()
        self._persist()

    def pop(self, key: str, default: Any = ...) -> Any:
        value = self._data.pop(key) if default is ... else self._data.pop(key, default)
        self._persist()
        return value

    def popitem(self) -> tuple[str, Any]:
        item = self._data.popitem()
        self._persist()
        return item

    def setdefault(self, key: str, default: Any = None) -> Any:
        if key not in self._data:
            self._data[key] = _to_plain(default)
            self._persist()
        return self._wrap_if_needed(self._data[key])

    def update(self, *args: Any, **kwargs: Any) -> None:
        updates = dict(*args, **kwargs)
        for key, value in updates.items():
            self._data[key] = _to_plain(value)
        self._persist()

    def to_plain_dict(self) -> dict[str, Any]:
        return {key: _to_plain(value) for key, value in self._data.items()}

    def __repr__(self) -> str:
        return repr(self.to_plain_dict())


def _to_plain(value: Any) -> Any:
    if isinstance(value, _WriteThroughDict):
        return value.to_plain_dict()
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_plain(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_to_plain(item) for item in value)
    return value


class AsyncBridgePOC(MutableMapping[str, Any]):
    """Minimal sync API over an async backend using a dedicated loop thread."""

    def __init__(
        self,
        backend: DummyAsyncKVBackend | None = None,
        *,
        json_encoder: Callable[[Any], str] = json.dumps,
        json_decoder: Callable[[str], Any] = json.loads,
    ) -> None:
        self._backend = backend if backend is not None else DummyAsyncKVBackend()
        self._json_encoder = json_encoder
        self._json_decoder = json_decoder
        self._executor = _AsyncLoopThread()

    def __getitem__(self, key: str) -> Any:
        """Fetch and decode a value for key."""
        raw_value = self._executor.run(self._backend.get(key))
        if raw_value is None:
            raise KeyError(key)
        decoded = self._json_decoder(raw_value)
        if isinstance(decoded, dict):
            return _WriteThroughDict(decoded, lambda updated: self.__setitem__(key, updated))
        return decoded

    def __setitem__(self, key: str, value: Any) -> None:
        """Encode and store value for key."""
        encoded_value = self._json_encoder(value)
        self._executor.run(self._backend.set(key, encoded_value))

    def __delitem__(self, key: str) -> None:
        """Delete key from backend."""
        if key not in self:
            raise KeyError(key)
        self._executor.run(self._backend.delete(key))

    def __iter__(self) -> Iterator[str]:
        """Iterate over top-level keys."""
        return iter(self().keys())

    def __len__(self) -> int:
        """Return number of top-level keys."""
        return len(self())

    def setdefault(self, key: str, default: Any = None) -> Any:
        """Return existing key value or set and return default."""
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return self[key]

    def __get_time__(self, key: str) -> Any:
        """Alias requested for POC verification."""
        return self.__getitem__(key)

    def __set_item__(self, key: str, value: Any) -> None:
        """Alias requested for POC verification."""
        self.__setitem__(key, value)

    def __call__(self) -> dict[str, Any]:
        """Hacky callable form returning all keys as a decoded dict."""
        raw_items = self._executor.run(self._backend.snapshot())
        return {key: self._json_decoder(raw_value) for key, raw_value in raw_items.items()}

    def __repr__(self) -> str:
        """Represent mapping as a plain dict string."""
        return repr(self())

    def __str__(self) -> str:
        """Render mapping as a plain dict string."""
        return str(self())

    def close(self) -> None:
        """Stop background loop resources used by the bridge."""
        self._executor.close()
