"""MutableMapping facade over an async backend with nested JSON support."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable, Iterator, MutableMapping
from typing import TYPE_CHECKING, Any, TypeVar, override

from kv_dict.key_mapping import KeyMapper, reconstruct_nested


if TYPE_CHECKING:
    from collections.abc import Coroutine
    from concurrent.futures import Future

    from kv_dict.backends import Backend


_T = TypeVar("_T")


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


class _WriteThroughDict(MutableMapping[str, Any]):
    """Dict-like wrapper that persists parent mapping on mutation."""

    def __init__(self, data: dict[str, Any], on_change: Callable[[dict[str, Any]], None]) -> None:
        super().__init__()
        self._data = data
        self._on_change = on_change

    def _persist(self) -> None:
        self._on_change(self.to_plain_dict())

    def _wrap_if_needed(self, value: Any) -> Any:
        if isinstance(value, dict):
            return _WriteThroughDict(value, lambda _updated: self._persist())
        return value

    @override
    def __getitem__(self, key: str) -> Any:
        return self._wrap_if_needed(self._data[key])

    @override
    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = _to_plain(value)
        self._persist()

    @override
    def __delitem__(self, key: str) -> None:
        del self._data[key]
        self._persist()

    @override
    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    @override
    def __len__(self) -> int:
        return len(self._data)

    @override
    def update(self, *args: Any, **kwargs: Any) -> None:
        updates = dict(*args, **kwargs)
        for key, value in updates.items():
            self._data[key] = _to_plain(value)
        self._persist()

    def to_plain_dict(self) -> dict[str, Any]:
        return {key: _to_plain(value) for key, value in self._data.items()}

    @override
    def __repr__(self) -> str:
        return repr(self.to_plain_dict())


class _AsyncLoopBridge:
    """Bridge sync calls to async backend operations on a dedicated loop."""

    def __init__(self) -> None:
        super().__init__()
        self._loop_ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread = threading.Thread(target=self._run, name="kv-dict-remote-mapping", daemon=True)
        self._thread.start()
        _ = self._loop_ready.wait()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._loop_ready.set()
        loop.run_forever()

    def run(self, coroutine: Coroutine[Any, Any, _T]) -> _T:
        if self._loop is None:
            msg = "remote mapping async loop not initialized"
            raise RuntimeError(msg)
        future: Future[Any] = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result()

    def close(self) -> None:
        if self._loop is None:
            return
        _ = self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


class RemoteKVMapping(MutableMapping[str, Any]):
    """Dict-like sync API over a flattened async KV backend."""

    def __init__(
        self,
        backend: Backend,
        entry_point: str,
        sep: str = ":",
        json_encoder: Callable[[Any], str] = json.dumps,
        json_decoder: Callable[[str], Any] = json.loads,
    ) -> None:
        super().__init__()
        self._backend = backend
        self._mapper = KeyMapper(entry_point=entry_point, sep=sep)
        self._json_encoder = json_encoder
        self._json_decoder = json_decoder
        self._bridge = _AsyncLoopBridge()

    def _as_dict(self) -> dict[str, Any]:
        return {key: self[key] for key in self}

    def _relevant_backend_keys(self, top_key: str) -> list[str]:
        base = self._mapper.full_key(top_key)
        keys = self._bridge.run(self._backend.list_keys(base))
        return [key for key in keys if key == base or key.startswith(f"{base}{self._mapper.sep}")]

    @override
    def __getitem__(self, key: str) -> Any:
        """Return nested object reconstructed from matching backend keys."""
        backend_keys = self._relevant_backend_keys(key)
        if not backend_keys:
            raise KeyError(key)

        root_key = self._mapper.full_key(key)
        pairs: list[tuple[tuple[str, ...], Any]] = []
        for backend_key in backend_keys:
            raw_value = self._bridge.run(self._backend.get(backend_key))
            if raw_value is None:
                continue
            decoded_value = self._json_decoder(raw_value)
            remainder = backend_key.removeprefix(root_key).removeprefix(self._mapper.sep)
            path = tuple(remainder.split(self._mapper.sep)) if remainder else ()
            pairs.append((path, decoded_value))

        if not pairs:
            raise KeyError(key)

        result = reconstruct_nested(pairs)
        if isinstance(result, dict):
            return _WriteThroughDict(result, lambda updated: self.__setitem__(key, updated))
        return result

    @override
    def __setitem__(self, key: str, value: Any) -> None:
        """Store a top-level value at entry_point:key."""
        backend_key = self._mapper.full_key(key)
        encoded_value = self._json_encoder(_to_plain(value))
        self._bridge.run(self._backend.set(backend_key, encoded_value))

    @override
    def __delitem__(self, key: str) -> None:
        """Delete a top-level key and all nested descendants."""
        backend_keys = self._relevant_backend_keys(key)
        if not backend_keys:
            raise KeyError(key)
        for backend_key in backend_keys:
            self._bridge.run(self._backend.delete(backend_key))

    @override
    def __iter__(self) -> Iterator[str]:
        """Iterate sorted top-level keys under the configured entry point."""
        keys = self._bridge.run(self._backend.list_keys(self._mapper.prefix))
        top_level_keys: set[str] = set()
        for backend_key in keys:
            if not self._mapper.matches(backend_key):
                continue
            parts = self._mapper.relative_parts(backend_key)
            top_level_keys.add(parts[0])
        return iter(sorted(top_level_keys))

    @override
    def __len__(self) -> int:
        """Return count of top-level keys."""
        return len(list(iter(self)))

    def copy(self) -> dict[str, Any]:
        """Return a detached plain-dict snapshot of current mapping contents."""
        return _to_plain(self._as_dict())

    @override
    def __repr__(self) -> str:
        """Represent mapping as a plain dictionary string."""
        return repr(self._as_dict())

    @override
    def __str__(self) -> str:
        """Render mapping as a plain dictionary string."""
        return str(self._as_dict())

    def close(self) -> None:
        """Close backend and bridge resources."""
        self._bridge.run(self._backend.close())
        self._bridge.close()
