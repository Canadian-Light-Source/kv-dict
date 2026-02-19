"""MutableMapping facade over an async backend with nested JSON support."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable, Iterator, MutableMapping
from typing import TYPE_CHECKING, Any

from kv_dict.key_mapping import KeyMapper, reconstruct_nested


if TYPE_CHECKING:
    from concurrent.futures import Future

    from kv_dict.backends import Backend


class _AsyncLoopBridge:
    """Bridge sync calls to async backend operations on a dedicated loop."""

    def __init__(self) -> None:
        self._loop_ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread = threading.Thread(target=self._run, name="kv-dict-remote-mapping", daemon=True)
        self._thread.start()
        self._loop_ready.wait()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._loop_ready.set()
        loop.run_forever()

    def run(self, coroutine: Any) -> Any:
        if self._loop is None:
            msg = "remote mapping async loop not initialized"
            raise RuntimeError(msg)
        future: Future[Any] = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result()

    def close(self) -> None:
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._loop.stop)
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
        self._backend = backend
        self._mapper = KeyMapper(entry_point=entry_point, sep=sep)
        self._json_encoder = json_encoder
        self._json_decoder = json_decoder
        self._bridge = _AsyncLoopBridge()

    def _relevant_backend_keys(self, top_key: str) -> list[str]:
        base = self._mapper.full_key(top_key)
        keys = self._bridge.run(self._backend.list_keys(base))
        return [key for key in keys if key == base or key.startswith(f"{base}{self._mapper.sep}")]

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

        return reconstruct_nested(pairs)

    def __setitem__(self, key: str, value: Any) -> None:
        """Store a top-level value at entry_point:key."""
        backend_key = self._mapper.full_key(key)
        encoded_value = self._json_encoder(value)
        self._bridge.run(self._backend.set(backend_key, encoded_value))

    def __delitem__(self, key: str) -> None:
        """Delete a top-level key and all nested descendants."""
        backend_keys = self._relevant_backend_keys(key)
        if not backend_keys:
            raise KeyError(key)
        for backend_key in backend_keys:
            self._bridge.run(self._backend.delete(backend_key))

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

    def __len__(self) -> int:
        """Return count of top-level keys."""
        return len(list(iter(self)))

    def close(self) -> None:
        """Close backend and bridge resources."""
        self._bridge.run(self._backend.close())
        self._bridge.close()
