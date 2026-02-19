"""In-memory backend implementation."""

from __future__ import annotations

import asyncio

from .protocol import Backend


class InMemoryAsyncBackend(Backend):
    """Simple in-memory backend for local development and tests."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> str | None:
        """Return raw value for key, or None when key does not exist."""
        async with self._lock:
            return self._store.get(key)

    async def set(self, key: str, value: str) -> None:
        """Store raw value for key."""
        async with self._lock:
            self._store[key] = value

    async def delete(self, key: str) -> None:
        """Delete key if present."""
        async with self._lock:
            self._store.pop(key, None)

    async def list_keys(self, prefix: str) -> list[str]:
        """List all keys beginning with prefix in sorted order."""
        async with self._lock:
            matching = [key for key in self._store if key.startswith(prefix)]
        return sorted(matching)

    async def close(self) -> None:
        """Release backend resources."""
        return
