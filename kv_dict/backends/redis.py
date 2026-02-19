"""Redis-compatible backend implementation."""

from __future__ import annotations

from inspect import isawaitable
from typing import Any


try:
    import redis.asyncio as redis_async
except ImportError:  # pragma: no cover - exercised when dependency is absent
    redis_async = None

from .protocol import Backend


def _normalize_string(value: str | bytes | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode()
    return value


class RedisBackend(Backend):
    """Redis-compatible async backend using ``redis.asyncio`` client APIs."""

    def __init__(self, url: str = "redis://localhost:6379/0", *, client: Any | None = None) -> None:
        """Create a backend from URL or an injected async client.

        Parameters
        ----------
        url
            Redis connection URL used when ``client`` is not provided.
        client
            Optional injected client with ``get/set/delete/scan_iter/aclose`` API.
        """
        self._url = url
        if client is not None:
            self._client = client
            return

        if redis_async is None:
            msg = "redis dependency is required for RedisBackend; install with `uv add redis`"
            raise RuntimeError(msg)

        self._client = redis_async.from_url(url, decode_responses=True)

    async def get(self, key: str) -> str | None:
        """Return raw value for key, or None when key does not exist."""
        return _normalize_string(await self._client.get(key))

    async def set(self, key: str, value: str) -> None:
        """Store raw value for key."""
        await self._client.set(key, value)

    async def delete(self, key: str) -> None:
        """Delete key if present."""
        await self._client.delete(key)

    async def list_keys(self, prefix: str) -> list[str]:
        """List all keys beginning with prefix in sorted order."""
        keys: list[str] = []
        async for key in self._client.scan_iter(match=f"{prefix}*"):
            normalized = _normalize_string(key)
            if normalized is not None:
                keys.append(normalized)
        return sorted(keys)

    async def close(self) -> None:
        """Release backend resources."""
        close_method = getattr(self._client, "aclose", None)
        if close_method is None:
            close_method = getattr(self._client, "close", None)
        if close_method is None:
            return

        maybe_awaitable = close_method()
        if isawaitable(maybe_awaitable):
            await maybe_awaitable
