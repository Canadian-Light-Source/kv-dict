"""Backend interface definitions."""

from __future__ import annotations

from abc import ABC, abstractmethod


class Backend(ABC):
    """Async key-value backend interface."""

    @abstractmethod
    async def get(self, key: str) -> str | None:
        """Return raw value for key, or None when key does not exist."""

    @abstractmethod
    async def set(self, key: str, value: str) -> None:
        """Store raw value for key."""

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete key if present."""

    @abstractmethod
    async def list_keys(self, prefix: str) -> list[str]:
        """List all keys beginning with prefix."""

    @abstractmethod
    async def close(self) -> None:
        """Close any backend resources."""
