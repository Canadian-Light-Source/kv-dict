"""Backend contracts and implementations."""

from .in_memory import InMemoryAsyncBackend
from .protocol import Backend


__all__ = ["Backend", "InMemoryAsyncBackend"]
