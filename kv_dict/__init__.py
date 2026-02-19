"""kv-dict - KV bacjed dictionary, similar to RedisJSONDict"""

import importlib.metadata
import warnings

from ._version import version as __version__
from .async_bridge_poc import AsyncBridgePOC, DummyAsyncKVBackend
from .backends import Backend, InMemoryAsyncBackend
from .key_mapping import KeyMapper
from .mappings import RemoteKVMapping


__all__ = [
    "AsyncBridgePOC",
    "Backend",
    "DummyAsyncKVBackend",
    "InMemoryAsyncBackend",
    "KeyMapper",
    "RemoteKVMapping",
    "__version__",
]
