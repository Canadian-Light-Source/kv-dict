"""kv-dict - KV bacjed dictionary, similar to RedisJSONDict"""

import importlib.metadata
import warnings

from ._version import version as __version__
from .backends import Backend, InMemoryAsyncBackend, NatsBackend, PostgresBackend, RedisBackend
from .key_mapping import KeyMapper
from .mappings import RemoteKVMapping


__all__ = [
    "Backend",
    "InMemoryAsyncBackend",
    "KeyMapper",
    "NatsBackend",
    "PostgresBackend",
    "RedisBackend",
    "RemoteKVMapping",
    "__version__",
]
