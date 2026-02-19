"""kv-dict - KV bacjed dictionary, similar to RedisJSONDict"""

import importlib.metadata
import warnings

from ._version import version as __version__
from .backends import Backend, InMemoryAsyncBackend, RedisBackend
from .key_mapping import KeyMapper
from .mappings import RemoteKVMapping


__all__ = ["Backend", "InMemoryAsyncBackend", "KeyMapper", "RedisBackend", "RemoteKVMapping", "__version__"]
