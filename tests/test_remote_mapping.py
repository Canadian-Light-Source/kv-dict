from kv_dict.backends.in_memory import InMemoryAsyncBackend
from kv_dict.mappings.remote import RemoteKVMapping


def test_remote_mapping_set_get_delete_and_len() -> None:
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping["user"] = {"alice": {"age": 30}}
        assert mapping["user"] == {"alice": {"age": 30}}
        assert len(mapping) == 1
        assert list(mapping) == ["user"]

        del mapping["user"]
        assert len(mapping) == 0
    finally:
        mapping.close()


def test_remote_mapping_reconstructs_nested_children_under_top_key() -> None:
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping._bridge.run(backend.set("ep1:main:sup1", '{"key": "value"}'))
        mapping._bridge.run(backend.set("ep1:main:sup2", "[1, 2, 3]"))

        assert mapping["main"] == {"sup1": {"key": "value"}, "sup2": [1, 2, 3]}
    finally:
        mapping.close()


def test_remote_mapping_conflict_scalar_leaf_plus_child() -> None:
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping._bridge.run(backend.set("ep1:main", "[1, 2, 3]"))
        mapping._bridge.run(backend.set("ep1:main:sub", '{"nested": true}'))

        assert mapping["main"] == {"_value": [1, 2, 3], "sub": {"nested": True}}
    finally:
        mapping.close()
