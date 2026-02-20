from hypothesis import given
from hypothesis import strategies as st

from kv_dict.backends.in_memory import InMemoryAsyncBackend
from kv_dict.mappings.remote import RemoteKVMapping


_KEYS = st.text(min_size=1, max_size=20).filter(lambda value: ":" not in value)
_JSON_SCALARS = st.none() | st.booleans() | st.integers(min_value=-10_000, max_value=10_000) | st.text(max_size=30)
_JSON_VALUES = st.recursive(
    _JSON_SCALARS,
    lambda children: st.lists(children, max_size=4) | st.dictionaries(st.text(max_size=12), children, max_size=4),
    max_leaves=15,
)


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


def test_remote_mapping_repr_and_str_are_dict_like() -> None:
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping["user"] = {"alice": {"age": 30}}
        expected = "{'user': {'alice': {'age': 30}}}"
        assert repr(mapping) == expected
        assert str(mapping) == expected
    finally:
        mapping.close()


def test_remote_mapping_nested_update_persists() -> None:
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping["3d"] = {"x": [1, 2, 3], "y": [3, 2, 1], "z": [0, 0, 0]}
        mapping["3d"].update({"a": [1, 1, 1]})
        assert mapping["3d"]["a"] == [1, 1, 1]
    finally:
        mapping.close()


def test_remote_mapping_nested_item_assignment_persists() -> None:
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping["user"] = {"alice": {"age": 30}}
        mapping["user"]["alice"]["email"] = "alice@example.com"
        assert mapping["user"]["alice"]["email"] == "alice@example.com"
    finally:
        mapping.close()


def test_remote_mapping_copy_returns_plain_detached_snapshot() -> None:
    expected_age = 30
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping["user"] = {"alice": {"age": expected_age}}
        snapshot = mapping.copy()

        assert snapshot == {"user": {"alice": {"age": expected_age}}}
        assert isinstance(snapshot, dict)

        snapshot["user"]["alice"]["age"] = 99
        assert mapping["user"]["alice"]["age"] == expected_age
    finally:
        mapping.close()


@given(key=_KEYS, value=_JSON_VALUES)
def test_remote_mapping_roundtrip_property(key: str, value: object) -> None:
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping[key] = value
        assert mapping[key] == value
    finally:
        mapping.close()


@given(payload=st.dictionaries(keys=_KEYS, values=_JSON_VALUES, max_size=10))
def test_remote_mapping_multi_key_property(payload: dict[str, object]) -> None:
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping.update(payload)
        assert len(mapping) == len(payload)
        assert list(mapping) == sorted(payload.keys())
        for key, expected in payload.items():
            assert mapping[key] == expected
    finally:
        mapping.close()
