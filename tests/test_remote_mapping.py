from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Generator

import pytest
from hypothesis import given
from hypothesis import strategies as st

from kv_dict.backends.in_memory import InMemoryAsyncBackend
from kv_dict.mappings.remote import RemoteKVMapping, _AsyncLoopBridge, _WriteThroughDict, _WriteThroughList


_KEYS = st.text(min_size=1, max_size=20).filter(lambda value: ":" not in value)
_JSON_SCALARS = st.none() | st.booleans() | st.integers(min_value=-10_000, max_value=10_000) | st.text(max_size=30)
_JSON_VALUES = st.recursive(
    _JSON_SCALARS,
    lambda children: st.lists(children, max_size=4) | st.dictionaries(st.text(max_size=12), children, max_size=4),
    max_leaves=15,
)


@pytest.fixture
def mapping() -> Generator[RemoteKVMapping]:
    test_mapping = RemoteKVMapping(backend=InMemoryAsyncBackend(), entry_point="ep1", sep=":")
    try:
        yield test_mapping
    finally:
        test_mapping.close()


def test_remote_mapping_set_get_delete_and_len(mapping: RemoteKVMapping) -> None:
    mapping["user"] = {"alice": {"age": 30}}
    assert mapping["user"] == {"alice": {"age": 30}}
    assert len(mapping) == 1
    assert list(mapping) == ["user"]

    del mapping["user"]
    assert len(mapping) == 0


def test_remote_mapping_reconstructs_nested_children_under_top_key(mapping: RemoteKVMapping) -> None:
    mapping._bridge.run(mapping._backend.set("ep1:main:sup1", '{"key": "value"}'))
    mapping._bridge.run(mapping._backend.set("ep1:main:sup2", "[1, 2, 3]"))

    assert mapping["main"] == {"sup1": {"key": "value"}, "sup2": [1, 2, 3]}


def test_remote_mapping_conflict_scalar_leaf_plus_child(mapping: RemoteKVMapping) -> None:
    mapping._bridge.run(mapping._backend.set("ep1:main", "[1, 2, 3]"))
    mapping._bridge.run(mapping._backend.set("ep1:main:sub", '{"nested": true}'))

    assert mapping["main"] == {"_value": [1, 2, 3], "sub": {"nested": True}}


def test_remote_mapping_repr_and_str_are_dict_like(mapping: RemoteKVMapping) -> None:
    mapping["user"] = {"alice": {"age": 30}}
    expected = "{'user': {'alice': {'age': 30}}}"
    assert repr(mapping) == expected
    assert str(mapping) == expected


def test_remote_mapping_nested_update_persists(mapping: RemoteKVMapping) -> None:
    mapping["3d"] = {"x": [1, 2, 3], "y": [3, 2, 1], "z": [0, 0, 0]}
    mapping["3d"].update({"a": [1, 1, 1]})
    assert mapping["3d"]["a"] == [1, 1, 1]


def test_remote_mapping_nested_item_assignment_persists(mapping: RemoteKVMapping) -> None:
    mapping["user"] = {"alice": {"age": 30}}
    mapping["user"]["alice"]["email"] = "alice@example.com"
    assert mapping["user"]["alice"]["email"] == "alice@example.com"


def test_remote_mapping_nested_list_item_assignment_persists(mapping: RemoteKVMapping) -> None:
    mapping["a_list"] = {"a": [1, 1, 1]}
    mapping["a_list"]["a"][0] = 99
    assert mapping["a_list"]["a"] == [99, 1, 1]


def test_remote_mapping_top_level_list_item_assignment_persists(mapping: RemoteKVMapping) -> None:
    mapping["arr"] = [1, 2, 3]
    mapping["arr"][1] = 9
    assert mapping["arr"] == [1, 9, 3]


def test_remote_mapping_top_level_list_slice_assignment_persists(mapping: RemoteKVMapping) -> None:
    mapping["arr"] = [1, 2, 3, 4]
    mapping["arr"][0:2] = [9, 8]
    assert mapping["arr"] == [9, 8, 3, 4]


def test_remote_mapping_top_level_list_slice_assignment_non_list_raises_type_error(mapping: RemoteKVMapping) -> None:
    mapping["arr"] = [1, 2, 3, 4]

    with pytest.raises(TypeError, match="slice assignment requires a list value"):
        mapping["arr"][0:2] = (9, 8)


def test_remote_mapping_top_level_list_slice_getitem_returns_plain_list(mapping: RemoteKVMapping) -> None:
    mapping["arr"] = [1, {"nested": 2}, [3, 4], 5]

    result = mapping["arr"][0:3]

    assert result == [1, {"nested": 2}, [3, 4]]
    assert isinstance(result, list)


def test_remote_mapping_top_level_list_slice_delete_persists(mapping: RemoteKVMapping) -> None:
    mapping["arr"] = [1, 2, 3, 4, 5]

    del mapping["arr"][1:4]

    assert mapping["arr"] == [1, 5]


def test_remote_mapping_tuple_roundtrip(mapping: RemoteKVMapping) -> None:
    value = (1, "two", True)
    mapping["tuple_value"] = value
    assert mapping["tuple_value"] == [1, "two", True]


def test_remote_mapping_nested_tuple_containing_list_persists_updates(mapping: RemoteKVMapping) -> None:
    mapping["tuple_nested"] = ({"items": [1, 2]}, "tail")
    mapping["tuple_nested"][0]["items"][0] = 99
    assert mapping["tuple_nested"] == [{"items": [99, 2]}, "tail"]


def test_remote_mapping_returns_write_through_dict_wrappers(mapping: RemoteKVMapping) -> None:
    mapping["user"] = {"alice": {"age": 30}}
    result = mapping["user"]
    assert isinstance(result, _WriteThroughDict)
    assert isinstance(result["alice"], _WriteThroughDict)


@given(
    users=st.dictionaries(
        keys=st.text(min_size=1, max_size=12),
        values=st.dictionaries(keys=st.text(min_size=1, max_size=8), values=st.integers(), max_size=3),
        min_size=2,
        max_size=6,
    )
)
def test_remote_mapping_write_through_dict_iter_len_delete_persist(users: dict[str, dict[str, int]]) -> None:
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping["user"] = users
        user = mapping["user"]

        assert isinstance(user, _WriteThroughDict)
        assert set(user) == set(users)
        assert len(user) == len(users)

        removed_key = next(iter(users))
        del user[removed_key]

        expected = dict(users)
        del expected[removed_key]
        assert len(user) == len(expected)
        assert mapping["user"] == expected
    finally:
        mapping.close()


def test_remote_mapping_returns_write_through_list_wrappers(mapping: RemoteKVMapping) -> None:
    mapping["arr"] = [1, {"nested": [2, 3]}, [4, 5]]
    result = mapping["arr"]
    assert isinstance(result, _WriteThroughList)
    assert result[0] == 1
    assert isinstance(result[1], _WriteThroughDict)
    assert isinstance(result[1]["nested"], _WriteThroughList)
    assert isinstance(result[2], _WriteThroughList)


def test_write_through_dict_hash_raises_type_error(mapping: RemoteKVMapping) -> None:
    mapping["user"] = {"alice": {"age": 30}}
    user = mapping["user"]

    with pytest.raises(TypeError, match="unhashable type: '_WriteThroughDict'"):
        hash(user)


def test_write_through_list_hash_raises_type_error(mapping: RemoteKVMapping) -> None:
    mapping["arr"] = [1, 2, 3]
    arr = mapping["arr"]

    with pytest.raises(TypeError, match="unhashable type: '_WriteThroughList'"):
        hash(arr)


def test_async_loop_bridge_run_raises_runtime_error_when_loop_is_uninitialized() -> None:
    bridge = _AsyncLoopBridge()
    original_loop = bridge._loop
    try:
        bridge._loop = None
        with pytest.raises(RuntimeError, match="remote mapping async loop not initialized"):
            bridge.run(None)  # type: ignore[arg-type]
    finally:
        bridge._loop = original_loop
        bridge.close()


def test_remote_mapping_getitem_raises_key_error_when_backend_gets_none(mapping: RemoteKVMapping) -> None:
    mapping["user"] = {"alice": {"age": 30}}

    async def always_none(_key: str) -> str | None:
        return None

    mapping._backend.get = always_none  # type: ignore[method-assign]

    with pytest.raises(KeyError, match="user"):
        _ = mapping["user"]


def test_remote_mapping_delitem_missing_key_raises_key_error(mapping: RemoteKVMapping) -> None:
    with pytest.raises(KeyError, match="missing"):
        del mapping["missing"]


def test_remote_mapping_copy_returns_plain_detached_snapshot(mapping: RemoteKVMapping) -> None:
    expected_age = 30
    mapping["user"] = {"alice": {"age": expected_age}}
    snapshot = mapping.copy()

    assert snapshot == {"user": {"alice": {"age": expected_age}}}
    assert isinstance(snapshot, dict)

    snapshot["user"]["alice"]["age"] = 99
    assert mapping["user"]["alice"]["age"] == expected_age


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


def _assert_ior(mapping: RemoteKVMapping, operand: object, expected: dict[str, object]) -> None:
    result = mapping.__ior__(operand)
    assert result is mapping
    assert mapping.copy() == expected


def _assert_or(mapping: RemoteKVMapping, operand: object, expected: dict[str, object]) -> None:
    merged = mapping | operand
    assert merged == expected
    assert isinstance(merged, dict)


def test_remote_mapping_ior_with_mapping_operand(mapping: RemoteKVMapping) -> None:
    updated_a = 2
    updated_b = 3
    mapping["a"] = 1
    _assert_ior(mapping, {"a": updated_a, "b": updated_b}, {"a": updated_a, "b": updated_b})


def test_remote_mapping_ior_with_iterable_pairs_operand(mapping: RemoteKVMapping) -> None:
    _assert_ior(mapping, [("x", {"value": 1}), ("y", {"value": 2})], {"x": {"value": 1}, "y": {"value": 2}})


def test_remote_mapping_ior_invalid_operand_raises_type_error(mapping: RemoteKVMapping) -> None:
    with pytest.raises(TypeError):
        mapping |= 42


def test_remote_mapping_or_invalid_operand_raises_type_error(mapping: RemoteKVMapping) -> None:
    with pytest.raises(TypeError):
        _ = mapping | 42


def test_remote_mapping_or_returns_detached_merged_snapshot(mapping: RemoteKVMapping) -> None:
    mapping["a"] = {"value": 1}
    _assert_or(mapping, {"a": {"value": 2}, "b": {"value": 3}}, {"a": {"value": 2}, "b": {"value": 3}})

    assert mapping["a"] == {"value": 1}
    with pytest.raises(KeyError):
        _ = mapping["b"]


@given(
    base=st.dictionaries(keys=_KEYS, values=_JSON_VALUES, max_size=8),
    other=st.dictionaries(keys=_KEYS, values=_JSON_VALUES, max_size=8),
)
def test_remote_mapping_or_matches_dict_union_property(base: dict[str, object], other: dict[str, object]) -> None:
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping.update(base)
        expected = dict(base) | dict(other)
        _assert_or(mapping, other, expected)
        assert mapping.copy() == base
    finally:
        mapping.close()
