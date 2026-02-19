import asyncio

import pytest

from kv_dict.async_bridge_poc import AsyncBridgePOC


def test_set_and_get_roundtrip() -> None:
    mapping = AsyncBridgePOC()
    try:
        mapping["user"] = {"age": 30, "roles": ["admin"]}
        assert mapping["user"] == {"age": 30, "roles": ["admin"]}
    finally:
        mapping.close()


def test_alias_methods_roundtrip() -> None:
    expected_count = 42
    mapping = AsyncBridgePOC()
    try:
        mapping.__set_item__("count", expected_count)
        assert mapping.__get_time__("count") == expected_count
    finally:
        mapping.close()


def test_get_missing_key_raises_keyerror() -> None:
    mapping = AsyncBridgePOC()
    try:
        with pytest.raises(KeyError):
            _ = mapping["missing"]
    finally:
        mapping.close()


def test_sync_bridge_works_inside_running_event_loop() -> None:
    async def scenario() -> None:
        mapping = AsyncBridgePOC()
        try:
            mapping["inside-loop"] = [1, 2, 3]
            assert mapping["inside-loop"] == [1, 2, 3]
        finally:
            mapping.close()

    asyncio.run(scenario())


def test_callable_returns_decoded_dict_snapshot() -> None:
    mapping = AsyncBridgePOC()
    try:
        mapping["count"] = 7
        mapping["user"] = {"name": "alice"}
        assert mapping() == {"count": 7, "user": {"name": "alice"}}
    finally:
        mapping.close()


def test_repr_and_str_are_dict_like() -> None:
    mapping = AsyncBridgePOC()
    try:
        mapping["count"] = 7
        mapping["user"] = {"name": "alice"}
        expected = "{'count': 7, 'user': {'name': 'alice'}}"
        assert repr(mapping) == expected
        assert str(mapping) == expected
    finally:
        mapping.close()


def test_setdefault_sets_when_missing() -> None:
    mapping = AsyncBridgePOC()
    try:
        result = mapping.setdefault("counter", 1)
        assert result == 1
        assert mapping["counter"] == 1
    finally:
        mapping.close()


def test_setdefault_returns_existing_without_overwrite() -> None:
    mapping = AsyncBridgePOC()
    try:
        mapping["counter"] = 1
        result = mapping.setdefault("counter", 999)
        assert result == 1
        assert mapping["counter"] == 1
    finally:
        mapping.close()


def test_delitem_iter_and_len() -> None:
    initial_length = 2
    remaining_length = 1
    mapping = AsyncBridgePOC()
    try:
        mapping["a"] = 1
        mapping["b"] = 2
        assert len(mapping) == initial_length
        assert set(iter(mapping)) == {"a", "b"}
        del mapping["a"]
        assert len(mapping) == remaining_length
        assert set(iter(mapping)) == {"b"}
    finally:
        mapping.close()


def test_nested_setdefault_then_inplace_update_persists() -> None:
    mapping = AsyncBridgePOC()
    try:
        versions = mapping.setdefault("versions", {})
        versions["bluesky"] = "1.13.1"
        assert mapping["versions"] == {"bluesky": "1.13.1"}
        assert mapping() == {"versions": {"bluesky": "1.13.1"}}
    finally:
        mapping.close()
