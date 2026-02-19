import pytest

from kv_dict.backends.in_memory import InMemoryAsyncBackend


@pytest.mark.asyncio
async def test_get_set_roundtrip() -> None:
    backend = InMemoryAsyncBackend()
    await backend.set("ep1:user:alice", '{"age": 30}')
    assert await backend.get("ep1:user:alice") == '{"age": 30}'


@pytest.mark.asyncio
async def test_get_missing_returns_none() -> None:
    backend = InMemoryAsyncBackend()
    assert await backend.get("missing") is None


@pytest.mark.asyncio
async def test_delete_existing_and_missing() -> None:
    backend = InMemoryAsyncBackend()
    await backend.set("ep1:user:alice", "A")
    await backend.delete("ep1:user:alice")
    assert await backend.get("ep1:user:alice") is None

    await backend.delete("ep1:user:alice")
    assert await backend.get("ep1:user:alice") is None


@pytest.mark.asyncio
async def test_list_keys_filters_by_prefix_and_sorts() -> None:
    backend = InMemoryAsyncBackend()
    await backend.set("ep1:z", "1")
    await backend.set("ep1:a", "2")
    await backend.set("ep2:x", "3")

    assert await backend.list_keys("ep1:") == ["ep1:a", "ep1:z"]
    assert await backend.list_keys("ep2:") == ["ep2:x"]
    assert await backend.list_keys("missing:") == []


@pytest.mark.asyncio
async def test_close_is_noop() -> None:
    backend = InMemoryAsyncBackend()
    await backend.set("k", "v")
    await backend.close()
    assert await backend.get("k") == "v"
