import pytest

from kv_dict.backends.redis import RedisBackend


class _FakeRedisClient:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.closed = False

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(self, key: str, value: str) -> None:
        self.store[key] = value

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)

    async def scan_iter(self, match: str):
        prefix = match.removesuffix("*")
        for key in sorted(self.store.keys()):
            if key.startswith(prefix):
                yield key

    async def aclose(self) -> None:
        self.closed = True


class _FakeBytesRedisClient(_FakeRedisClient):
    async def get(self, key: str) -> bytes | None:
        value = self.store.get(key)
        return value.encode() if value is not None else None

    async def scan_iter(self, match: str):
        prefix = match.removesuffix("*")
        for key in sorted(self.store.keys()):
            if key.startswith(prefix):
                yield key.encode()


class _FakeCloseOnlyClient:
    def __init__(self) -> None:
        self.closed = False

    async def get(self, key: str) -> str | None:
        return None

    async def set(self, key: str, value: str) -> None:
        return

    async def delete(self, key: str) -> None:
        return

    async def scan_iter(self, match: str):
        if False:
            yield match

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_redis_backend_get_set_delete_roundtrip() -> None:
    client = _FakeRedisClient()
    backend = RedisBackend(client=client)

    await backend.set("ep1:user", '{"alice": true}')
    assert await backend.get("ep1:user") == '{"alice": true}'

    await backend.delete("ep1:user")
    assert await backend.get("ep1:user") is None


@pytest.mark.asyncio
async def test_redis_backend_list_keys_filters_and_sorts() -> None:
    client = _FakeRedisClient()
    backend = RedisBackend(client=client)

    await backend.set("ep1:z", "1")
    await backend.set("ep1:a", "2")
    await backend.set("ep2:x", "3")

    assert await backend.list_keys("ep1:") == ["ep1:a", "ep1:z"]


@pytest.mark.asyncio
async def test_redis_backend_normalizes_bytes_from_client() -> None:
    client = _FakeBytesRedisClient()
    backend = RedisBackend(client=client)

    await backend.set("ep1:key", "value")
    assert await backend.get("ep1:key") == "value"
    assert await backend.list_keys("ep1:") == ["ep1:key"]


@pytest.mark.asyncio
async def test_redis_backend_close_prefers_aclose() -> None:
    client = _FakeRedisClient()
    backend = RedisBackend(client=client)

    await backend.close()
    assert client.closed is True


@pytest.mark.asyncio
async def test_redis_backend_close_falls_back_to_close() -> None:
    client = _FakeCloseOnlyClient()
    backend = RedisBackend(client=client)

    await backend.close()
    assert client.closed is True
