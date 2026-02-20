import pytest

from kv_dict.backends import nats as nats_module
from kv_dict.backends.nats import NatsBackend


class BucketNotFoundError(Exception):
    pass


class KeyNotFoundError(Exception):
    pass


class NoKeysError(Exception):
    pass


class _FakeEntry:
    def __init__(self, value: bytes | str) -> None:
        self.value = value
        super().__init__()


class _FakeKVBucket:
    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        super().__init__()

    async def get(self, key: str) -> _FakeEntry:
        if key not in self.store:
            raise KeyNotFoundError
        return _FakeEntry(self.store[key])

    async def put(self, key: str, value: bytes) -> None:
        self.store[key] = value

    async def delete(self, key: str) -> None:
        _ = self.store.pop(key, None)

    async def keys(self) -> list[str]:
        if not self.store:
            raise NoKeysError
        return list(self.store)


class _FakeJetStream:
    def __init__(self, buckets: dict[str, _FakeKVBucket]) -> None:
        self.buckets = buckets
        super().__init__()

    async def key_value(self, bucket: str) -> _FakeKVBucket:
        if bucket not in self.buckets:
            raise BucketNotFoundError
        return self.buckets[bucket]

    async def create_key_value(self, bucket: str) -> _FakeKVBucket:
        created = _FakeKVBucket()
        self.buckets[bucket] = created
        return created


class _FakeNatsClient:
    def __init__(self, buckets: dict[str, _FakeKVBucket] | None = None) -> None:
        super().__init__()
        self._js = _FakeJetStream({} if buckets is None else buckets)
        self.closed = False

    def jetstream(self) -> _FakeJetStream:
        return self._js

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_nats_backend_get_set_delete_roundtrip_existing_bucket() -> None:
    bucket = _FakeKVBucket()
    backend = NatsBackend(client=_FakeNatsClient({"kv_dict": bucket}), bucket="kv_dict")

    await backend.set("ep1:user", '{"alice": true}')
    assert await backend.get("ep1:user") == '{"alice": true}'

    await backend.delete("ep1:user")
    assert await backend.get("ep1:user") is None


@pytest.mark.asyncio
async def test_nats_backend_list_keys_filters_and_sorts() -> None:
    bucket = _FakeKVBucket()
    backend = NatsBackend(client=_FakeNatsClient({"kv_dict": bucket}), bucket="kv_dict")

    await backend.set("ep1:z", "1")
    await backend.set("ep1:a", "2")
    await backend.set("ep2:x", "3")

    assert await backend.list_keys("ep1:") == ["ep1:a", "ep1:z"]


@pytest.mark.asyncio
async def test_nats_backend_missing_bucket_raises_runtime_error_when_create_disabled() -> None:
    backend = NatsBackend(client=_FakeNatsClient({}), bucket="kv_dict", create_bucket=False)

    with pytest.raises(RuntimeError, match="jetstream KV bucket 'kv_dict' is not available"):
        _ = await backend.get("ep1:user")


@pytest.mark.asyncio
async def test_nats_backend_missing_bucket_can_be_created_when_enabled() -> None:
    backend = NatsBackend(client=_FakeNatsClient({}), bucket="kv_dict", create_bucket=True)

    await backend.set("ep1:user", "value")
    assert await backend.get("ep1:user") == "value"


@pytest.mark.asyncio
async def test_nats_backend_close_closes_client() -> None:
    client = _FakeNatsClient({"kv_dict": _FakeKVBucket()})
    backend = NatsBackend(client=client, bucket="kv_dict")

    await backend.close()
    assert client.closed is True


@pytest.mark.asyncio
async def test_nats_backend_close_without_client_is_noop() -> None:
    backend = NatsBackend(client=None)
    await backend.close()


@pytest.mark.asyncio
async def test_nats_backend_requires_dependency_without_injected_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(nats_module, "nats_module", None)
    backend = NatsBackend(client=None)

    with pytest.raises(RuntimeError, match="nats-py dependency is required"):
        _ = await backend.get("ep1:user")
