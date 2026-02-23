import pytest

from kv_dict.backends import postgres as postgres_module
from kv_dict.backends.postgres import PostgresBackend


class UndefinedTableError(Exception):
    pass


class _FakePostgresClient:
    def __init__(self, *, table_exists: bool = True) -> None:
        super().__init__()
        self.store: dict[str, str] = {}
        self.closed = False
        self.table_exists = table_exists

    async def fetchrow(self, _query: str, key: str) -> dict[str, str] | None:
        if not self.table_exists:
            raise UndefinedTableError
        if key not in self.store:
            return None
        return {"v": self.store[key]}

    async def fetch(self, _query: str, pattern: str) -> list[dict[str, str]]:
        if not self.table_exists:
            raise UndefinedTableError
        prefix = pattern.removesuffix("%")
        return [{"k": key} for key in sorted(self.store) if key.startswith(prefix)]

    async def execute(self, query: str, *args: str) -> str:
        if query.startswith("CREATE TABLE IF NOT EXISTS"):
            self.table_exists = True
            return "CREATE TABLE"

        if not self.table_exists:
            raise UndefinedTableError

        if query.startswith("INSERT INTO"):
            key, value = args
            self.store[key] = value
            return "INSERT 0 1"
        if query.startswith("DELETE FROM"):
            key = args[0]
            _ = self.store.pop(key, None)
            return "DELETE 1"

        return "OK"

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_postgres_backend_get_set_delete_roundtrip() -> None:
    backend = PostgresBackend(client=_FakePostgresClient(table_exists=True))

    await backend.set("ep1:user", '{"alice": true}')
    assert await backend.get("ep1:user") == '{"alice": true}'

    await backend.delete("ep1:user")
    assert await backend.get("ep1:user") is None


@pytest.mark.asyncio
async def test_postgres_backend_list_keys_filters_and_sorts() -> None:
    backend = PostgresBackend(client=_FakePostgresClient(table_exists=True))

    await backend.set("ep1:z", "1")
    await backend.set("ep1:a", "2")
    await backend.set("ep2:x", "3")

    assert await backend.list_keys("ep1:") == ["ep1:a", "ep1:z"]


@pytest.mark.asyncio
async def test_postgres_backend_missing_table_raises_runtime_error_when_create_disabled() -> None:
    backend = PostgresBackend(client=_FakePostgresClient(table_exists=False), create_table=False)

    with pytest.raises(RuntimeError, match="postgres table 'kv_store' is not available"):
        _ = await backend.get("ep1:user")


@pytest.mark.asyncio
async def test_postgres_backend_missing_table_can_be_created_when_enabled() -> None:
    backend = PostgresBackend(client=_FakePostgresClient(table_exists=False), create_table=True)

    await backend.set("ep1:user", "value")
    assert await backend.get("ep1:user") == "value"


@pytest.mark.asyncio
async def test_postgres_backend_close_closes_client() -> None:
    client = _FakePostgresClient(table_exists=True)
    backend = PostgresBackend(client=client)

    await backend.close()
    assert client.closed is True


@pytest.mark.asyncio
async def test_postgres_backend_requires_dependency_without_injected_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(postgres_module, "asyncpg_module", None)
    backend = PostgresBackend(client=None)

    with pytest.raises(RuntimeError, match="asyncpg dependency is required"):
        _ = await backend.get("ep1:user")


def test_postgres_backend_rejects_invalid_table_identifier() -> None:
    with pytest.raises(ValueError, match="valid unquoted SQL identifier"):
        _ = PostgresBackend(table="kv-store")
