"""PostgreSQL backend implementation."""

from __future__ import annotations

import importlib
import re
from typing import Any, override


try:
    asyncpg_module = importlib.import_module("asyncpg")
except ImportError:  # pragma: no cover - exercised when dependency is absent
    asyncpg_module = None

from .protocol import Backend


_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _normalize_text(value: str | bytes | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode()
    return value


def _is_missing_table_error(error: Exception) -> bool:
    return error.__class__.__name__ == "UndefinedTableError"


class PostgresBackend(Backend):
    """PostgreSQL async backend for raw key/value text storage."""

    def __init__(
        self,
        dsn: str = "postgresql://postgres:postgres@postgres:5432/postgres",
        table: str = "kv_store",
        *,
        client: Any | None = None,
        create_table: bool = False,
    ) -> None:
        """Create a backend from DSN or an injected async client.

        Parameters
        ----------
        dsn
            PostgreSQL DSN used when ``client`` is not provided.
        table
            Table name that stores ``k`` and ``v`` columns.
        client
            Optional injected async client with ``fetchrow/fetch/execute/close`` API.
        create_table
            When True, creates the table if missing. Defaults to False.
        """
        super().__init__()
        if not _VALID_IDENTIFIER.fullmatch(table):
            msg = "table must be a valid unquoted SQL identifier"
            raise ValueError(msg)

        self._dsn = dsn
        self._table = table
        self._client = client
        self._create_table = create_table
        self._is_initialized = False

    def _missing_table_error(self) -> RuntimeError:
        msg = f"postgres table '{self._table}' is not available; create it first or initialize with create_table=True"
        return RuntimeError(msg)

    def _client_or_raise(self) -> Any:
        if self._client is None:
            msg = "postgres client is not initialized"
            raise RuntimeError(msg)
        return self._client

    async def _ensure_initialized(self) -> None:
        if self._is_initialized:
            return

        if self._client is None:
            if asyncpg_module is None:
                msg = "asyncpg dependency is required for PostgresBackend; install with `uv add asyncpg`"
                raise RuntimeError(msg)
            self._client = await asyncpg_module.connect(self._dsn)

        if self._create_table:
            await self._client.execute(
                f'CREATE TABLE IF NOT EXISTS "{self._table}" (k TEXT PRIMARY KEY, v TEXT NOT NULL)'
            )

        self._is_initialized = True

    @override
    async def get(self, key: str) -> str | None:
        """Return raw value for key, or None when key does not exist."""
        await self._ensure_initialized()
        client = self._client_or_raise()
        try:
            row = await client.fetchrow(f'SELECT v FROM "{self._table}" WHERE k = $1', key)  # noqa: S608
        except Exception as error:
            if _is_missing_table_error(error):
                raise self._missing_table_error() from error
            raise

        if row is None:
            return None

        value = row["v"] if isinstance(row, dict) else row[0]
        return _normalize_text(value)

    @override
    async def set(self, key: str, value: str) -> None:
        """Store raw value for key."""
        await self._ensure_initialized()
        client = self._client_or_raise()
        try:
            await client.execute(
                (
                    f'INSERT INTO "{self._table}" (k, v) VALUES ($1, $2) '  # noqa: S608
                    "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"
                ),
                key,
                value,
            )
        except Exception as error:
            if _is_missing_table_error(error):
                raise self._missing_table_error() from error
            raise

    @override
    async def delete(self, key: str) -> None:
        """Delete key if present."""
        await self._ensure_initialized()
        client = self._client_or_raise()
        try:
            await client.execute(f'DELETE FROM "{self._table}" WHERE k = $1', key)  # noqa: S608
        except Exception as error:
            if _is_missing_table_error(error):
                raise self._missing_table_error() from error
            raise

    @override
    async def list_keys(self, prefix: str) -> list[str]:
        """List all keys beginning with prefix in sorted order."""
        await self._ensure_initialized()
        client = self._client_or_raise()
        try:
            rows = await client.fetch(
                f'SELECT k FROM "{self._table}" WHERE k LIKE $1 ORDER BY k ASC',  # noqa: S608
                f"{prefix}%",
            )
        except Exception as error:
            if _is_missing_table_error(error):
                raise self._missing_table_error() from error
            raise

        return [_normalize_text(row["k"] if isinstance(row, dict) else row[0]) or "" for row in rows]

    @override
    async def close(self) -> None:
        """Release backend resources."""
        if self._client is None:
            return
        _ = await self._client.close()
