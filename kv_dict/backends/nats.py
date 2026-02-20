"""NATS JetStream KV backend implementation."""

from __future__ import annotations

from typing import Any, override


try:
    import nats as nats_module
except ImportError:  # pragma: no cover - exercised when dependency is absent
    nats_module = None

from .protocol import Backend


_NOT_FOUND_ERROR_NAMES = {"BucketNotFoundError", "KeyNotFoundError", "NoKeysError"}


def _is_not_found_error(error: Exception) -> bool:
    return error.__class__.__name__ in _NOT_FOUND_ERROR_NAMES


class NatsBackend(Backend):
    """NATS JetStream KV backend.

    The backend uses an existing KV bucket by default.
    Set ``create_bucket=True`` to allow creating it when missing.
    """

    def __init__(
        self,
        url: str = "nats://nats:4222",
        bucket: str = "kv_dict",
        *,
        client: Any | None = None,
        create_bucket: bool = False,
    ) -> None:
        """Create a backend using a NATS URL or injected client.

        Parameters
        ----------
        url
            NATS server URL used when ``client`` is not provided.
        bucket
            JetStream KV bucket name.
        client
            Optional injected connected NATS client with ``jetstream`` API.
        create_bucket
            When True, creates bucket if missing. Defaults to False.
        """
        super().__init__()
        self._url = url
        self._bucket_name = bucket
        self._client = client
        self._create_bucket = create_bucket
        self._kv: Any | None = None

    async def _ensure_kv(self) -> Any:
        if self._kv is not None:
            return self._kv

        if self._client is None:
            if nats_module is None:
                msg = "nats-py dependency is required for NatsBackend; install with `uv add nats-py`"
                raise RuntimeError(msg)
            connect = getattr(nats_module, "connect", None)
            if connect is None:
                msg = "nats.connect is unavailable in installed nats-py package"
                raise RuntimeError(msg)
            self._client = await connect(servers=[self._url])

        jetstream = self._client.jetstream()

        try:
            self._kv = await jetstream.key_value(self._bucket_name)
        except Exception as error:
            if _is_not_found_error(error) and self._create_bucket:
                self._kv = await jetstream.create_key_value(bucket=self._bucket_name)
            else:
                msg = (
                    f"jetstream KV bucket '{self._bucket_name}' is not available; "
                    "create it first or initialize with create_bucket=True"
                )
                raise RuntimeError(msg) from error

        return self._kv

    @override
    async def get(self, key: str) -> str | None:
        """Return raw value for key, or None when key does not exist."""
        kv = await self._ensure_kv()
        try:
            entry = await kv.get(key)
        except Exception as error:
            if _is_not_found_error(error):
                return None
            raise

        value = entry.value
        if isinstance(value, bytes):
            return value.decode()
        return value

    @override
    async def set(self, key: str, value: str) -> None:
        """Store raw value for key."""
        kv = await self._ensure_kv()
        await kv.put(key, value.encode())

    @override
    async def delete(self, key: str) -> None:
        """Delete key if present."""
        kv = await self._ensure_kv()
        await kv.delete(key)

    @override
    async def list_keys(self, prefix: str) -> list[str]:
        """List all keys beginning with prefix in sorted order."""
        kv = await self._ensure_kv()
        try:
            keys = await kv.keys()
        except Exception as error:
            if _is_not_found_error(error):
                return []
            raise

        if not keys:
            return []
        return sorted([key for key in keys if key.startswith(prefix)])

    @override
    async def close(self) -> None:
        """Close NATS client resources."""
        if self._client is None:
            return
        await self._client.close()
