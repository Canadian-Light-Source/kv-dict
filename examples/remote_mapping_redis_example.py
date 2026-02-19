"""Minimal example for RemoteKVMapping using a Redis-compatible backend."""

from kv_dict.backends.redis import RedisBackend
from kv_dict.mappings.remote import RemoteKVMapping


def main() -> None:
    """Run a basic set/get/iterate flow against Redis/Dragonfly."""
    backend = RedisBackend(url="redis://redis:6379/0")
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping["user"] = {"alice": {"age": 30}}
        print("user:", mapping["user"])
        user = mapping.get("user")
        print("user via get:", user)
        mapping.update({"user": {"alice": {"age": 42}}})
        print(f"{mapping=}")
    finally:
        mapping.close()


if __name__ == "__main__":
    main()
