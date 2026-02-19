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

        # update test
        mapping["3d"] = {"x": [1, 2, 3], "y": [3, 2, 1], "z": [0, 0, 0]}
        mapping["3d"].update({"a": [1, 1, 1]})
        assert mapping["3d"]["a"] == [1, 1, 1]  # noqa: S101
        mapping["3d"].update({"a": [1, 1, 1]})
        mapping["3d"]["a"][0] = 99
        assert mapping["3d"]["a"] == [99, 1, 1]  # noqa: S101
    finally:
        mapping.close()


if __name__ == "__main__":
    main()
