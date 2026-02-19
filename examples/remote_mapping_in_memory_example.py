"""Minimal example for RemoteKVMapping using the in-memory backend."""

from kv_dict.backends.in_memory import InMemoryAsyncBackend
from kv_dict.mappings.remote import RemoteKVMapping


def main() -> None:
    """Run a basic set/get/delete flow on the in-memory backend."""
    backend = InMemoryAsyncBackend()
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=":")
    try:
        mapping["user"] = {"alice": {"age": 30}}
        print(f"{mapping=}")
        print("user:", mapping["user"])
        print("keys:", list(mapping))
        print("len:", len(mapping))

        del mapping["user"]
        print("after delete keys:", list(mapping))
    finally:
        mapping.close()


if __name__ == "__main__":
    main()
