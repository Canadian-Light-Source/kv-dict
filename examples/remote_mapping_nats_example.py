"""Minimal example for RemoteKVMapping using a NATS JetStream KV backend."""

from kv_dict.backends.nats import NatsBackend
from kv_dict.mappings.remote import RemoteKVMapping


def main() -> None:
    """Run a basic set/get/iterate flow against NATS JetStream KV."""
    backend = NatsBackend(url="nats://nats:4222", bucket="kv_dict", create_bucket=False)
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep=".")
    try:
        print(f"start: {mapping=}")
        mapping["user"] = {"alice": {"age": 30}}
        print("user:", mapping["user"])
        print("keys:", list(mapping))
        print("len:", len(mapping))

        mapping["user"]["alice"]["age"] = 31
        print("updated user:", mapping["user"])

        del mapping["user"]
        print("after delete keys:", list(mapping))
    finally:
        mapping.close()


if __name__ == "__main__":
    main()
