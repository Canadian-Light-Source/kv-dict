"""Minimal example for RemoteKVMapping using PostgreSQL."""

from kv_dict.backends.postgres import PostgresBackend
from kv_dict.mappings.remote import RemoteKVMapping


def main() -> None:
    """Run a basic set/get/iterate flow against PostgreSQL."""
    backend = PostgresBackend(
        dsn="postgresql://postgres:postgres@postgres:5432/postgres", table="kv_store", create_table=True
    )
    mapping = RemoteKVMapping(backend=backend, entry_point="ep1", sep="/")
    try:
        print(f"start: {mapping=}")
        mapping["user"] = {"alice": {"age": 30}}
        print("user:", mapping["user"])
        user = mapping.get("user")
        print("user via get:", user)
        mapping.update({"user": {"alice": {"age": 42}}})

        # update test
        mapping["3d"] = {"x": [1, 2, 3], "y": [3, 2, 1], "z": [0, 0, 0]}
        mapping["3d"].update({"a": [1, 1, 1]})
        assert mapping["3d"]["a"] == [1, 1, 1]  # noqa: S101
        mapping["3d"].update({"a": [1, 1, 1]})
        print(f"before: {mapping=}")
        mapping["3d"]["a"][0] = 99
        assert mapping["3d"]["a"] == [99, 1, 1]  # noqa: S101
        print(f"after:  {mapping=}")
    finally:
        mapping.close()


if __name__ == "__main__":
    main()
