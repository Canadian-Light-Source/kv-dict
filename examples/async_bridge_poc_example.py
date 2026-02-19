from bluesky import RunEngine

from kv_dict.async_bridge_poc import AsyncBridgePOC


def main() -> None:
    """Run a quick AsyncBridgePOC interoperability example."""
    mapping = AsyncBridgePOC()
    try:
        mapping.__set_item__("count", 7)
        print("count via alias:", mapping.__get_time__("count"))

        mapping["user"] = {"name": "alice", "roles": ["admin", "editor"]}
        print("user via __getitem__:", mapping["user"])

        run_engine = RunEngine(mapping)
        print(f"RunEngine metadata: {run_engine.md=}")
    finally:
        mapping.close()


if __name__ == "__main__":
    main()
