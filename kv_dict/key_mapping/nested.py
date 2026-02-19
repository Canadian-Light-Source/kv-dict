"""Nested structure reconstruction from flattened key paths."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Iterable


def _deep_merge(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    merged = dict(left)
    for key, value in right.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        elif key in merged and not isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = {"_value": merged[key], **value}
        else:
            merged[key] = value
    return merged


def reconstruct_nested(items: Iterable[tuple[tuple[str, ...], Any]]) -> Any:
    """Reconstruct a nested object from path/value pairs.

    Each item consists of a tuple path and a decoded Python value.
    The empty tuple path represents the leaf value at the current root.
    """
    path_to_value = dict(items)

    def build(path: tuple[str, ...]) -> Any:
        leaf_exists = path in path_to_value
        leaf_value = path_to_value.get(path)

        grouped_children: dict[str, list[tuple[str, ...]]] = defaultdict(list)
        for candidate_path in path_to_value:
            if len(candidate_path) <= len(path):
                continue
            if candidate_path[: len(path)] != path:
                continue
            child_segment = candidate_path[len(path)]
            grouped_children[child_segment].append(candidate_path)

        if not grouped_children:
            if not leaf_exists:
                msg = "cannot build path with no leaf and no children"
                raise ValueError(msg)
            return leaf_value

        children_obj = {
            child_segment: build((*path, child_segment)) for child_segment in sorted(grouped_children.keys())
        }

        if not leaf_exists:
            return children_obj

        if isinstance(leaf_value, dict):
            return _deep_merge(leaf_value, children_obj)

        return {"_value": leaf_value, **children_obj}

    return build(())
