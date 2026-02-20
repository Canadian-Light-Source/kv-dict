import pytest

from kv_dict.key_mapping.mapper import KeyMapper
from kv_dict.key_mapping.nested import reconstruct_nested


def test_key_mapper_full_key_and_relative_parts() -> None:
    mapper = KeyMapper(entry_point="ep1", sep=":")
    assert mapper.full_key("main", "sup1") == "ep1:main:sup1"
    assert mapper.relative_parts("ep1:main:sup1") == ("main", "sup1")


def test_key_mapper_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="entry_point must not be empty"):
        _ = KeyMapper(entry_point="", sep=":")
    with pytest.raises(ValueError, match="sep must not be empty"):
        _ = KeyMapper(entry_point="ep1", sep="")
    with pytest.raises(ValueError, match="entry_point must not contain separator"):
        _ = KeyMapper(entry_point="ep:1", sep=":")

    mapper = KeyMapper(entry_point="ep1", sep=":")
    with pytest.raises(ValueError, match="at least one key part is required"):
        _ = mapper.full_key()
    with pytest.raises(ValueError, match="key parts must not be empty"):
        _ = mapper.full_key("")
    with pytest.raises(ValueError, match="key parts must not contain separator"):
        _ = mapper.full_key("bad:key")
    with pytest.raises(ValueError, match="key does not match entry point prefix"):
        _ = mapper.relative_parts("ep2:main")
    with pytest.raises(ValueError, match="relative key path must not be empty"):
        _ = mapper.relative_parts("ep1:")
    with pytest.raises(ValueError, match="invalid key path with empty segment"):
        _ = mapper.relative_parts("ep1:main::child")


def test_reconstruct_nested_no_conflict() -> None:
    data = [(("sup1",), {"key": "value"}), (("sup2",), [1, 2, 3])]
    assert reconstruct_nested(data) == {"sup1": {"key": "value"}, "sup2": [1, 2, 3]}


def test_reconstruct_nested_dict_leaf_plus_children_merges() -> None:
    data = [((), {"base": True}), (("sub",), [1, 2, 3])]
    assert reconstruct_nested(data) == {"base": True, "sub": [1, 2, 3]}


def test_reconstruct_nested_scalar_leaf_plus_children_uses_value() -> None:
    data = [((), [1, 2, 3]), (("sub",), {"nested": True})]
    assert reconstruct_nested(data) == {"_value": [1, 2, 3], "sub": {"nested": True}}
