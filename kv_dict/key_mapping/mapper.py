"""Key mapping utilities for entry-point-prefixed KV keys."""

from __future__ import annotations


class KeyMapper:
    """Map between backend KV keys and logical nested paths."""

    def __init__(self, entry_point: str, sep: str = ":") -> None:
        super().__init__()
        if not entry_point:
            msg = "entry_point must not be empty"
            raise ValueError(msg)
        if not sep:
            msg = "sep must not be empty"
            raise ValueError(msg)
        if sep in entry_point:
            msg = "entry_point must not contain separator"
            raise ValueError(msg)

        self.entry_point = entry_point
        self.sep = sep
        self.prefix = f"{entry_point}{sep}"

    def full_key(self, *parts: str) -> str:
        """Build a backend key from one or more logical path parts."""
        if not parts:
            msg = "at least one key part is required"
            raise ValueError(msg)
        for part in parts:
            if not part:
                msg = "key parts must not be empty"
                raise ValueError(msg)
            if self.sep in part:
                msg = "key parts must not contain separator"
                raise ValueError(msg)
        return self.prefix + self.sep.join(parts)

    def matches(self, kv_key: str) -> bool:
        """Return True when a backend key belongs to this entry point."""
        return kv_key.startswith(self.prefix)

    def relative_parts(self, kv_key: str) -> tuple[str, ...]:
        """Convert a backend key into relative path parts."""
        if not self.matches(kv_key):
            msg = f"key does not match entry point prefix: {kv_key}"
            raise ValueError(msg)

        relative = kv_key.removeprefix(self.prefix)
        if not relative:
            msg = "relative key path must not be empty"
            raise ValueError(msg)
        parts = tuple(relative.split(self.sep))
        if any(not part for part in parts):
            msg = f"invalid key path with empty segment: {kv_key}"
            raise ValueError(msg)
        return parts
