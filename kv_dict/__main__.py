"""Interface for ``python -m kv_dict``."""

from argparse import ArgumentParser
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Sequence

from ._version import version


__all__ = ["main"]


def main(args: Sequence[str] | None = None) -> None:
    """Argument parser for the CLI."""
    parser = ArgumentParser()
    _ = parser.add_argument("-v", "--version", action="version", version=version)
    _ = parser.parse_args(args)


if __name__ == "__main__":
    main()
