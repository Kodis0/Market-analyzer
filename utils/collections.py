"""Collection utilities."""
from __future__ import annotations

from typing import Iterable, List, TypeVar

T = TypeVar("T")


def chunked(seq: Iterable[T], size: int) -> Iterable[List[T]]:
    """Split sequence into chunks of given size. Yields lists."""
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i : i + size]
