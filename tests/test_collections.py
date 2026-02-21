"""Tests for utils.collections."""

from __future__ import annotations

from utils.collections import chunked


def test_chunked_empty():
    assert list(chunked([], 3)) == []


def test_chunked_exact_fit():
    assert list(chunked([1, 2, 3], 3)) == [[1, 2, 3]]


def test_chunked_partial_last():
    assert list(chunked([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


def test_chunked_size_one():
    assert list(chunked([1, 2, 3], 1)) == [[1], [2], [3]]


def test_chunked_size_larger_than_seq():
    assert list(chunked([1, 2], 10)) == [[1, 2]]


def test_chunked_strings():
    assert list(chunked(["a", "b", "c", "d"], 2)) == [["a", "b"], ["c", "d"]]
