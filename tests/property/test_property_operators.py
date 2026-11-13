"""Property-based tests: operators agree with plain Python for arbitrary input."""

from __future__ import annotations

import asyncio

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from datacurrent import Stream

SETTINGS = settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)


def run(coro):
    return asyncio.run(coro)


@given(st.lists(st.integers(), max_size=40))
@SETTINGS
def test_map_matches_list_comprehension(items: list[int]) -> None:
    result = run(Stream.from_iterable(items).map(lambda n: n * 3 + 1).collect())
    assert result == [n * 3 + 1 for n in items]


@given(st.lists(st.integers(), max_size=40))
@SETTINGS
def test_filter_matches_list_comprehension(items: list[int]) -> None:
    result = run(Stream.from_iterable(items).filter(lambda n: n % 2 == 0).collect())
    assert result == [n for n in items if n % 2 == 0]


@given(st.lists(st.integers(), max_size=40), st.integers(min_value=1, max_value=7))
@SETTINGS
def test_batch_flattens_back_to_input(items: list[int], size: int) -> None:
    batches = run(Stream.from_iterable(items).batch(size).collect())
    flattened = [item for batch in batches for item in batch]
    assert flattened == items
    assert all(1 <= len(batch) <= size for batch in batches)


@given(st.lists(st.integers(), max_size=40))
@SETTINGS
def test_distinct_matches_dict_fromkeys(items: list[int]) -> None:
    result = run(Stream.from_iterable(items).distinct().collect())
    assert result == list(dict.fromkeys(items))


@given(
    st.lists(st.integers(), max_size=40),
    st.integers(min_value=0, max_value=10),
    st.integers(min_value=0, max_value=10),
)
@SETTINGS
def test_skip_and_take_match_slicing(items: list[int], skip: int, take: int) -> None:
    result = run(Stream.from_iterable(items).skip(skip).take(take).collect())
    assert result == items[skip : skip + take]


@given(st.lists(st.integers(), max_size=30), st.integers(min_value=1, max_value=6))
@SETTINGS
def test_window_sizes(items: list[int], size: int) -> None:
    windows = run(Stream.from_iterable(items).window(size).collect())
    expected_count = max(0, len(items) - size + 1)
    assert len(windows) == expected_count
    assert all(len(window) == size for window in windows)
    for index, window in enumerate(windows):
        assert window == items[index : index + size]


@given(st.lists(st.integers(), max_size=30))
@SETTINGS
def test_flat_map_matches_concatenation(items: list[int]) -> None:
    result = run(Stream.from_iterable(items).flat_map(lambda n: [n, -n]).collect())
    assert result == [value for n in items for value in (n, -n)]


@given(st.lists(st.integers(), max_size=30), st.lists(st.integers(), max_size=30))
@SETTINGS
def test_zip_matches_shortest(first: list[int], second: list[int]) -> None:
    result = run(Stream.from_iterable(first).zip(second).collect())
    assert result == list(zip(first, second, strict=False))


@given(st.lists(st.integers(), max_size=40))
@SETTINGS
def test_reduce_matches_sum(items: list[int]) -> None:
    result = run(Stream.from_iterable(items).reduce(lambda acc, n: acc + n, 0))
    assert result == sum(items)
