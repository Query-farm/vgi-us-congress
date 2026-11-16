"""Shared table-function plumbing: a cursor over an in-memory Arrow table, and value converters."""

from __future__ import annotations

import datetime as dt
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, TypeVar

import pyarrow as pa
from vgi.table_function import OutputCollector, ProcessParams, TableFunctionGenerator
from vgi_rpc import ArrowSerializableDataclass

BATCH_ROWS = 50_000

A = TypeVar("A")


@dataclass(kw_only=True)
class RowCursor(ArrowSerializableDataclass):
    """Cursor: rows already emitted from the (re-derivable) result table."""

    offset: int = 0


class ArrowTableSource(TableFunctionGenerator[A, RowCursor]):
    """A table function whose whole result is a derivable Arrow table, streamed in slices.

    ``build`` derives the result from cached local files (memoized per process),
    so a continuation served by another process re-derives it and resumes at
    ``offset``: the cursor is just a row number.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([])

    @classmethod
    def build(cls, params: ProcessParams[Any]) -> pa.Table:
        """Return the full result table (all columns of ``FIXED_SCHEMA``)."""
        raise NotImplementedError

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> RowCursor:
        """Start at row 0."""
        return RowCursor()

    @classmethod
    def process(cls, params: ProcessParams[Any], state: RowCursor, out: OutputCollector) -> None:
        """Emit the next slice of the result; finish after the last row."""
        table = cls.build(params)
        if state.offset >= table.num_rows:
            out.finish()
            return
        piece = table.slice(state.offset, BATCH_ROWS)
        state.offset += piece.num_rows
        columns = [piece.column(name) for name in params.output_schema.names]
        batch = pa.Table.from_arrays(columns, schema=params.output_schema).combine_chunks().to_batches()
        if batch:
            out.emit(batch[0])
        if state.offset >= table.num_rows:
            out.finish()


_memo_lock = threading.Lock()
_memo: dict[tuple[Any, ...], Any] = {}
_MEMO_MAX = 16


def memoized(key: tuple[Any, ...], paths: list[Path], factory: Callable[[], Any]) -> Any:
    """Return ``factory()``, cached per process until any of ``paths`` changes on disk."""
    stamp = tuple((str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in paths)
    full = (*key, stamp)
    with _memo_lock:
        if full in _memo:
            return _memo[full]
    value = factory()
    with _memo_lock:
        if len(_memo) >= _MEMO_MAX:
            _memo.pop(next(iter(_memo)))
        _memo[full] = value
    return value


def parse_date(value: Any) -> dt.date | None:
    """Parse ``YYYY-MM-DD`` (or the date part of an ISO timestamp); ``None`` for blanks."""
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def parse_timestamp(value: Any) -> dt.datetime | None:
    """Parse an ISO-8601 timestamp (``Z`` or offset) or a bare date into an aware UTC datetime."""
    if not value:
        return None
    text = str(value).strip()
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def to_int(value: Any) -> int | None:
    """Coerce ``value`` to int, or ``None`` when blank or not a number."""
    if value is None or value == "":
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        try:
            return int(float(str(value)))
        except ValueError:
            return None


def to_text(value: Any) -> str | None:
    """Coerce ``value`` to a stripped string, or ``None`` when blank."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


CHAMBERS = ("house", "senate")
CHAMBER_DOC = "Limit to one chamber: 'house' or 'senate'. NULL (the default) reads both."


def chamber_name(value: Any) -> str | None:
    """Normalize a chamber spelling ('House', 'H', 'House of Representatives', 'sen') to 'house'/'senate'."""
    text = (to_text(value) or "").lower()
    if text.startswith(("h", "rep")):
        return "house"
    if text.startswith("s"):
        return "senate"
    if text.startswith("p"):
        return "president"
    return None
