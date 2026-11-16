"""The ``cache_files`` table: what the worker has downloaded, where, and when."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Any, ClassVar

import pyarrow as pa
from vgi.catalog import Table
from vgi.catalog.constraints import constraint_record
from vgi.table_function import ProcessParams, bind_fixed_schema, init_single_worker

from .common import ArrowTableSource
from .fetch import cache_entries, settings_of
from .meta import Q, column, function_tags

CACHE_SCHEMA = pa.schema(
    [
        column(
            "cache_dir",
            pa.string(),
            "The cache directory in use (ATTACH option cache_dir, or the per-user default).",
            nullable=False,
        ),
        column("path", pa.string(), "Absolute path of the cached file.", nullable=False),
        column("url", pa.string(), "Upstream URL the file was downloaded from."),
        column("bytes", pa.int64(), "File size in bytes."),
        column("fetched_at", pa.timestamp("us", tz="UTC"), "When the file was last downloaded or revalidated."),
        column("etag", pa.string(), "Upstream ETag, used to revalidate after the TTL."),
        column("last_modified", pa.string(), "Upstream Last-Modified header."),
        column("sha256", pa.string(), "SHA-256 of the downloaded bytes."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """The scan takes no arguments."""


@bind_fixed_schema
@init_single_worker
class CacheInfo(ArrowTableSource[NoArgs]):
    """Rows of the ``cache_files`` table."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CACHE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "cache_files_source"
        description = "Files in the worker's download cache (backs the cache_files table)"

    @classmethod
    def build(cls, params: ProcessParams[Any]) -> pa.Table:
        """Scan the cache directory."""
        settings = settings_of(params)
        rows = [
            {
                **e,
                "cache_dir": str(settings.cache_root),
                "fetched_at": dt.datetime.fromtimestamp(float(e["fetched_at"]), dt.UTC)
                if e.get("fetched_at")
                else None,
            }
            for e in cache_entries(settings)
        ]
        return pa.Table.from_pylist(rows, schema=CACHE_SCHEMA)


CACHE_TABLE = Table(
    name="cache_files",
    function=CacheInfo,
    constraints=(
        constraint_record(
            ["main"],
            "cache_files",
            "path_not_null",
            "NOT_NULL",
            [["path"]],
            enforcement="ENFORCED",
            validation="ALL_ROWS",
        ),
        constraint_record(
            ["main"], "cache_files", "pk", "PRIMARY_KEY", [["path"]], enforcement="ENFORCED", validation="ALL_ROWS"
        ),
    ),
    comment="The bulk files (legislators, Voteview, Senate XML) this worker has downloaded, with source and freshness.",
    column_comments={f.name: (f.metadata or {})[b"comment"].decode() for f in CACHE_SCHEMA},
    tags=function_tags(
        title="Download Cache",
        category="utilities",
        keywords="cache, downloads, disk, files, freshness, etag, storage",
        doc_llm=(
            "One row per file in the worker's download cache: cache_dir, path, url, bytes, fetched_at, etag, "
            "last_modified, sha256. Files are downloaded on first use by members, member_terms, the Voteview "
            "functions and the Senate functions, and revalidated after the cache TTL (ATTACH option "
            "cache_ttl_hours, default 24). Set ATTACH option cache_dir to move the cache. Use it to see how "
            "fresh the data is or how much disk it uses. Reading it makes no network requests."
        ),
        doc_md=(
            "## cache_files\n\nWhat has been downloaded, from where, and when. Delete files from `cache_dir` "
            "to force a fresh download."
        ),
    )
    | {
        "domain": "government",
        "vgi.example_queries": json.dumps(
            [
                {
                    "description": "Cached files, largest first",
                    "sql": f"SELECT url, bytes, fetched_at FROM {Q}.cache_files ORDER BY bytes DESC",
                },
                {
                    "description": "How much disk the cache uses",
                    "sql": f"SELECT count(*) AS files, coalesce(sum(bytes), 0) AS total_bytes FROM {Q}.cache_files",
                },
            ]
        ),
    },
)
