"""Assemble the ``us_congress`` catalog and run the worker.

Run it:

    vgi-us-congress-worker                      # stdio: what DuckDB launches by default
    vgi-us-congress-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    ATTACH 'us_congress' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-us-congress-worker');
    CREATE SECRET (TYPE us_congress, API_KEY '...');   -- only for the Congress.gov functions
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from vgi import Worker
from vgi.catalog import Catalog, Schema
from vgi.catalog.attach_option import AttachOption

from .cache_info import CACHE_TABLE
from .congress_gov import CONGRESS_GOV_FUNCTIONS, CONGRESS_SECRET
from .fetch import (
    DEFAULT_CACHE_TTL_HOURS,
    DEFAULT_CONGRESS_GOV_URL,
    DEFAULT_LEGISLATORS_URL,
    DEFAULT_SENATE_URL,
    DEFAULT_VOTEVIEW_URL,
)
from .legislators import MEMBER_TERMS_TABLE, MEMBERS_TABLE
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_COMMENT,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .senate import SenateVotePositions, SenateVotes
from .voteview import MemberIdeology, MemberIdeologyAll, MemberVotes, Rollcalls, RollcallsAll, VoteCastLabel

FUNCTIONS: list[type[Any]] = [
    Rollcalls,
    RollcallsAll,
    MemberVotes,
    MemberIdeology,
    MemberIdeologyAll,
    SenateVotes,
    SenateVotePositions,
    VoteCastLabel,
    *CONGRESS_GOV_FUNCTIONS,
]


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries``; overloads share one SQL name."""
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entries[0] for entries in by_name.values() if entries]


_SCHEMA_EXAMPLES = _stamp_example_queries(FUNCTIONS)


class AttachOptions:
    """ATTACH options: where each upstream lives and where downloads are cached."""

    cache_dir: Annotated[
        str | None,
        AttachOption(desc="Directory for downloaded bulk files; defaults to the per-user cache directory."),
    ] = None
    cache_ttl_hours: Annotated[
        float | None,
        AttachOption(
            desc=f"Hours a cached file is used before it is revalidated upstream (default {DEFAULT_CACHE_TTL_HOURS:g})."
        ),
    ] = None
    congress_gov_url: Annotated[
        str | None, AttachOption(desc=f"Congress.gov API root (default {DEFAULT_CONGRESS_GOV_URL}).")
    ] = None
    legislators_url: Annotated[
        str | None, AttachOption(desc=f"congress-legislators file root (default {DEFAULT_LEGISLATORS_URL}).")
    ] = None
    voteview_url: Annotated[str | None, AttachOption(desc=f"Voteview data root (default {DEFAULT_VOTEVIEW_URL}).")] = (
        None
    )
    senate_url: Annotated[
        str | None, AttachOption(desc=f"Senate LIS roll-call XML root (default {DEFAULT_SENATE_URL}).")
    ] = None


class UsCongressWorker(Worker):
    """The worker process: the ``us_congress`` catalog with one ``main`` schema."""

    AttachOptions = AttachOptions

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment=SCHEMA_COMMENT,
                functions=FUNCTIONS,
                tables=[MEMBERS_TABLE, MEMBER_TERMS_TABLE, CACHE_TABLE],
                tags={
                    "vgi.title": "US Congress",
                    "domain": "government",
                    "vgi.keywords": keywords_array(
                        "members of congress, terms, roll call votes, voteview, senate votes, bills, laws, "
                        "amendments, committees, nominations, treaties"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_SCHEMA_EXAMPLES),
                },
            )
        ],
    )

    secret_types = [CONGRESS_SECRET]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    UsCongressWorker.main()


if __name__ == "__main__":
    main()
