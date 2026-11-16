"""Discovery metadata for the ``us_congress`` catalog, plus the helpers every module uses.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these tags.
Every function and table carries ``function_tags(...)``: a title, a category from
``SCHEMA_CATEGORIES``, search keywords, LLM- and human-facing docs, and (for
fixed-schema table functions) the result columns.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "us_congress"
Q = f"{CATALOG_NAME}.main"

SOURCE_URL = "https://github.com/Query-farm/vgi-us-congress"

CONGRESS_GOV_DOCS = "https://github.com/LibraryOfCongress/api.congress.gov"
LEGISLATORS_DOCS = "https://github.com/unitedstates/congress-legislators"
VOTEVIEW_DOCS = "https://voteview.com/data"
SENATE_DOCS = "https://www.senate.gov/legislative/votes_new.htm"
API_KEY_SIGNUP = "https://api.congress.gov/sign-up/"

SCHEMA_CATEGORIES = [
    {
        "name": "members",
        "title": "Members of Congress",
        "description": "Every senator and representative since 1789, their terms, ideology scores and identifiers.",
    },
    {
        "name": "votes",
        "title": "Roll-call votes",
        "description": "House and Senate roll-call votes and how each member voted, historical and current.",
    },
    {
        "name": "legislation",
        "title": "Bills and laws",
        "description": "Bills, resolutions, amendments and laws from the Congress.gov API, with actions and sponsors.",
    },
    {
        "name": "senate_business",
        "title": "Committees, nominations and treaties",
        "description": "Congressional committees, presidential nominations and treaties from the Congress.gov API.",
    },
    {
        "name": "utilities",
        "title": "Utilities",
        "description": "Code decoders and the worker's download cache.",
    },
]

API_KEY_NOTE = (
    "Needs a free Congress.gov (api.data.gov) API key stored in a DuckDB secret of type us_congress (field "
    "API_KEY); without one the scan fails with a message naming that secret type."
)
KEYLESS_NOTE = "No API key needed."


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def duckdb_type(t: pa.DataType) -> str:
    """Spell an Arrow type the way DuckDB names it (for ``vgi.result_columns_schema``)."""
    if pa.types.is_timestamp(t):
        return "TIMESTAMP WITH TIME ZONE" if t.tz else "TIMESTAMP"
    if pa.types.is_list(t):
        return f"{duckdb_type(t.value_type)}[]"
    simple = {
        pa.string(): "VARCHAR",
        pa.int64(): "BIGINT",
        pa.int32(): "INTEGER",
        pa.int16(): "SMALLINT",
        pa.bool_(): "BOOLEAN",
        pa.float64(): "DOUBLE",
        pa.date32(): "DATE",
    }
    return simple.get(t, str(t).upper())


def result_columns(schema: pa.Schema) -> str:
    """``vgi.result_columns_schema`` for a fixed output schema."""
    return json.dumps(
        [
            {"name": f.name, "type": duckdb_type(f.type), "description": (f.metadata or {})[b"comment"].decode()}
            for f in schema
        ]
    )


def function_tags(
    *,
    title: str,
    category: str,
    keywords: str,
    doc_llm: str,
    doc_md: str,
    schema: pa.Schema | None = None,
    executable_examples: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    """Build the standard per-object discovery tags.

    Args:
        title: Human title.
        category: One of ``SCHEMA_CATEGORIES``.
        keywords: Comma-separated search keywords.
        doc_llm: Guidance for agents.
        doc_md: Markdown for people.
        schema: Fixed output schema, published as ``vgi.result_columns_schema``.
        executable_examples: Deterministic, offline examples with expected results.

    Returns:
        The tag dictionary.

    Raises:
        ValueError: ``category`` is not declared.

    """
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    tags = {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }
    if schema is not None:
        tags["vgi.result_columns_schema"] = result_columns(schema)
    if executable_examples:
        tags["vgi.executable_examples"] = json.dumps(executable_examples)
    return tags


# The catalog comment is the one-line description directories show: one plain
# paragraph, no Markdown, no function list.
CATALOG_COMMENT = (
    "US Congress lawmaking as SQL: members of Congress since 1789 keyed by bioguide ID, their terms and "
    "DW-NOMINATE ideology, House and Senate roll-call votes back to the 1st Congress, and bills, laws, "
    "amendments, committees, nominations and treaties from Congress.gov."
)

CATALOG_DOC_LLM = (
    "US Congress data keyed on bioguide_id, the Biographical Directory ID used by Congress.gov, the House and "
    "Senate. Four upstreams: (1) the CC0 unitedstates/congress-legislators dataset for every member since 1789 "
    "and each of their terms, with cross-walk IDs (thomas, lis, govtrack, icpsr, fec, wikidata, opensecrets); "
    "(2) Voteview (Lewis et al.) for every House and Senate roll call since 1789, each member's vote, and "
    "DW-NOMINATE ideology scores; (3) the Senate's roll-call XML for the newest Senate votes and positions; "
    "(4) the Congress.gov API v3 for bills, laws, amendments, committees, nominations, treaties and House "
    "roll-call votes (118th Congress onward). The member, vote and Senate relations need no key. The "
    "Congress.gov relations need an api.data.gov key stored in a DuckDB secret of type us_congress. "
    "Congress numbers are "
    "integers (the 118th Congress is 118, covering 2023-2025; congress = "
    "(year - 1789) / 2 + 1 for odd starting years). Chamber values are 'house' and 'senate'. Bill types are "
    "lower-case Congress.gov codes: hr, s, hjres, sjres, hconres, sconres, hres, sres. Bulk files are cached "
    "per user and reused; Congress.gov is read live, so its results change as Congress acts."
)

CATALOG_DOC_MD = (
    "# US Congress in DuckDB\n\n"
    "Members, votes and legislation of the United States Congress, joined on **bioguide_id**.\n\n"
    "| Upstream | What it covers | Key |\n|---|---|---|\n"
    f"| [unitedstates/congress-legislators]({LEGISLATORS_DOCS}) (CC0) | every member since 1789, terms, IDs | "
    "none |\n"
    f"| [Voteview]({VOTEVIEW_DOCS}) (Lewis, Poole, Rosenthal et al.) | all roll calls since 1789, member votes, "
    "DW-NOMINATE | none |\n"
    f"| [Senate roll-call votes]({SENATE_DOCS}) | the newest Senate votes, member by member | none |\n"
    f"| [Congress.gov API v3]({CONGRESS_GOV_DOCS}) | bills, laws, amendments, committees, nominations, treaties, "
    f"House votes | [free api.data.gov key]({API_KEY_SIGNUP}) |\n\n"
    "Bulk files are downloaded on first use into a per-user cache and revalidated after a TTL (24 hours by "
    "default). The Congress.gov API is read live, 250 rows per request, with congress, type and date filters "
    "pushed into the request.\n\n"
    "```sql\nCREATE SECRET (TYPE us_congress, API_KEY 'your-api.data.gov-key');\n```\n\n"
    "Please cite Voteview as: Lewis, Jeffrey B., Keith Poole, Howard Rosenthal, Adam Boche, Aaron Rudkin, and "
    "Luke Sonnet. *Voteview: Congressional Roll-Call Votes Database*. https://voteview.com/"
)

SCHEMA_COMMENT = (
    "Members of Congress, roll-call votes and legislation, keyed on bioguide_id; the member and vote relations "
    "need no key, the Congress.gov relations need an API key secret."
)

SCHEMA_DOC_LLM = (
    "Everything here joins on bioguide_id. People come first: the members table has one row per person since "
    "1789 with cross-walk IDs, and member_terms has one row per term with chamber, dates, state, district and "
    "party; filter terms on start_date/end_date to find who served when. Voting behaviour has three layers: "
    "Voteview's per-Congress files for every roll call since 1789, each member's position and DW-NOMINATE "
    "ideology (pass the Congress number and chamber := 'house' or 'senate' to download only what you need); "
    "senate.gov's XML for Senate votes taken today; and Congress.gov's beta House vote endpoint from the 118th "
    "Congress on. Legislation (bills and their actions, cosponsors, subjects, summaries and text; laws; "
    "amendments; committees; nominations; treaties) comes live from the Congress.gov API and needs an API key "
    "secret of type us_congress; congress, bill type and update_date filters are pushed into those requests. "
    "Prefer the keyless relations when they can answer the question."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "- **Members**: who served, when, for which state and party, with ideology scores.\n"
    "- **Votes**: every roll call since 1789 from Voteview, the newest Senate votes from senate.gov, and House "
    "votes from Congress.gov.\n"
    "- **Legislation**: bills, laws, amendments, committees, nominations and treaties from Congress.gov "
    "(API key required).\n\n"
    "Join everything on `bioguide_id`."
)

AGENT_TEST_TASKS = [
    {
        "name": "longest_serving_senators",
        "prompt": (
            "Using the us_congress worker, which five people served the most total days as US senators, adding up "
            "all of their Senate terms? Return their bioguide IDs and total days, longest first."
        ),
    },
    {
        "name": "women_in_119th_house",
        "prompt": (
            "Using the us_congress worker, how many different women held a seat in the US House of "
            "Representatives (delegates included) at any time during the 119th Congress, which began on "
            "2025-01-03?"
        ),
    },
    {
        "name": "senate_rollcalls_118",
        "prompt": "Using the us_congress worker, how many Senate roll-call votes were taken in the 118th Congress?",
    },
    {
        "name": "most_conservative_house_119",
        "prompt": (
            "Using the us_congress worker, which member of the House in the 119th Congress has the most "
            "conservative DW-NOMINATE first-dimension score? Give the bioguide ID and the score."
        ),
    },
    {
        "name": "sanders_missed_votes_118",
        "prompt": (
            "Using the us_congress worker, on how many Senate roll calls of the 118th Congress was Bernie Sanders "
            "(bioguide S000033) recorded as not voting?"
        ),
    },
    {
        "name": "senate_votes_2025",
        "prompt": (
            "Using the us_congress worker, how many roll-call votes did the Senate hold in 2025 (119th Congress, "
            "session 1)?"
        ),
    },
    {
        "name": "first_senate_vote_2025_nays",
        "prompt": (
            "Using the us_congress worker, how many senators voted Nay on the Senate's first roll-call vote of the "
            "119th Congress (session 1, vote 1)?"
        ),
    },
    {
        "name": "madison_house_terms",
        "prompt": (
            "Using the us_congress worker, how many terms did James Madison serve in the House, and which Virginia "
            "districts did he represent?"
        ),
    },
    {
        "name": "cache_footprint",
        "prompt": "Using the us_congress worker, how many files has it downloaded into its cache, and how many bytes?",
    },
    {
        "name": "hr2882_profile",
        "prompt": (
            "Using the us_congress worker with its Congress.gov API key secret configured: for H.R. 2882 of the "
            "118th Congress, who was the sponsor (bioguide ID), how many cosponsors, recorded actions, legislative "
            "subjects, CRS summaries and text versions does it have, and which public law did it become?"
        ),
    },
    {
        "name": "congress_118_output",
        "prompt": (
            "Using the us_congress worker with its Congress.gov API key secret configured, profile the 118th "
            "Congress: how many public laws were enacted, how many House roll calls on legislation were held in "
            "session 1, how many Senate committees and subcommittees existed, how many nominations and treaties "
            "were received, how many Senate amendments were filed, how many bills were introduced, how many "
            "members represented California, how many bills Chuck Schumer (S000148) sponsored and cosponsored in "
            "total, and how Nancy Pelosi (P000197) voted on House roll call 296 of session 1."
        ),
    },
    {
        "name": "decode_cast_code",
        "prompt": "Using the us_congress worker, what label does Voteview give to vote cast code 6?",
    },
]

CATALOG_TAGS = {
    "vgi.title": "Congress Votes & Laws",
    "vgi.subtitle": "Members since 1789, roll-call votes, bills and laws of the US Congress, keyed on bioguide ID.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "congress, us congress, senate, house of representatives, bills, laws, roll call votes, legislators, "
        "bioguide, voteview, dw-nominate, congress.gov, civic data"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.doc_links": json.dumps(
        [
            {"title": "Congress.gov API documentation", "url": CONGRESS_GOV_DOCS},
            {"title": "Get a Congress.gov API key", "url": API_KEY_SIGNUP},
            {"title": "unitedstates/congress-legislators", "url": LEGISLATORS_DOCS},
            {"title": "Voteview data", "url": VOTEVIEW_DOCS},
            {"title": "Senate roll call votes", "url": SENATE_DOCS},
        ]
    ),
    # Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
