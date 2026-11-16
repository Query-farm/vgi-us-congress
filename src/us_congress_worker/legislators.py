"""Members of Congress since 1789 from the CC0 unitedstates/congress-legislators dataset.

Two catalog tables, read from ``legislators-current.json`` and
``legislators-historical.json`` (about 15 MB together), downloaded on first scan
and cached:

* ``members``: one row per person, keyed by ``bioguide_id``.
* ``member_terms``: one row per term served, keyed by ``(bioguide_id, term_number)``.

The same files give the cross-walks other relations need: ``lis`` IDs (Senate
roll-call XML) and ``icpsr`` IDs (Voteview) to ``bioguide_id``.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa
from vgi.catalog import Table
from vgi.catalog.constraints import ReferencedKey, constraint_record
from vgi.table_function import ProcessParams, bind_fixed_schema, init_single_worker

from .common import ArrowTableSource, memoized, parse_date, to_int, to_text
from .fetch import Settings, cached_file, settings_of
from .meta import KEYLESS_NOTE, LEGISLATORS_DOCS, Q, column, function_tags

FILES = ("legislators-current.json", "legislators-historical.json")


def legislator_files(settings: Settings) -> list[Path]:
    """Download (or reuse) the current and historical legislator files."""
    return [cached_file(settings, f"{settings.legislators_url}/{name}") for name in FILES]


def _people(paths: list[Path]) -> list[tuple[dict[str, Any], bool]]:
    """Every person in the files, with a flag for the current-members file."""
    people: list[tuple[dict[str, Any], bool]] = []
    for path, current in zip(paths, (True, False), strict=True):
        people.extend((p, current) for p in json.loads(path.read_text(encoding="utf-8")))
    return people


def _full_name(name: dict[str, Any]) -> str | None:
    if name.get("official_full"):
        return str(name["official_full"])
    parts = [name.get("first"), name.get("middle"), name.get("last")]
    full = " ".join(str(p) for p in parts if p)
    if name.get("suffix"):
        full = f"{full}, {name['suffix']}"
    return full or None


def _chamber(term_type: str | None) -> str | None:
    return {"rep": "house", "sen": "senate"}.get(term_type or "")


def _id_int(ids: dict[str, Any], key: str) -> int | None:
    return to_int(ids.get(key))


MEMBER_FIELDS: list[pa.Field[Any]] = [
    column("bioguide_id", pa.string(), "Biographical Directory ID, e.g. 'S000148'; the join key.", nullable=False),
    column("full_name", pa.string(), "Official full name, or first/middle/last when no official form exists."),
    column("first_name", pa.string(), "Given name."),
    column("middle_name", pa.string(), "Middle name or initial."),
    column("last_name", pa.string(), "Surname."),
    column("suffix", pa.string(), "Name suffix such as 'Jr.' or 'III'."),
    column("nickname", pa.string(), "Commonly used nickname."),
    column("birthday", pa.date32(), "Date of birth, when known."),
    column("gender", pa.string(), "'M' or 'F' as recorded by the dataset."),
    column("is_current", pa.bool_(), "True for members in the legislators-current file (serving now)."),
    column("first_term_start", pa.date32(), "Start date of the member's first term."),
    column("last_term_end", pa.date32(), "End date of the member's latest term (a future date for current terms)."),
    column("term_count", pa.int32(), "Number of terms served (House terms are two years, Senate terms six)."),
    column("house_terms", pa.int32(), "Number of House terms (including delegate and resident-commissioner terms)."),
    column("senate_terms", pa.int32(), "Number of Senate terms."),
    column("latest_chamber", pa.string(), "'house' or 'senate': the chamber of the latest term."),
    column("latest_state", pa.string(), "USPS state or territory code of the latest term, e.g. 'VT'."),
    column("latest_party", pa.string(), "Party of the latest term, e.g. 'Democrat', 'Republican', 'Independent'."),
    column("thomas_id", pa.string(), "THOMAS ID (Library of Congress legacy system), 5 digits."),
    column("lis_id", pa.string(), "Senate LIS member ID, e.g. 'S313'; used by Senate roll-call XML."),
    column("govtrack_id", pa.int64(), "GovTrack.us person ID (an identifier, not a quantity)."),
    column("icpsr_id", pa.int64(), "ICPSR member ID (an identifier); Voteview's member key."),
    column("opensecrets_id", pa.string(), "OpenSecrets (CRP) candidate ID, e.g. 'N00000528'."),
    column("votesmart_id", pa.int64(), "Vote Smart candidate ID (an identifier)."),
    column("fec_ids", pa.list_(pa.string()), "Federal Election Commission candidate IDs (one per office sought)."),
    column("cspan_id", pa.int64(), "C-SPAN video library person ID (an identifier)."),
    column("wikipedia", pa.string(), "English Wikipedia page title."),
    column("wikidata_id", pa.string(), "Wikidata entity ID, e.g. 'Q359442'."),
    column("ballotpedia", pa.string(), "Ballotpedia page title."),
    column("house_history_id", pa.int64(), "History, Art & Archives of the House person ID (an identifier)."),
    column("google_entity_id", pa.string(), "Google Knowledge Graph entity ID."),
]
MEMBER_SCHEMA = pa.schema(MEMBER_FIELDS)

TERM_FIELDS: list[pa.Field[Any]] = [
    column("bioguide_id", pa.string(), "Biographical Directory ID of the member.", nullable=False),
    column(
        "term_number",
        pa.int32(),
        "Ordinal of the term (1 for the member's first), counting both chambers.",
        nullable=False,
    ),
    column("chamber", pa.string(), "'house' or 'senate'.", nullable=False),
    column("start_date", pa.date32(), "Date the term began (or the member was sworn in).", nullable=False),
    column("end_date", pa.date32(), "Date the term ended or ends; earlier than scheduled if the member left early."),
    column("state", pa.string(), "USPS state or territory code, e.g. 'CA', 'PR', 'DC'.", nullable=False),
    column(
        "district",
        pa.int32(),
        "House district number (an identifier); 0 for at-large seats and delegates, -1 where historic data "
        "has no number. "
        "NULL for senators.",
    ),
    column(
        "senate_class",
        pa.int32(),
        "Senate election class (1, 2 or 3: which third of the Senate); NULL for House terms.",
    ),
    column("state_rank", pa.string(), "'senior' or 'junior' senator (current senators only)."),
    column("party", pa.string(), "Party at the start of the term, e.g. 'Democrat', 'Republican', 'Whig'."),
    column("caucus", pa.string(), "Party the member caucuses with, when different from party."),
    column("how", pa.string(), "'appointment' or 'special-election' when the term did not start at an election."),
    column("end_type", pa.string(), "'special-election' when an appointed term ended at a special election."),
    column("is_current", pa.bool_(), "True when today falls within the term of a currently serving member."),
    column("url", pa.string(), "Official website for the term."),
    column("office_address", pa.string(), "Washington office mailing address."),
    column("phone", pa.string(), "Washington office phone number."),
    column("contact_form", pa.string(), "Official web contact form."),
]
TERM_SCHEMA = pa.schema(TERM_FIELDS)


def _member_row(person: dict[str, Any], current: bool) -> dict[str, Any]:
    ids, name, bio, terms = person["id"], person.get("name", {}), person.get("bio", {}), person.get("terms", [])
    last = terms[-1] if terms else {}
    fec = ids.get("fec")
    return {
        "bioguide_id": ids["bioguide"],
        "full_name": _full_name(name),
        "first_name": to_text(name.get("first")),
        "middle_name": to_text(name.get("middle")),
        "last_name": to_text(name.get("last")),
        "suffix": to_text(name.get("suffix")),
        "nickname": to_text(name.get("nickname")),
        "birthday": parse_date(bio.get("birthday")),
        "gender": to_text(bio.get("gender")),
        "is_current": current,
        "first_term_start": parse_date(terms[0].get("start")) if terms else None,
        "last_term_end": parse_date(last.get("end")),
        "term_count": len(terms),
        "house_terms": sum(1 for t in terms if t.get("type") == "rep"),
        "senate_terms": sum(1 for t in terms if t.get("type") == "sen"),
        "latest_chamber": _chamber(last.get("type")),
        "latest_state": to_text(last.get("state")),
        "latest_party": to_text(last.get("party")),
        "thomas_id": to_text(ids.get("thomas")),
        "lis_id": to_text(ids.get("lis")),
        "govtrack_id": _id_int(ids, "govtrack"),
        "icpsr_id": _id_int(ids, "icpsr"),
        "opensecrets_id": to_text(ids.get("opensecrets")),
        "votesmart_id": _id_int(ids, "votesmart"),
        "fec_ids": [str(f) for f in fec] if isinstance(fec, list) else None,
        "cspan_id": _id_int(ids, "cspan"),
        "wikipedia": to_text(ids.get("wikipedia")),
        "wikidata_id": to_text(ids.get("wikidata")),
        "ballotpedia": to_text(ids.get("ballotpedia")),
        "house_history_id": _id_int(ids, "house_history"),
        "google_entity_id": to_text(ids.get("google_entity_id")),
    }


def _term_rows(person: dict[str, Any], current: bool, today: dt.date) -> list[dict[str, Any]]:
    rows = []
    for i, t in enumerate(person.get("terms", []), start=1):
        start, end = parse_date(t.get("start")), parse_date(t.get("end"))
        rows.append(
            {
                "bioguide_id": person["id"]["bioguide"],
                "term_number": i,
                "chamber": _chamber(t.get("type")),
                "start_date": start,
                "end_date": end,
                "state": to_text(t.get("state")),
                "district": to_int(t.get("district")) if t.get("type") == "rep" else None,
                "senate_class": to_int(t.get("class")),
                "state_rank": to_text(t.get("state_rank")),
                "party": to_text(t.get("party")),
                "caucus": to_text(t.get("caucus")),
                "how": to_text(t.get("how")),
                "end_type": to_text(t.get("end-type")),
                "is_current": bool(current and start and start <= today and (end is None or today <= end)),
                "url": to_text(t.get("url")),
                "office_address": to_text(t.get("address")),
                "phone": to_text(t.get("phone")),
                "contact_form": to_text(t.get("contact_form")),
            }
        )
    return rows


@dataclass(frozen=True, slots=True)
class Legislators:
    """The parsed legislator files."""

    members: pa.Table
    terms: pa.Table
    lis_to_bioguide: dict[str, str]
    icpsr_to_bioguide: dict[int, str]


def parse_legislators(paths: list[Path], today: dt.date | None = None) -> Legislators:
    """Flatten the current and historical files into the members and member_terms tables."""
    today = today or dt.date.today()
    people = _people(paths)
    members = [_member_row(p, cur) for p, cur in people]
    terms = [row for p, cur in people for row in _term_rows(p, cur, today)]
    lis = {str(p["id"]["lis"]): str(p["id"]["bioguide"]) for p, _ in people if p["id"].get("lis")}
    icpsr = {int(p["id"]["icpsr"]): str(p["id"]["bioguide"]) for p, _ in people if p["id"].get("icpsr")}
    return Legislators(
        members=pa.Table.from_pylist(members, schema=MEMBER_SCHEMA),
        terms=pa.Table.from_pylist(terms, schema=TERM_SCHEMA),
        lis_to_bioguide=lis,
        icpsr_to_bioguide=icpsr,
    )


def load_legislators(settings: Settings) -> Legislators:
    """Download/reuse the files and return the parsed tables (memoized per process)."""
    paths = legislator_files(settings)
    result: Legislators = memoized(("legislators", dt.date.today()), paths, lambda: parse_legislators(paths))
    return result


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """The table scans take no arguments."""


_SOURCE_MD = (
    f"Source: [unitedstates/congress-legislators]({LEGISLATORS_DOCS}) (CC0 public domain), downloaded on first "
    "scan and cached; refreshed after the cache TTL."
)


@bind_fixed_schema
@init_single_worker
class MembersSource(ArrowTableSource[NoArgs]):
    """Rows of the ``members`` table."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = MEMBER_SCHEMA

    class Meta:
        """Function metadata."""

        name = "members_source"
        description = "Every member of Congress since 1789, one row per person (backs the members table)"

    @classmethod
    def build(cls, params: ProcessParams[Any]) -> pa.Table:
        """Return the members table."""
        return load_legislators(settings_of(params)).members


@bind_fixed_schema
@init_single_worker
class MemberTermsSource(ArrowTableSource[NoArgs]):
    """Rows of the ``member_terms`` table."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TERM_SCHEMA

    class Meta:
        """Function metadata."""

        name = "member_terms_source"
        description = "Every term served by a member of Congress since 1789 (backs the member_terms table)"

    @classmethod
    def build(cls, params: ProcessParams[Any]) -> pa.Table:
        """Return the member_terms table."""
        return load_legislators(settings_of(params)).terms


def _comments(schema: pa.Schema) -> dict[str, str]:
    return {f.name: (f.metadata or {})[b"comment"].decode() for f in schema}


def _not_null(table: str, cols: list[str]) -> tuple[Any, ...]:
    return tuple(
        constraint_record(
            ["main"], table, f"{c}_not_null", "NOT_NULL", [[c]], enforcement="ENFORCED", validation="ALL_ROWS"
        )
        for c in cols
    )


MEMBERS_TABLE = Table(
    name="members",
    function=MembersSource,
    comment="Every member of Congress since 1789, one row per person, keyed by bioguide_id, with cross-walk IDs.",
    constraints=(
        *_not_null("members", ["bioguide_id"]),
        constraint_record(
            ["main"], "members", "pk", "PRIMARY_KEY", [["bioguide_id"]], enforcement="ENFORCED", validation="ALL_ROWS"
        ),
    ),
    column_comments=_comments(MEMBER_SCHEMA),
    tags=function_tags(
        title="Members of Congress",
        category="members",
        keywords=(
            "members, legislators, senators, representatives, bioguide, congress-legislators, biography, "
            "icpsr, govtrack, fec id, wikidata"
        ),
        doc_llm=(
            "One row per person who has served in the US House or Senate since 1789 (about 12,800 rows), keyed "
            "by bioguide_id. Columns: names, birthday, gender, is_current (serving now), first_term_start, "
            "last_term_end, term counts per chamber, the latest term's chamber/state/party, and cross-walk IDs "
            "(thomas_id, lis_id for Senate votes, govtrack_id, icpsr_id for Voteview, opensecrets_id, "
            "votesmart_id, fec_ids list, cspan_id, wikidata_id, ...). For who served when, which state, district "
            "or party at a date, use member_terms instead. " + KEYLESS_NOTE
        ),
        doc_md=(
            "## members\n\nOne row per member of Congress since 1789, keyed by `bioguide_id`, with the "
            "identifiers other datasets use (ICPSR, GovTrack, FEC, OpenSecrets, Wikidata). " + _SOURCE_MD
        ),
    )
    | {
        "domain": "government",
        "vgi.example_queries": json.dumps(
            [
                {
                    "description": "Current senators by party",
                    "sql": (
                        f"SELECT latest_party, count(*) AS senators FROM {Q}.members "
                        "WHERE is_current AND latest_chamber = 'senate' GROUP BY latest_party ORDER BY senators DESC"
                    ),
                },
                {
                    "description": "Members with the most terms served",
                    "sql": (
                        f"SELECT bioguide_id, full_name, term_count FROM {Q}.members "
                        "ORDER BY term_count DESC, bioguide_id LIMIT 10"
                    ),
                },
            ]
        ),
    },
)

MEMBER_TERMS_TABLE = Table(
    name="member_terms",
    function=MemberTermsSource,
    comment="Every term served in the House or Senate since 1789: chamber, dates, state, district and party.",
    constraints=(
        *_not_null("member_terms", ["bioguide_id", "term_number", "chamber", "start_date", "state"]),
        constraint_record(
            ["main"],
            "member_terms",
            "pk",
            "PRIMARY_KEY",
            [["bioguide_id"], ["term_number"]],
            enforcement="ENFORCED",
            validation="ALL_ROWS",
        ),
        constraint_record(
            ["main"],
            "member_terms",
            "member_fk",
            "FOREIGN_KEY",
            [["bioguide_id"]],
            enforcement="ENFORCED",
            validation="ALL_ROWS",
            references=ReferencedKey(
                schema_path=["main"], table="members", columns=[["bioguide_id"]], constraint_id="pk"
            ),
        ),
    ),
    column_comments=_comments(TERM_SCHEMA),
    tags=function_tags(
        title="Terms Served in Congress",
        category="members",
        keywords="terms, service, tenure, districts, party affiliation, senate class, seniority, who served",
        doc_llm=(
            "One row per term served by a member of Congress since 1789 (about 45,500 rows), keyed by "
            "(bioguide_id, term_number). chamber is 'house' or 'senate'; start_date and end_date are DATEs "
            "(House terms run two years, Senate six; end_date is earlier when a member left early). state is the "
            "USPS code; district is the House district (0 at-large or delegate, -1 historic unknown, NULL for "
            "senators); senate_class is 1-3. party is the party at the start of the term. To find who served on "
            "a date D, filter start_date <= D AND end_date >= D; for a Congress N use the window starting "
            "January 3 of year 1789 + 2*(N-1). is_current marks terms in progress today. Join to members on "
            "bioguide_id. " + KEYLESS_NOTE
        ),
        doc_md=(
            "## member_terms\n\nOne row per term served, with chamber, dates, state, district, senate class, "
            "party and office contact details. " + _SOURCE_MD
        ),
    )
    | {
        "domain": "government",
        "vgi.example_queries": json.dumps(
            [
                {
                    "description": "Senators serving California on January 1, 2000",
                    "sql": (
                        f"SELECT bioguide_id, party, start_date, end_date FROM {Q}.member_terms "
                        "WHERE chamber = 'senate' AND state = 'CA' "
                        "AND DATE '2000-01-01' BETWEEN start_date AND end_date ORDER BY bioguide_id"
                    ),
                },
                {
                    "description": "Size of each party's House delegation in the 1st Congress",
                    "sql": (
                        f"SELECT party, count(DISTINCT bioguide_id) AS members FROM {Q}.member_terms "
                        "WHERE chamber = 'house' AND start_date < DATE '1791-03-04' AND end_date > DATE '1789-03-04' "
                        "GROUP BY party ORDER BY members DESC"
                    ),
                },
            ]
        ),
    },
)
