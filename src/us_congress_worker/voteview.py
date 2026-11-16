"""House and Senate roll-call votes since 1789 from Voteview (Lewis, Poole, Rosenthal et al.).

Voteview publishes per-chamber, per-Congress CSV files (``H118_votes.csv``,
``S119_rollcalls.csv``, ...) and whole-history ``HSall_*`` files. A query
downloads only the files its arguments need into the cache: ``member_votes(118,
chamber := 'senate')`` fetches ``S118_votes.csv`` and ``S118_members.csv``, never
the 700 MB ``HSall_votes.csv``.

Please cite: Lewis, Jeffrey B., Keith Poole, Howard Rosenthal, Adam Boche, Aaron
Rudkin, and Luke Sonnet. Voteview: Congressional Roll-Call Votes Database.
https://voteview.com/
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, ClassVar, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
from vgi.arguments import Arg, Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction
from vgi.table_function import ProcessParams, bind_fixed_schema, init_single_worker

from .common import CHAMBER_DOC, CHAMBERS, ArrowTableSource, memoized
from .fetch import Settings, cached_file, settings_of
from .meta import KEYLESS_NOTE, VOTEVIEW_DOCS, Q, column, function_tags

CITATION = (
    "Lewis, Jeffrey B., Keith Poole, Howard Rosenthal, Adam Boche, Aaron Rudkin, and Luke Sonnet. "
    "Voteview: Congressional Roll-Call Votes Database. https://voteview.com/"
)
MAX_CONGRESS = 200

#: Voteview cast codes: (detailed label, simplified vote).
CAST_CODES: dict[int, tuple[str, str]] = {
    0: ("Not a member", "Not a member"),
    1: ("Yea", "Yea"),
    2: ("Paired Yea", "Yea"),
    3: ("Announced Yea", "Yea"),
    4: ("Announced Nay", "Nay"),
    5: ("Paired Nay", "Nay"),
    6: ("Nay", "Nay"),
    7: ("Present (some Congresses)", "Present"),
    8: ("Present (some Congresses)", "Present"),
    9: ("Not Voting", "Not Voting"),
}


def cast_labels(codes: pa.Array[Any], *, detailed: bool) -> pa.Array[Any]:
    """Map an integer array of cast codes to labels; unknown codes and NULLs become NULL."""
    keys = pa.array(list(CAST_CODES), pa.int32())
    values = pa.array([v[0] if detailed else v[1] for v in CAST_CODES.values()], pa.string())
    idx = pc.index_in(pc.cast(codes, pa.int32()), value_set=keys)
    return pc.take(values, idx)


# ---------------------------------------------------------------- files


def _prefix(chamber: str | None) -> list[str]:
    return {"house": ["H"], "senate": ["S"], None: ["H", "S"]}[chamber]


def voteview_file(settings: Settings, kind: str, congress: int | None, letter: str = "HS") -> Path:
    """Download/reuse one Voteview CSV: ``kind`` is votes, rollcalls, members or parties."""
    name = f"{letter}all_{kind}.csv" if congress is None else f"{letter}{congress:03d}_{kind}.csv"
    return cached_file(settings, f"{settings.voteview_url}/{kind}/{name}")


_INT: pa.DataType = pa.int32()
_F: pa.DataType = pa.float64()
_S: pa.DataType = pa.string()

ROLLCALL_TYPES: dict[str, pa.DataType] = {
    "congress": _INT,
    "chamber": _S,
    "rollnumber": _INT,
    "date": pa.date32(),
    "session": _INT,
    "clerk_rollnumber": _INT,
    "majority_requirement": _S,
    "yea_count": _INT,
    "nay_count": _INT,
    "nominate_mid_1": _F,
    "nominate_mid_2": _F,
    "nominate_spread_1": _F,
    "nominate_spread_2": _F,
    "nominate_log_likelihood": _F,
    "bill_number": _S,
    "vote_result": _S,
    "vote_desc": _S,
    "vote_question": _S,
    "dtl_desc": _S,
}
VOTE_TYPES: dict[str, pa.DataType] = {
    "congress": _INT,
    "chamber": _S,
    "rollnumber": _INT,
    "icpsr": pa.int64(),
    "cast_code": _INT,
    "prob": _F,
}
MEMBER_TYPES: dict[str, pa.DataType] = {
    "congress": _INT,
    "chamber": _S,
    "icpsr": pa.int64(),
    "state_icpsr": _INT,
    "district_code": _INT,
    "state_abbrev": _S,
    "party_code": _INT,
    "occupancy": _INT,
    "last_means": _INT,
    "bioname": _S,
    "bioguide_id": _S,
    "born": _F,
    "died": _F,
    "nominate_dim1": _F,
    "nominate_dim2": _F,
    "nominate_log_likelihood": _F,
    "nominate_geo_mean_probability": _F,
    "nominate_number_of_votes": _F,
    "nominate_number_of_errors": _F,
    "conditional": _S,
    "nokken_poole_dim1": _F,
    "nokken_poole_dim2": _F,
}
PARTY_TYPES: dict[str, pa.DataType] = {"party_code": _INT, "party_name": _S}


def read_csv(path: Path, types: dict[str, pa.DataType]) -> pa.Table:
    """Read a Voteview CSV with explicit column types (missing columns are added as NULL)."""
    header = path.open(encoding="utf-8").readline().strip().split(",")
    present = {k: v for k, v in types.items() if k in header}
    # Integer columns are read as float and cast: some Voteview files write codes as '200.0'.
    as_read = {k: (_F if pa.types.is_integer(v) else v) for k, v in present.items()}
    table = pacsv.read_csv(
        path,
        convert_options=pacsv.ConvertOptions(
            column_types=as_read,
            include_columns=list(present),
            strings_can_be_null=True,
            null_values=["", "NA", "N/A"],
        ),
    )
    for name, typ in present.items():
        if typ != as_read[name]:
            idx = table.schema.get_field_index(name)
            table = table.set_column(idx, name, pc.cast(table.column(name), typ, safe=False))
    for name, typ in types.items():
        if name not in present:
            table = table.append_column(pa.field(name, typ), pa.array([None] * table.num_rows, type=typ))
    return table


def _lower_chamber(table: pa.Table) -> pa.Table:
    idx = table.schema.get_field_index("chamber")
    return table.set_column(idx, "chamber", pc.utf8_lower(table.column("chamber")))


def _concat(paths: list[Path], types: dict[str, pa.DataType]) -> pa.Table:
    return pa.concat_tables([read_csv(p, types) for p in paths])


def party_names(settings: Settings) -> dict[int, str]:
    """party_code -> party name, from Voteview's parties file."""
    path = voteview_file(settings, "parties", None)

    def build() -> dict[int, str]:
        t = read_csv(path, PARTY_TYPES)
        return {
            int(c): str(n)
            for c, n in zip(t.column("party_code").to_pylist(), t.column("party_name").to_pylist(), strict=True)
            if c is not None and n is not None
        }

    result: dict[int, str] = memoized(("parties",), [path], build)
    return result


# ---------------------------------------------------------------- schemas

ROLLCALL_SCHEMA = pa.schema(
    [
        column("congress", _INT, "Congress number, e.g. 118 for 2023-2025.", nullable=False),
        column("chamber", _S, "'house' or 'senate'.", nullable=False),
        column("rollnumber", _INT, "Voteview roll-call number, sequential within the Congress.", nullable=False),
        column("date", pa.date32(), "Date of the vote."),
        column("session", _INT, "Session of the Congress (1 or 2; occasionally 3)."),
        column(
            "clerk_rollnumber",
            _INT,
            "The chamber clerk's roll-call number, which restarts each session; matches senate_votes.vote_number "
            "and house_votes.roll_call_number.",
        ),
        column("majority_requirement", _S, "Fraction needed to pass, e.g. '1/2', '3/5', '2/3'."),
        column("yea_count", _INT, "Members voting yea."),
        column("nay_count", _INT, "Members voting nay."),
        column("bill_number", _S, "Measure voted on, e.g. 'HR2882', 'S5' or a nomination 'PN11817'."),
        column("vote_result", _S, "Outcome as recorded, e.g. 'Passed', 'Cloture Motion Agreed to'."),
        column("vote_desc", _S, "Description of the measure or question."),
        column("vote_question", _S, "The question put, e.g. 'On Passage', 'On the Cloture Motion'."),
        column("dtl_desc", _S, "Detailed description, for historical votes."),
        column("nominate_mid_1", _F, "DW-NOMINATE cutting-line midpoint, dimension 1."),
        column("nominate_mid_2", _F, "DW-NOMINATE cutting-line midpoint, dimension 2."),
        column("nominate_spread_1", _F, "DW-NOMINATE spread, dimension 1."),
        column("nominate_spread_2", _F, "DW-NOMINATE spread, dimension 2."),
        column("nominate_log_likelihood", _F, "Log-likelihood of the vote under the DW-NOMINATE model."),
    ]
)

VOTE_SCHEMA = pa.schema(
    [
        column("congress", _INT, "Congress number.", nullable=False),
        column("chamber", _S, "'house' or 'senate'.", nullable=False),
        column("rollnumber", _INT, "Voteview roll-call number; joins to rollcalls.", nullable=False),
        column("icpsr", pa.int64(), "Member's ICPSR ID (Voteview's member key).", nullable=False),
        column("bioguide_id", _S, "Member's bioguide ID, from Voteview's member file for that Congress."),
        column("cast_code", _INT, "Voteview cast code 0-9; see vote_cast_label()."),
        column("vote", _S, "Simplified position: 'Yea', 'Nay', 'Present', 'Not Voting' or 'Not a member'."),
        column("vote_detail", _S, "Detailed position, e.g. 'Paired Yea', 'Announced Nay'."),
        column("prob", _F, "Probability (0-100) DW-NOMINATE assigns to the vote cast."),
    ]
)

IDEOLOGY_SCHEMA = pa.schema(
    [
        column("congress", _INT, "Congress number.", nullable=False),
        column("chamber", _S, "'house', 'senate' or 'president'.", nullable=False),
        column("icpsr", pa.int64(), "ICPSR member ID.", nullable=False),
        column("bioguide_id", _S, "Bioguide ID (NULL for presidents who never served in Congress)."),
        column("bioname", _S, "Name as 'LAST, First Middle', e.g. 'SANDERS, Bernard'."),
        column("state_abbrev", _S, "USPS state code ('USA' for presidents)."),
        column("district_code", _INT, "House district (0 at-large; 0 for senators)."),
        column("party_code", _INT, "Voteview party code, e.g. 100 Democrat, 200 Republican, 328 Independent."),
        column("party_name", _S, "Party name for party_code, from Voteview's parties file."),
        column("born", _INT, "Year of birth."),
        column("died", _INT, "Year of death."),
        column("nominate_dim1", _F, "DW-NOMINATE dimension 1 (economic liberal -1 to conservative +1)."),
        column("nominate_dim2", _F, "DW-NOMINATE dimension 2 (cross-cutting issues of the era)."),
        column("nominate_log_likelihood", _F, "Log-likelihood of the member's votes."),
        column("nominate_geo_mean_probability", _F, "Geometric mean probability of the member's votes."),
        column("nominate_number_of_votes", _INT, "Votes used to estimate the scores."),
        column("nominate_number_of_errors", _INT, "Votes the model predicts wrongly."),
        column("nokken_poole_dim1", _F, "Nokken-Poole (single-Congress) score, dimension 1."),
        column("nokken_poole_dim2", _F, "Nokken-Poole (single-Congress) score, dimension 2."),
        column("state_icpsr", _INT, "ICPSR state code."),
        column("occupancy", _INT, "ICPSR occupancy code for the seat (order of occupants)."),
        column("last_means", _INT, "ICPSR code for how the member last attained the seat."),
    ]
)


# ---------------------------------------------------------------- builders


def build_rollcalls(settings: Settings, congress: int | None, chamber: str | None) -> pa.Table:
    """Build the rollcalls result for one Congress (or all) and one chamber (or both)."""
    if congress is None:
        paths = [voteview_file(settings, "rollcalls", None)]
    else:
        paths = [voteview_file(settings, "rollcalls", congress, p) for p in _prefix(chamber)]

    def build() -> pa.Table:
        t = _lower_chamber(_concat(paths, ROLLCALL_TYPES))
        if chamber:
            t = t.filter(pc.equal(t.column("chamber"), pa.scalar(chamber)))
        return t.select(ROLLCALL_SCHEMA.names).cast(ROLLCALL_SCHEMA)

    result: pa.Table = memoized(("rollcalls", congress, chamber), paths, build)
    return result


def build_member_votes(settings: Settings, congress: int, chamber: str | None) -> pa.Table:
    """Each member's vote on each roll call of one Congress, with bioguide IDs joined in."""
    prefixes = _prefix(chamber)
    votes = [voteview_file(settings, "votes", congress, p) for p in prefixes]
    members = [voteview_file(settings, "members", congress, p) for p in prefixes]

    def build() -> pa.Table:
        v = _concat(votes, VOTE_TYPES)
        m = _concat(members, MEMBER_TYPES).select(["congress", "chamber", "icpsr", "bioguide_id"])
        # One bioguide per (congress, chamber, icpsr); presidents appear in both chamber files.
        m = m.group_by(["congress", "chamber", "icpsr"]).aggregate([("bioguide_id", "max")])
        m = m.rename_columns(["congress", "chamber", "icpsr", "bioguide_id"])
        joined = v.join(m, keys=["congress", "chamber", "icpsr"], join_type="left outer")
        joined = joined.sort_by([("chamber", "ascending"), ("rollnumber", "ascending"), ("icpsr", "ascending")])
        joined = _lower_chamber(joined)
        codes = joined.column("cast_code").combine_chunks()
        joined = joined.append_column("vote", cast_labels(codes, detailed=False))
        joined = joined.append_column("vote_detail", cast_labels(codes, detailed=True))
        return joined.select(VOTE_SCHEMA.names).cast(VOTE_SCHEMA)

    result: pa.Table = memoized(("member_votes", congress, chamber), votes + members, build)
    return result


def build_ideology(settings: Settings, congress: int | None, chamber: str | None) -> pa.Table:
    """Voteview member rows with DW-NOMINATE scores and party names."""
    if congress is None:
        paths = [voteview_file(settings, "members", None)]
    else:
        paths = [voteview_file(settings, "members", congress, p) for p in _prefix(chamber)]
    parties = party_names(settings)

    def build() -> pa.Table:
        tables = [_lower_chamber(read_csv(p, MEMBER_TYPES)) for p in paths]
        # Presidents are listed in both chamber files; keep the House file's row.
        tables[1:] = [x.filter(pc.not_equal(x.column("chamber"), pa.scalar("president"))) for x in tables[1:]]
        t = pa.concat_tables(tables)
        if chamber:
            t = t.filter(pc.equal(t.column("chamber"), pa.scalar(chamber)))
        names = pa.array([parties.get(c) if c is not None else None for c in t.column("party_code").to_pylist()])
        t = t.append_column("party_name", names)
        t = t.sort_by([("congress", "ascending"), ("chamber", "ascending"), ("icpsr", "ascending")])
        out = {name: t.column(name) for name in IDEOLOGY_SCHEMA.names}
        for name in ("born", "died", "nominate_number_of_votes", "nominate_number_of_errors"):
            out[name] = pc.cast(t.column(name), _INT)
        return pa.table(out).cast(IDEOLOGY_SCHEMA)

    result: pa.Table = memoized(("ideology", congress, chamber), paths, build)
    return result


# ---------------------------------------------------------------- arguments


CONGRESS_DOC = "Congress number, 1 (1789-1791) to the current one; e.g. 118 for 2023-2025."


@dataclass(slots=True, frozen=True, kw_only=True)
class CongressChamberArgs:
    """``fn(congress, chamber := NULL)``."""

    congress: Annotated[int, Arg(0, doc=CONGRESS_DOC, ge=1, le=MAX_CONGRESS)]
    chamber: Annotated[str | None, Arg("chamber", default=None, doc=CHAMBER_DOC, choices=list(CHAMBERS))]


@dataclass(slots=True, frozen=True, kw_only=True)
class ChamberArgs:
    """``fn(chamber := NULL)`` over every Congress."""

    chamber: Annotated[str | None, Arg("chamber", default=None, doc=CHAMBER_DOC, choices=list(CHAMBERS))]


def _congress(params: ProcessParams[Any]) -> int | None:
    return cast("int | None", getattr(params.args, "congress", None))


_VV_SOURCE = (
    f"Source: [Voteview]({VOTEVIEW_DOCS}), updated as votes are taken; files are cached per user. Please cite: "
    + CITATION
)

_ROLLCALL_DOC_LLM = (
    "Roll-call votes from Voteview, one row per vote, House and Senate, from the 1st Congress (1789) to today. "
    "rollcalls(congress) reads one Congress; rollcalls() with no argument reads every Congress (a 30 MB "
    "download). chamber := 'house' or 'senate' limits to one chamber. Key: (congress, chamber, rollnumber); "
    "rollnumber is Voteview's sequential number, clerk_rollnumber the clerk's per-session number (matches "
    "senate_votes.vote_number). Columns include date, session, majority_requirement, yea_count, nay_count, "
    "bill_number ('HR2882', 'S5', nominations 'PN...'), vote_result, vote_desc, vote_question and DW-NOMINATE "
    "cutting-line parameters. Join member_votes on (congress, chamber, rollnumber) for individual positions. "
    + KEYLESS_NOTE
)


class _RollcallsBase(ArrowTableSource[Any]):
    FIXED_SCHEMA: ClassVar[pa.Schema] = ROLLCALL_SCHEMA

    @classmethod
    def build(cls, params: ProcessParams[Any]) -> pa.Table:
        """Return the roll calls the arguments select."""
        return build_rollcalls(settings_of(params), _congress(params), params.args.chamber)


_ROLLCALL_EXAMPLES = [
    FunctionExample(
        sql=(
            f"SELECT vote_result, count(*) AS votes FROM {Q}.rollcalls(118, chamber := 'senate') "
            "GROUP BY vote_result ORDER BY votes DESC LIMIT 5"
        ),
        description="The most common outcomes of Senate roll calls in the 118th Congress",
    ),
    FunctionExample(
        sql=(
            f"SELECT congress, count(*) AS roll_calls FROM {Q}.rollcalls(chamber := 'house') "
            "WHERE congress >= 110 GROUP BY congress ORDER BY congress"
        ),
        description="House roll calls per Congress since the 110th",
    ),
]

_ROLLCALL_TAGS: dict[str, Any] = {
    "title": "Roll-Call Votes (Voteview)",
    "category": "votes",
    "keywords": "roll call, votes, voteview, senate votes, house votes, yea, nay, cloture, history",
    "doc_llm": _ROLLCALL_DOC_LLM,
    "doc_md": (
        "## rollcalls(congress, chamber := NULL) / rollcalls(chamber := NULL)\n\n"
        "Every House and Senate roll call since 1789 with tallies, the question and the result. " + _VV_SOURCE
    ),
    "schema": ROLLCALL_SCHEMA,
}


@bind_fixed_schema
@init_single_worker
class Rollcalls(_RollcallsBase):
    """``rollcalls(congress, chamber := NULL)``."""

    FunctionArguments = CongressChamberArgs

    class Meta:
        """Function metadata."""

        name = "rollcalls"
        description = "House and Senate roll-call votes of one Congress from Voteview, with tallies and results"
        tags = function_tags(**_ROLLCALL_TAGS)
        examples = _ROLLCALL_EXAMPLES


@bind_fixed_schema
@init_single_worker
class RollcallsAll(_RollcallsBase):
    """``rollcalls(chamber := NULL)`` over every Congress."""

    FunctionArguments = ChamberArgs

    class Meta:
        """Function metadata."""

        name = "rollcalls"
        description = "Every House and Senate roll-call vote since 1789 from Voteview, with tallies and results"
        tags = function_tags(**_ROLLCALL_TAGS)
        examples = _ROLLCALL_EXAMPLES


@bind_fixed_schema
@init_single_worker
class MemberVotes(ArrowTableSource[CongressChamberArgs]):
    """``member_votes(congress, chamber := NULL)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = VOTE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "member_votes"
        description = "How every member voted on every roll call of one Congress, from Voteview, with bioguide IDs"
        tags = function_tags(
            title="Member Votes (Voteview)",
            category="votes",
            keywords="member votes, voting record, positions, yea, nay, abstain, roll call, party loyalty",
            doc_llm=(
                "One row per member per roll call for one Congress (required positional argument; the House "
                "alone has about 300,000 rows per Congress). chamber := 'house' or 'senate' limits to one chamber "
                "and one download. Columns: congress, chamber, rollnumber (join rollcalls on congress, chamber, "
                "rollnumber), icpsr, bioguide_id (join members/member_terms), cast_code (0-9), vote (simplified: "
                "'Yea', 'Nay', 'Present', 'Not Voting', 'Not a member'), vote_detail ('Paired Yea', 'Announced "
                "Nay', ...) and prob. Count vote = 'Yea' per member for voting records; compare with the party "
                "majority for party-unity scores. " + KEYLESS_NOTE
            ),
            doc_md=(
                "## member_votes(congress, chamber := NULL)\n\n"
                "Each member's position on each roll call, with cast codes decoded and bioguide IDs attached. "
                + _VV_SOURCE
            ),
            schema=VOTE_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT bioguide_id, count(*) FILTER (WHERE vote = 'Not Voting') AS missed, count(*) AS votes "
                    f"FROM {Q}.member_votes(118, chamber := 'senate') GROUP BY bioguide_id "
                    "ORDER BY missed DESC LIMIT 5"
                ),
                description="Senators who missed the most roll calls in the 118th Congress",
            ),
            FunctionExample(
                sql=(
                    f"SELECT v.vote, count(*) AS members FROM {Q}.member_votes(119, chamber := 'senate') v "
                    "WHERE v.rollnumber = 1 GROUP BY v.vote ORDER BY members DESC"
                ),
                description="How the Senate split on the first roll call of the 119th Congress",
            ),
        ]

    @classmethod
    def build(cls, params: ProcessParams[Any]) -> pa.Table:
        """Return the member votes the arguments select."""
        return build_member_votes(settings_of(params), params.args.congress, params.args.chamber)


_IDEOLOGY_TAGS: dict[str, Any] = {
    "title": "Member Ideology (DW-NOMINATE)",
    "category": "members",
    "keywords": "dw-nominate, ideology, liberal, conservative, polarization, voteview, nokken-poole, party",
    "doc_llm": (
        "Voteview's member list with DW-NOMINATE ideal points: one row per member per Congress per chamber "
        "(presidents appear with chamber 'president'). member_ideology(congress) reads one Congress; "
        "member_ideology() reads all (a 6 MB download). chamber := 'house' or 'senate' narrows it. nominate_dim1 "
        "runs from about -1 (liberal) to +1 (conservative) and is comparable across Congresses; nokken_poole_dim1 "
        "is estimated per Congress. party_code is Voteview's (100 Democrat, 200 Republican, 328 Independent) and "
        "party_name decodes it. bioguide_id joins to members. Use for polarization and party-median questions. "
        + KEYLESS_NOTE
    ),
    "doc_md": (
        "## member_ideology(congress, chamber := NULL) / member_ideology(chamber := NULL)\n\n"
        "DW-NOMINATE and Nokken-Poole ideology scores for every member of every Congress. " + _VV_SOURCE
    ),
    "schema": IDEOLOGY_SCHEMA,
}
_IDEOLOGY_EXAMPLES = [
    FunctionExample(
        sql=(
            f"SELECT party_name, round(median(nominate_dim1), 3) AS median_dim1 "
            f"FROM {Q}.member_ideology(118, chamber := 'house') GROUP BY party_name ORDER BY median_dim1"
        ),
        description="Party medians on the first DW-NOMINATE dimension in the 118th House",
    ),
    FunctionExample(
        sql=(
            f"SELECT congress, round(median(nominate_dim1) FILTER (WHERE party_code = 200) - "
            "median(nominate_dim1) FILTER (WHERE party_code = 100), 3) AS gap "
            f"FROM {Q}.member_ideology(chamber := 'senate') WHERE congress >= 100 GROUP BY congress ORDER BY congress"
        ),
        description="The gap between Senate Republican and Democratic medians since the 100th Congress",
    ),
]


class _IdeologyBase(ArrowTableSource[Any]):
    FIXED_SCHEMA: ClassVar[pa.Schema] = IDEOLOGY_SCHEMA

    @classmethod
    def build(cls, params: ProcessParams[Any]) -> pa.Table:
        """Return the members the arguments select."""
        return build_ideology(settings_of(params), _congress(params), params.args.chamber)


@bind_fixed_schema
@init_single_worker
class MemberIdeology(_IdeologyBase):
    """``member_ideology(congress, chamber := NULL)``."""

    FunctionArguments = CongressChamberArgs

    class Meta:
        """Function metadata."""

        name = "member_ideology"
        description = "DW-NOMINATE ideology scores and party of every member of one Congress, from Voteview"
        tags = function_tags(**_IDEOLOGY_TAGS)
        examples = _IDEOLOGY_EXAMPLES


@bind_fixed_schema
@init_single_worker
class MemberIdeologyAll(_IdeologyBase):
    """``member_ideology(chamber := NULL)`` over every Congress."""

    FunctionArguments = ChamberArgs

    class Meta:
        """Function metadata."""

        name = "member_ideology"
        description = "DW-NOMINATE ideology scores and party of every member of every Congress, from Voteview"
        tags = function_tags(**_IDEOLOGY_TAGS)
        examples = _IDEOLOGY_EXAMPLES


class VoteCastLabel(ScalarFunction):
    """``vote_cast_label(cast_code)``: decode a Voteview cast code."""

    class Meta:
        """Function metadata."""

        name = "vote_cast_label"
        description = "Decode a Voteview cast code (0-9) into its label: 1 is 'Yea', 6 is 'Nay', 9 is 'Not Voting'"
        tags = function_tags(
            title="Vote Cast Code Label",
            category="utilities",
            keywords="cast code, voteview, decode, yea, nay, paired, announced, not voting",
            doc_llm=(
                "Return Voteview's label for a cast code: 0 'Not a member', 1 'Yea', 2 'Paired Yea', 3 'Announced "
                "Yea', 4 'Announced Nay', 5 'Paired Nay', 6 'Nay', 7 and 8 'Present (some Congresses)', 9 'Not "
                "Voting'. Other codes and NULL give NULL. member_votes already carries the decoded vote and "
                "vote_detail columns; use this on raw Voteview data. Runs locally, no network."
            ),
            doc_md=(
                "## vote_cast_label(cast_code)\n\nThe Voteview label for a cast code, e.g. "
                "`vote_cast_label(6)` = `'Nay'`."
            ),
            executable_examples=[
                {
                    "description": "Decode three cast codes",
                    "sql": (f"SELECT c, {Q}.vote_cast_label(c) AS label FROM (VALUES (1), (6), (9)) t(c) ORDER BY c"),
                    "expected_result": [[1, "Yea"], [6, "Nay"], [9, "Not Voting"]],
                },
            ],
        )
        examples = [
            FunctionExample(
                sql=f"SELECT {Q}.vote_cast_label(2) AS label",
                description="What a paired yea is called",
            ),
            FunctionExample(
                sql=(f"SELECT c AS cast_code, {Q}.vote_cast_label(c) AS label FROM range(10) t(c) ORDER BY cast_code"),
                description="List all ten Voteview cast codes",
            ),
        ]

    @classmethod
    def compute(
        cls,
        cast_code: Annotated[pa.Int64Array, Param(doc="Voteview cast code, 0 to 9; NULL in, NULL out.", ge=0, le=9)],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Decode every code in the column."""
        return cast("pa.StringArray", cast_labels(cast_code, detailed=True))
