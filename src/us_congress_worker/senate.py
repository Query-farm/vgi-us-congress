"""The newest Senate roll-call votes, read from senate.gov's public XML.

* ``vote_menu_{congress}_{session}.xml`` lists every vote of a session.
* ``vote{congress}{session}/vote_{congress}_{session}_{nnnnn}.xml`` gives one vote
  with each senator's position, keyed by the Senate's ``lis_member_id``, which is
  mapped to ``bioguide_id`` with the congress-legislators cross-walk.

Senate.gov publishes a vote within hours; Voteview's ``rollcalls`` and
``member_votes`` cover the same votes (and all earlier ones) a little later.
"""

from __future__ import annotations

import datetime as dt
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pyarrow as pa
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import ProcessParams, bind_fixed_schema, init_single_worker

from .common import ArrowTableSource, memoized, to_int, to_text
from .fetch import Settings, cached_file, settings_of
from .legislators import load_legislators
from .meta import KEYLESS_NOTE, SENATE_DOCS, Q, column, function_tags

log = logging.getLogger(__name__)

VOTE_FILE_TTL_HOURS = 24.0 * 30
_MONTHS = {
    m: i for i, m in enumerate(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)
}

try:
    EASTERN: dt.tzinfo = ZoneInfo("America/New_York")
except ZoneInfoNotFoundError:  # pragma: no cover - only without tzdata
    EASTERN = dt.timezone(dt.timedelta(hours=-5), "EST")


def menu_url(settings: Settings, congress: int, session: int) -> str:
    """URL of a session's vote menu."""
    return f"{settings.senate_url}/roll_call_lists/vote_menu_{congress}_{session}.xml"


def vote_url(settings: Settings, congress: int, session: int, number: int) -> str:
    """URL of one vote's XML."""
    return f"{settings.senate_url}/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{number:05d}.xml"


def _text(node: ET.Element | None, path: str) -> str | None:
    if node is None:
        return None
    found = node.find(path)
    return to_text(" ".join(found.text.split())) if found is not None and found.text else None


def parse_menu(xml_bytes: bytes) -> list[dict[str, Any]]:
    """Parse a vote menu into rows ordered by vote number.

    The menu dates read ``09-Jan``; the year is the session's ``congress_year``,
    advanced by one when a later vote's month falls before an earlier one's (a
    session that runs into January of the next year).
    """
    root = ET.fromstring(xml_bytes)
    congress = to_int(_text(root, "congress"))
    session = to_int(_text(root, "session"))
    year = to_int(_text(root, "congress_year")) or 0
    votes: list[dict[str, Any]] = []
    for v in root.iterfind("votes/vote"):
        votes.append(
            {
                "congress": congress,
                "session": session,
                "vote_number": to_int(_text(v, "vote_number")),
                "raw_date": _text(v, "vote_date"),
                "issue": _text(v, "issue"),
                "question": _text(v, "question"),
                "result": _text(v, "result"),
                "yeas": to_int(_text(v, "vote_tally/yeas")),
                "nays": to_int(_text(v, "vote_tally/nays")),
                "title": _text(v, "title"),
            }
        )
    votes.sort(key=lambda r: r["vote_number"] or 0)
    last_month = 0
    for row in votes:
        raw = str(row.pop("raw_date") or "")
        day, _, mon = raw.partition("-")
        month = _MONTHS.get(mon[:3].title(), 0)
        if month and month < last_month:
            year += 1
        last_month = month or last_month
        row["vote_date"] = dt.date(year, month, int(day)) if month and day.isdigit() and year else None
    return votes


def _parse_when(text: str | None) -> dt.datetime | None:
    """Parse 'January 9, 2025,  02:54 PM' (Eastern) into an aware UTC datetime."""
    if not text:
        return None
    try:
        local = dt.datetime.strptime(" ".join(text.split()), "%B %d, %Y, %I:%M %p")
    except ValueError:
        return None
    return local.replace(tzinfo=EASTERN).astimezone(dt.UTC)


def parse_vote(xml_bytes: bytes, lis_to_bioguide: dict[str, str]) -> list[dict[str, Any]]:
    """Parse one vote file into one row per senator."""
    root = ET.fromstring(xml_bytes)
    common = {
        "congress": to_int(_text(root, "congress")),
        "session": to_int(_text(root, "session")),
        "vote_number": to_int(_text(root, "vote_number")),
        "vote_timestamp": _parse_when(_text(root, "vote_date")),
        "question": _text(root, "vote_question_text") or _text(root, "question"),
        "vote_title": _text(root, "vote_title"),
        "vote_result": _text(root, "vote_result"),
        "majority_requirement": _text(root, "majority_requirement"),
        "document_name": _text(root, "document/document_name"),
    }
    rows = []
    for m in root.iterfind("members/member"):
        lis = _text(m, "lis_member_id")
        rows.append(
            {
                **common,
                "lis_member_id": lis,
                "bioguide_id": lis_to_bioguide.get(lis or ""),
                "first_name": _text(m, "first_name"),
                "last_name": _text(m, "last_name"),
                "party": _text(m, "party"),
                "state": _text(m, "state"),
                "vote_cast": _text(m, "vote_cast"),
            }
        )
    return rows


SENATE_VOTE_SCHEMA = pa.schema(
    [
        column("congress", pa.int32(), "Congress number.", nullable=False),
        column("session", pa.int32(), "Session of the Congress (1 or 2).", nullable=False),
        column("vote_number", pa.int32(), "Senate roll-call number within the session.", nullable=False),
        column("vote_date", pa.date32(), "Date of the vote."),
        column("issue", pa.string(), "Measure or nomination voted on, e.g. 'S. 5', 'PN373'."),
        column("question", pa.string(), "The question, e.g. 'On the Cloture Motion', 'On Passage of the Bill'."),
        column("result", pa.string(), "Outcome, e.g. 'Agreed to', 'Rejected', 'Confirmed', 'Passed'."),
        column("yeas", pa.int32(), "Yea votes."),
        column("nays", pa.int32(), "Nay votes."),
        column("title", pa.string(), "Title of the vote, e.g. 'Motion to Invoke Cloture: ...'."),
    ]
)

POSITION_SCHEMA = pa.schema(
    [
        column("congress", pa.int32(), "Congress number.", nullable=False),
        column("session", pa.int32(), "Session of the Congress.", nullable=False),
        column("vote_number", pa.int32(), "Senate roll-call number within the session.", nullable=False),
        column("vote_timestamp", pa.timestamp("us", tz="UTC"), "When the vote was taken."),
        column("question", pa.string(), "Full question text, e.g. 'On Cloture on the Motion to Proceed S. 5'."),
        column("vote_title", pa.string(), "Title of the vote."),
        column("vote_result", pa.string(), "Outcome of the vote."),
        column("majority_requirement", pa.string(), "Fraction needed, e.g. '1/2' or '3/5'."),
        column("document_name", pa.string(), "Measure voted on, e.g. 'S. 5' or 'PN373'."),
        column("lis_member_id", pa.string(), "Senate LIS member ID, e.g. 'S428'.", nullable=False),
        column("bioguide_id", pa.string(), "Bioguide ID of the senator, via congress-legislators."),
        column("first_name", pa.string(), "Senator's first name."),
        column("last_name", pa.string(), "Senator's last name."),
        column("party", pa.string(), "Party letter: 'D', 'R' or 'I'."),
        column("state", pa.string(), "USPS state code."),
        column("vote_cast", pa.string(), "'Yea', 'Nay', 'Present', 'Not Voting' or 'Guilty'/'Not Guilty' at trials."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class SessionArgs:
    """``senate_votes(congress, session)``."""

    congress: Annotated[int, Arg(0, doc="Congress number; senate.gov XML covers the 101st (1989) onward.", ge=101)]
    session: Annotated[int, Arg(1, doc="Session of the Congress: 1 (odd year) or 2 (even year).", ge=1, le=3)]


@dataclass(slots=True, frozen=True, kw_only=True)
class VoteArgs:
    """``senate_vote_positions(congress, session, vote_number)``."""

    congress: Annotated[int, Arg(0, doc="Congress number; senate.gov XML covers the 101st (1989) onward.", ge=101)]
    session: Annotated[int, Arg(1, doc="Session of the Congress: 1 or 2.", ge=1, le=3)]
    vote_number: Annotated[int, Arg(2, doc="Senate roll-call number within the session, from senate_votes.", ge=1)]


_SOURCE_MD = f"Source: [senate.gov roll-call votes]({SENATE_DOCS}) (US government work, public domain)."


@bind_fixed_schema
@init_single_worker
class SenateVotes(ArrowTableSource[SessionArgs]):
    """``senate_votes(congress, session)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SENATE_VOTE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "senate_votes"
        description = "Every Senate roll-call vote of one session, straight from senate.gov, including today's votes"
        tags = function_tags(
            title="Senate Votes (senate.gov)",
            category="votes",
            keywords="senate votes, roll call, senate.gov, cloture, confirmation, nomination vote, latest votes",
            doc_llm=(
                "List the Senate's roll-call votes for one session from senate.gov's vote menu: "
                "senate_votes(congress, session), e.g. senate_votes(119, 1) for 2025. Columns: vote_number "
                "(restarts each session), vote_date, issue ('S. 5', 'H.R. 1', 'PN373'), question, result, yeas, "
                "nays, title. Fresher than Voteview (votes appear within hours). Pass vote_number to "
                "senate_vote_positions for each senator's position. Covers the 101st Congress (1989) on; use "
                "rollcalls(congress, chamber := 'senate') for earlier votes. " + KEYLESS_NOTE
            ),
            doc_md=(
                "## senate_votes(congress, session)\n\nThe Senate's list of roll-call votes for a session. "
                + _SOURCE_MD
            ),
            schema=SENATE_VOTE_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT vote_number, vote_date, question, result, yeas, nays FROM {Q}.senate_votes(119, 1) "
                    "ORDER BY vote_number DESC LIMIT 10"
                ),
                description="The last ten Senate votes of 2025",
            ),
            FunctionExample(
                sql=(
                    f"SELECT result, count(*) AS votes FROM {Q}.senate_votes(119, 1) "
                    "WHERE question ILIKE '%cloture%' GROUP BY result ORDER BY votes DESC"
                ),
                description="How cloture votes turned out in 2025",
            ),
        ]

    @classmethod
    def build(cls, params: ProcessParams[Any]) -> pa.Table:
        """Parse the session's vote menu."""
        settings = settings_of(params)
        path = cached_file(settings, menu_url(settings, params.args.congress, params.args.session))

        def build() -> pa.Table:
            return pa.Table.from_pylist(parse_menu(path.read_bytes()), schema=SENATE_VOTE_SCHEMA)

        result: pa.Table = memoized(("senate_menu",), [path], build)
        return result


@bind_fixed_schema
@init_single_worker
class SenateVotePositions(ArrowTableSource[VoteArgs]):
    """``senate_vote_positions(congress, session, vote_number)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = POSITION_SCHEMA

    class Meta:
        """Function metadata."""

        name = "senate_vote_positions"
        description = "How each senator voted on one Senate roll call, from senate.gov, with bioguide IDs"
        tags = function_tags(
            title="Senate Vote Positions (senate.gov)",
            category="votes",
            keywords="senator votes, positions, yea, nay, senate.gov, lis id, how did senators vote",
            doc_llm=(
                "One row per senator for one roll call: senate_vote_positions(congress, session, vote_number), "
                "e.g. (119, 1, 1). vote_cast is 'Yea', 'Nay', 'Present' or 'Not Voting' ('Guilty'/'Not Guilty' at "
                "impeachment trials). lis_member_id is mapped to bioguide_id through congress-legislators, so "
                "join members or member_terms on bioguide_id. Vote-level columns (vote_timestamp, question, "
                "vote_title, vote_result, majority_requirement, document_name) repeat on every row. Find "
                "vote numbers with senate_votes(congress, session). " + KEYLESS_NOTE
            ),
            doc_md=(
                "## senate_vote_positions(congress, session, vote_number)\n\nEach senator's position on one vote. "
                + _SOURCE_MD
            ),
            schema=POSITION_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT party, vote_cast, count(*) AS senators FROM {Q}.senate_vote_positions(119, 1, 1) "
                    "GROUP BY party, vote_cast ORDER BY party, vote_cast"
                ),
                description="The party breakdown of the first Senate vote of 2025",
            ),
            FunctionExample(
                sql=(
                    f"SELECT p.bioguide_id, m.full_name FROM {Q}.senate_vote_positions(119, 1, 1) p "
                    f"JOIN {Q}.members m USING (bioguide_id) WHERE p.vote_cast = 'Nay' ORDER BY m.full_name"
                ),
                description="Senators who voted nay on that vote, with their full names",
            ),
        ]

    @classmethod
    def build(cls, params: ProcessParams[Any]) -> pa.Table:
        """Parse the vote file and map senators to bioguide IDs."""
        settings = settings_of(params)
        a = params.args
        path = cached_file(
            settings, vote_url(settings, a.congress, a.session, a.vote_number), ttl_hours=VOTE_FILE_TTL_HOURS
        )
        lis = load_legislators(settings).lis_to_bioguide

        def build() -> pa.Table:
            return pa.Table.from_pylist(parse_vote(path.read_bytes(), lis), schema=POSITION_SCHEMA)

        result: pa.Table = memoized(("senate_vote", len(lis)), [path], build)
        return result
