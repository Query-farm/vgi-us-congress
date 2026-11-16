"""Bills, laws, amendments, committees, nominations, treaties and House votes from the Congress.gov API v3.

Every function here needs an api.data.gov key in a DuckDB secret::

    CREATE SECRET (TYPE us_congress, API_KEY 'your-key');

The key is read from the secret on every request and sent in the ``X-Api-Key``
header; it is never stored in the scan cursor. A missing secret fails the scan
(not the bind, so ``DESCRIBE`` and ``EXPLAIN`` work without one) with a message
naming the secret type.

**Paging.** Lists are read 250 rows per request (the API maximum). The cursor is
the resolved request URL and the next offset, so a scan resumes on any worker
process over HTTP transport.

**Pushdown.** List functions take the upstream's path filters as optional named
arguments (``bills(congress := 118, bill_type := 'hr')``) and also accept them as
``WHERE`` predicates: ``congress = N``, ``bill_type = 'hr'`` and ranges on
``update_date`` become the request path and ``fromDateTime`` / ``toDateTime``.
DuckDB re-applies every predicate to the rows returned, so a translation only
narrows the request.
"""

from __future__ import annotations

import datetime as dt
import logging
import urllib.error
import urllib.parse
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar, TypeVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.catalog import SecretTypeSpec
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_filter_pushdown import AndFilter, ComparisonOp, ConstantFilter, Filter
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .common import chamber_name, parse_date, parse_timestamp, to_int, to_text
from .fetch import get_json, settings_of
from .meta import API_KEY_NOTE, API_KEY_SIGNUP, CONGRESS_GOV_DOCS, SOURCE_URL, Q, column, function_tags

log = logging.getLogger(__name__)

SECRET_TYPE = "us_congress"
PAGE_LIMIT = 250

CONGRESS_SECRET = SecretTypeSpec(
    name=SECRET_TYPE,
    description="Congress.gov API key (a free api.data.gov key) for the bill, law, committee and House vote functions.",
    schema=pa.schema(
        [
            pa.field(
                "api_key",
                pa.string(),
                metadata={
                    "redact": "true",
                    "vgi_doc": f"api.data.gov key for the Congress.gov API v3; sign up free at {API_KEY_SIGNUP}",
                },
            )
        ],
        metadata={"vgi_doc_url": f"{SOURCE_URL}#api-key"},
    ),
)

BILL_TYPES = ["hr", "s", "hjres", "sjres", "hconres", "sconres", "hres", "sres"]
AMENDMENT_TYPES = ["hamdt", "samdt", "suamdt"]

MISSING_KEY = (
    "the Congress.gov functions need an API key: run CREATE SECRET (TYPE us_congress, API_KEY '<your api.data.gov "
    f"key>') first; a free key is issued at {API_KEY_SIGNUP}"
)


class CongressGovError(RuntimeError):
    """The Congress.gov API refused or failed a request."""


def api_key(params: ProcessParams[Any]) -> str:
    """Return the API key from the ``us_congress`` secret, or raise naming the secret type."""
    for secret in params.secrets.of_type(SECRET_TYPE):
        value = secret.get("api_key")
        if value is not None and value.as_py():
            return str(value.as_py())
    raise CongressGovError(MISSING_KEY)


def fetch_page(url: str, key: str) -> Any:
    """GET one Congress.gov page, translating HTTP errors into clear messages."""
    try:
        return get_json(url, {"X-Api-Key": key})
    except urllib.error.HTTPError as err:
        body = err.read(400).decode(errors="replace")
        if err.code in (401, 403):
            raise CongressGovError(
                f"Congress.gov rejected the API key (HTTP {err.code}); check the us_congress secret: {body}"
            ) from err
        if err.code == 429:
            raise CongressGovError(
                "Congress.gov rate limit reached (HTTP 429; api.data.gov allows 5,000 requests per hour per key). "
                "Narrow the query with congress / bill_type / update_date filters or try again later."
            ) from err
        if err.code == 404:
            raise CongressGovError(f"Congress.gov has no such record (HTTP 404): {body}") from err
        raise CongressGovError(f"Congress.gov request failed with HTTP {err.code}: {body}") from err


# ---------------------------------------------------------------- pushdown


def _flat(filters: Any) -> list[Filter]:
    out: list[Filter] = []
    stack = list(filters.filters) if filters is not None else []
    while stack:
        f = stack.pop()
        if isinstance(f, AndFilter):
            stack.extend(f.children)
        else:
            out.append(f)
    return out


def pushed_eq(params: ProcessParams[Any], column_name: str) -> Any:
    """Return the constant of a ``column = constant`` predicate DuckDB pushed down, if any."""
    for f in _flat(params.current_pushdown_filters):
        if isinstance(f, ConstantFilter) and f.column_name == column_name and f.op == ComparisonOp.EQ:
            return f.value.as_py()
    return None


def pushed_range(params: ProcessParams[Any], column_name: str) -> tuple[dt.datetime | None, dt.datetime | None]:
    """Return the tightest lower/upper timestamp bounds pushed down on ``column_name``."""
    lo: dt.datetime | None = None
    hi: dt.datetime | None = None
    for f in _flat(params.current_pushdown_filters):
        if not isinstance(f, ConstantFilter) or f.column_name != column_name:
            continue
        value = f.value.as_py()
        if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
            value = dt.datetime(value.year, value.month, value.day, tzinfo=dt.UTC)
        if not isinstance(value, dt.datetime):
            continue
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        if f.op in (ComparisonOp.GT, ComparisonOp.GE, ComparisonOp.EQ):
            lo = value if lo is None or value > lo else lo
        if f.op in (ComparisonOp.LT, ComparisonOp.LE, ComparisonOp.EQ):
            hi = value if hi is None or value < hi else hi
    return lo, hi


def _api_time(value: dt.datetime, *, upper: bool) -> str:
    """Format a bound as the API wants it, rounded outward to whole seconds (keeps a superset)."""
    value = value.astimezone(dt.UTC)
    if upper and value.microsecond:
        value = value + dt.timedelta(seconds=1)
    return value.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def date_window(params: ProcessParams[Any], column_name: str = "update_date") -> dict[str, str]:
    """``fromDateTime`` / ``toDateTime`` query parameters from pushed bounds on ``column_name``."""
    lo, hi = pushed_range(params, column_name)
    query: dict[str, str] = {}
    if lo is not None:
        # The list endpoints report some update dates at day precision: widen the window to whole days.
        query["fromDateTime"] = _api_time(lo.replace(hour=0, minute=0, second=0, microsecond=0), upper=False)
    if hi is not None:
        end = hi.replace(hour=23, minute=59, second=59, microsecond=0)
        query["toDateTime"] = _api_time(end, upper=True)
    return query


# ---------------------------------------------------------------- the paged base


@dataclass(kw_only=True)
class PageCursor(ArrowSerializableDataclass):
    """Cursor: the resolved request URL (no key) and the next offset."""

    url: str = ""
    offset: int = 0
    done: bool = False


A = TypeVar("A")


class CongressGovSource(TableFunctionGenerator[A, PageCursor]):
    """Base for Congress.gov table functions: resolve a request, page it, map items to rows."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([])
    PAGED: ClassVar[bool] = True

    @classmethod
    def on_bind(cls, params: BindParams[A]) -> BindResponse:
        """Declare the output schema (the secret is requested through ``Meta.required_secrets``)."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """Return the API path (after ``/v3``) and extra query parameters."""
        raise NotImplementedError

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """Extract the list of items from one response."""
        raise NotImplementedError

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """Map one item to a row of ``FIXED_SCHEMA``."""
        raise NotImplementedError

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> PageCursor:
        """Resolve the request from the arguments and pushed-down filters."""
        path, query = cls.endpoint(params)
        base = settings_of(params).congress_gov_url
        qs = urllib.parse.urlencode({"format": "json", **query})
        return PageCursor(url=f"{base}{path}?{qs}")

    @classmethod
    def process(cls, params: ProcessParams[Any], state: PageCursor, out: OutputCollector) -> None:
        """Fetch one page and emit it; finish when the API has no next page."""
        if state.done:
            out.finish()
            return
        key = api_key(params)
        url = f"{state.url}&offset={state.offset}&limit={PAGE_LIMIT}" if cls.PAGED else state.url
        body = fetch_page(url, key)
        items = cls.items(body, state.offset)
        rows = [cls.row(item, params) for item in items]
        has_next = bool(((body.get("pagination") or {}) if isinstance(body, dict) else {}).get("next"))
        state.offset += len(items)
        state.done = not (cls.PAGED and has_next and items)
        if rows:
            out.emit(pa.RecordBatch.from_pylist(rows, schema=cls.FIXED_SCHEMA).select(params.output_schema.names))
        if state.done:
            out.finish()


# ---------------------------------------------------------------- shared row helpers


def _latest(item: dict[str, Any]) -> tuple[dt.date | None, str | None]:
    la = item.get("latestAction") or {}
    return parse_date(la.get("actionDate")), to_text(la.get("text"))


def bill_id(bill_type: str | None, number: Any, congress: Any) -> str | None:
    """Return the unitedstates-project bill ID, e.g. ``hr2882-118``."""
    if not bill_type or number in (None, "") or congress in (None, ""):
        return None
    return f"{bill_type.lower()}{number}-{congress}"


def _lower(value: Any) -> str | None:
    text = to_text(value)
    return text.lower() if text else None


CONGRESS_COL = column("congress", pa.int32(), "Congress number, e.g. 118.")
BILL_ID_COLS = [
    column("congress", pa.int32(), "Congress number, e.g. 118.", nullable=False),
    column(
        "bill_type", pa.string(), "Bill type code: hr, s, hjres, sjres, hconres, sconres, hres, sres.", nullable=False
    ),
    column("bill_number", pa.int32(), "Bill number within its type and Congress, e.g. 2882.", nullable=False),
]


@dataclass(slots=True, frozen=True, kw_only=True)
class BillRefArgs:
    """``fn(congress, bill_type, bill_number)``."""

    congress: Annotated[int, Arg(0, doc="Congress number, e.g. 118 (2023-2025).", ge=1, le=200)]
    bill_type: Annotated[
        str,
        Arg(
            1,
            doc="Bill type code: 'hr', 's', 'hjres', 'sjres', 'hconres', 'sconres', 'hres', 'sres'.",
            choices=BILL_TYPES,
        ),
    ]
    bill_number: Annotated[int, Arg(2, doc="Bill number, e.g. 2882 for H.R. 2882.", ge=1)]


def _bill_path(params: ProcessParams[Any], suffix: str = "") -> str:
    a = params.args
    return f"/bill/{a.congress}/{a.bill_type}/{a.bill_number}{suffix}"


def _bill_ref(params: ProcessParams[Any]) -> dict[str, Any]:
    a = params.args
    return {"congress": a.congress, "bill_type": a.bill_type, "bill_number": a.bill_number}


def _md_source(extra: str = "") -> str:
    return (
        f"{extra}Source: [Congress.gov API v3]({CONGRESS_GOV_DOCS}) (US government work, public domain), read live. "
        f"Needs a `us_congress` secret with a free [api.data.gov key]({API_KEY_SIGNUP})."
    )


CG_KEYWORDS = "congress.gov, api, library of congress, "

# ---------------------------------------------------------------- bills

BILLS_SCHEMA = pa.schema(
    [
        *BILL_ID_COLS,
        column("bill_id", pa.string(), "Bill ID as '<type><number>-<congress>', e.g. 'hr2882-118'.", nullable=False),
        column("title", pa.string(), "Bill title (the latest official or display title)."),
        column("origin_chamber", pa.string(), "'house' or 'senate': where the bill was introduced."),
        column("introduced_date", pa.date32(), "Date introduced."),
        column("latest_action_date", pa.date32(), "Date of the latest action."),
        column("latest_action_text", pa.string(), "Text of the latest action, e.g. 'Became Public Law No: 118-47.'."),
        column("update_date", pa.timestamp("us", tz="UTC"), "When Congress.gov last updated the bill record."),
        column("update_date_including_text", pa.timestamp("us", tz="UTC"), "Last update including bill text."),
        column("api_url", pa.string(), "Congress.gov API URL of the bill detail."),
    ]
)


def _bill_list_row(item: dict[str, Any]) -> dict[str, Any]:
    bill_type = _lower(item.get("type"))
    date, text = _latest(item)
    return {
        "congress": to_int(item.get("congress")),
        "bill_type": bill_type,
        "bill_number": to_int(item.get("number")),
        "bill_id": bill_id(bill_type, item.get("number"), item.get("congress")),
        "title": to_text(item.get("title")),
        "origin_chamber": chamber_name(item.get("originChamber")),
        "introduced_date": parse_date(item.get("introducedDate")),
        "latest_action_date": date,
        "latest_action_text": text,
        "update_date": parse_timestamp(item.get("updateDate")),
        "update_date_including_text": parse_timestamp(item.get("updateDateIncludingText")),
        "api_url": to_text(item.get("url")),
    }


@dataclass(slots=True, frozen=True, kw_only=True)
class BillsArgs:
    """``bills(congress := NULL, bill_type := NULL)``."""

    congress: Annotated[
        int | None,
        Arg("congress", default=None, doc="Only this Congress (or filter WHERE congress = N).", ge=1, le=200),
    ]
    bill_type: Annotated[
        str | None,
        Arg(
            "bill_type",
            default=None,
            doc="Only this bill type (needs congress): 'hr', 's', 'hjres', ...",
            choices=BILL_TYPES,
        ),
    ]


def _congress_and(params: ProcessParams[Any], name: str, col: str | None = None) -> tuple[int | None, Any]:
    """Return the Congress and one other path filter, from the arguments or pushed ``=`` predicates."""
    congress = params.args.congress if params.args.congress is not None else to_int(pushed_eq(params, "congress"))
    other = getattr(params.args, name, None)
    if other is None:
        other = pushed_eq(params, col or name)
    return congress, other


@init_single_worker
class Bills(CongressGovSource[Any]):
    """``bills(congress := NULL, bill_type := NULL)``."""

    FunctionArguments = BillsArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = BILLS_SCHEMA

    class Meta:
        """Function metadata."""

        name = "bills"
        required_secrets = [SECRET_TYPE]
        description = (
            "List bills and resolutions from Congress.gov, newest update first, with filters pushed into the API"
        )
        filter_pushdown = True
        auto_apply_filters = True
        tags = function_tags(
            title="Bills (Congress.gov)",
            category="legislation",
            keywords=CG_KEYWORDS + "bills, resolutions, legislation, hr, senate bill, introduced, latest action",
            doc_llm=(
                "List bills and resolutions from Congress.gov, most recently updated first. Narrow it with "
                "congress := N and bill_type := 'hr' | 's' | 'hjres' | 'sjres' | 'hconres' | 'sconres' | 'hres' | "
                "'sres', or with WHERE congress = N AND bill_type = '...' (pushed into the request path) and WHERE "
                "update_date >= / <= `TIMESTAMPTZ` (pushed as fromDateTime/toDateTime). Without a congress filter it "
                "pages through every bill since 1973, 250 per request, so always filter or LIMIT. Columns: "
                "congress, bill_type, bill_number, bill_id ('hr2882-118'), title, origin_chamber, introduced_date, "
                "latest_action_date, latest_action_text, update_date. For sponsor, policy area and status use "
                "bill(congress, bill_type, bill_number). " + API_KEY_NOTE
            ),
            doc_md=(
                "## bills(congress := NULL, bill_type := NULL)\n\nBills and resolutions with their latest action. "
                "`congress`, `bill_type` and `update_date` filters are pushed into the API request. " + _md_source()
            ),
            schema=BILLS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT bill_id, title, latest_action_date FROM {Q}.bills(congress := 118, bill_type := 'hjres') "
                    "ORDER BY latest_action_date DESC LIMIT 10"
                ),
                description="The ten House joint resolutions of the 118th Congress with the most recent action",
            ),
            FunctionExample(
                sql=(
                    f"SELECT bill_type, count(*) AS bills FROM {Q}.bills() WHERE congress = 119 "
                    "AND update_date >= TIMESTAMPTZ '2026-11-01 00:00:00+00' GROUP BY bill_type ORDER BY bills DESC"
                ),
                description="Bills of the 119th Congress updated since November 1, 2026, by type",
            ),
        ]

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """``/bill[/congress[/type]]`` plus the pushed update_date window."""
        congress, bill_type = _congress_and(params, "bill_type")
        path = "/bill"
        if congress is not None:
            path += f"/{congress}"
            if bill_type:
                path += f"/{str(bill_type).lower()}"
        elif params.args.bill_type:
            raise CongressGovError("bills(bill_type := ...) also needs congress := N")
        return path, {"sort": "updateDate desc", **date_window(params)}

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``bills``."""
        return list(body.get("bills") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One bill."""
        return _bill_list_row(item)


BILL_SCHEMA = pa.schema(
    [
        *BILL_ID_COLS,
        column("bill_id", pa.string(), "Bill ID, e.g. 'hr2882-118'.", nullable=False),
        column("title", pa.string(), "Bill title."),
        column("origin_chamber", pa.string(), "'house' or 'senate'."),
        column("introduced_date", pa.date32(), "Date introduced."),
        column("policy_area", pa.string(), "CRS policy area, e.g. 'Economics and Public Finance'."),
        column("sponsor_bioguide_id", pa.string(), "Bioguide ID of the (first) sponsor."),
        column("sponsor_name", pa.string(), "Sponsor display name, e.g. 'Rep. Ciscomani, Juan [R-AZ-6]'."),
        column("sponsor_party", pa.string(), "Sponsor's party letter."),
        column("sponsor_state", pa.string(), "Sponsor's state code."),
        column("sponsor_district", pa.int32(), "Sponsor's House district (NULL for senators)."),
        column("cosponsor_count", pa.int32(), "Current cosponsors."),
        column("action_count", pa.int32(), "Recorded actions."),
        column("amendment_count", pa.int32(), "Amendments offered to the bill."),
        column("committee_count", pa.int32(), "Committees the bill was referred to."),
        column("summary_count", pa.int32(), "CRS summaries."),
        column("text_version_count", pa.int32(), "Published text versions."),
        column("latest_action_date", pa.date32(), "Date of the latest action."),
        column("latest_action_text", pa.string(), "Text of the latest action."),
        column("became_law", pa.bool_(), "True when the bill was enacted."),
        column("law_numbers", pa.list_(pa.string()), "Law numbers, e.g. ['118-47'], when enacted."),
        column("law_types", pa.list_(pa.string()), "'Public Law' or 'Private Law' for each law number."),
        column("update_date", pa.timestamp("us", tz="UTC"), "When Congress.gov last updated the record."),
        column("congress_gov_url", pa.string(), "Congress.gov web page of the bill."),
        column("api_url", pa.string(), "Congress.gov API URL of the bill."),
    ]
)


def _count(item: dict[str, Any], key: str) -> int | None:
    value = item.get(key)
    return to_int(value.get("count")) if isinstance(value, dict) else None


@init_single_worker
class Bill(CongressGovSource[Any]):
    """``bill(congress, bill_type, bill_number)``."""

    FunctionArguments = BillRefArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = BILL_SCHEMA
    PAGED: ClassVar[bool] = False

    class Meta:
        """Function metadata."""

        name = "bill"
        required_secrets = [SECRET_TYPE]
        description = "One bill's sponsor, policy area, status and counts from Congress.gov"
        tags = function_tags(
            title="Bill Detail (Congress.gov)",
            category="legislation",
            keywords=CG_KEYWORDS + "bill detail, sponsor, policy area, status, became law, public law",
            doc_llm=(
                "One row describing a bill: bill(congress, bill_type, bill_number), e.g. bill(118, 'hr', 2882). "
                "Columns: title, origin_chamber, introduced_date, policy_area, sponsor_bioguide_id (join members), "
                "sponsor_name/party/state/district, counts of cosponsors, actions, amendments, committees, "
                "summaries and text versions, latest action, became_law and law_numbers ('118-47'). " + API_KEY_NOTE
            ),
            doc_md="## bill(congress, bill_type, bill_number)\n\nOne bill's detail record. " + _md_source(),
            schema=BILL_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT title, sponsor_bioguide_id, policy_area, became_law, law_numbers "
                    f"FROM {Q}.bill(118, 'hr', 2882)"
                ),
                description="Who sponsored H.R. 2882 (118th) and whether it became law",
            ),
            FunctionExample(
                sql=(
                    f"SELECT b.title, m.full_name, m.latest_party FROM {Q}.bill(118, 'hr', 2882) b "
                    f"JOIN {Q}.members m ON m.bioguide_id = b.sponsor_bioguide_id"
                ),
                description="Join a bill's sponsor to the members table",
            ),
        ]

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """``/bill/{congress}/{type}/{number}``."""
        return _bill_path(params), {}

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """Return the single ``bill`` object."""
        return [body["bill"]] if body.get("bill") else []

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """Map the bill."""
        bill_type = _lower(item.get("type"))
        sponsor = (item.get("sponsors") or [{}])[0]
        laws = item.get("laws") or []
        date, text = _latest(item)
        return {
            "congress": to_int(item.get("congress")),
            "bill_type": bill_type,
            "bill_number": to_int(item.get("number")),
            "bill_id": bill_id(bill_type, item.get("number"), item.get("congress")),
            "title": to_text(item.get("title")),
            "origin_chamber": chamber_name(item.get("originChamber")),
            "introduced_date": parse_date(item.get("introducedDate")),
            "policy_area": to_text((item.get("policyArea") or {}).get("name")),
            "sponsor_bioguide_id": to_text(sponsor.get("bioguideId")),
            "sponsor_name": to_text(sponsor.get("fullName")),
            "sponsor_party": to_text(sponsor.get("party")),
            "sponsor_state": to_text(sponsor.get("state")),
            "sponsor_district": to_int(sponsor.get("district")),
            "cosponsor_count": _count(item, "cosponsors"),
            "action_count": _count(item, "actions"),
            "amendment_count": _count(item, "amendments"),
            "committee_count": _count(item, "committees"),
            "summary_count": _count(item, "summaries"),
            "text_version_count": _count(item, "textVersions"),
            "latest_action_date": date,
            "latest_action_text": text,
            "became_law": bool(laws),
            "law_numbers": [str(x.get("number")) for x in laws],
            "law_types": [str(x.get("type")) for x in laws],
            "update_date": parse_timestamp(item.get("updateDate")),
            "congress_gov_url": to_text(item.get("legislationUrl")),
            "api_url": f"{settings_of(params).congress_gov_url}{_bill_path(params)}",
        }


# ---------------------------------------------------------------- bill children


class _BillChild(CongressGovSource[Any]):
    FunctionArguments = BillRefArgs
    SUFFIX: ClassVar[str] = ""

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """``/bill/{congress}/{type}/{number}/<suffix>``."""
        return _bill_path(params, cls.SUFFIX), {}


def _bill_child_tags(title: str, keywords: str, what: str, md: str, schema: pa.Schema) -> dict[str, str]:
    return function_tags(
        title=title,
        category="legislation",
        keywords=CG_KEYWORDS + keywords,
        doc_llm=(
            f"{what} Arguments: (congress, bill_type, bill_number), e.g. (118, 'hr', 2882); bill_type is one of "
            "hr, s, hjres, sjres, hconres, sconres, hres, sres. The bill's congress, bill_type and bill_number "
            "are repeated on every row. " + API_KEY_NOTE
        ),
        doc_md=md + " " + _md_source(),
        schema=schema,
    )


ACTIONS_SCHEMA = pa.schema(
    [
        *BILL_ID_COLS,
        column("action_date", pa.date32(), "Date of the action."),
        column("action_time", pa.string(), "Time of day of the action (HH:MM:SS), when recorded."),
        column(
            "action_text",
            pa.string(),
            "What happened, e.g. 'Passed/agreed to in House: On passage Passed by the Yeas and Nays: 286 - 134.'.",
        ),
        column("action_type", pa.string(), "Action type, e.g. 'IntroReferral', 'Floor', 'President', 'BecameLaw'."),
        column("action_code", pa.string(), "Congress.gov action code, e.g. 'H11100', '36000'."),
        column("source_system", pa.string(), "Who recorded it, e.g. 'House floor actions', 'Library of Congress'."),
        column("committee_names", pa.list_(pa.string()), "Committees involved in the action."),
        column("vote_chamber", pa.string(), "'house' or 'senate' when the action was a recorded vote."),
        column("vote_session", pa.int32(), "Session of the recorded vote."),
        column(
            "vote_roll_number",
            pa.int32(),
            "Clerk roll-call number of the recorded vote (matches rollcalls.clerk_rollnumber, senate_votes.vote_number "
            "and house_votes.roll_call_number).",
        ),
    ]
)


@init_single_worker
class BillActions(_BillChild):
    """``bill_actions(congress, bill_type, bill_number)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ACTIONS_SCHEMA
    SUFFIX: ClassVar[str] = "/actions"

    class Meta:
        """Function metadata."""

        name = "bill_actions"
        required_secrets = [SECRET_TYPE]
        description = "Every recorded action on one bill, newest first, with links to roll-call votes"
        tags = _bill_child_tags(
            "Bill Actions (Congress.gov)",
            "actions, history, status, referral, floor action, passed, signed",
            "Every action on a bill (introduction, referral, floor votes, presidential action), newest first. "
            "vote_chamber/vote_session/vote_roll_number identify a recorded vote; join rollcalls on "
            "clerk_rollnumber (with congress, chamber and session) or senate_vote_positions(congress, session, "
            "vote_roll_number).",
            "## bill_actions(congress, bill_type, bill_number)\n\nA bill's legislative history.",
            ACTIONS_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT action_date, action_type, action_text FROM {Q}.bill_actions(118, 'hr', 2882) "
                    "WHERE vote_roll_number IS NOT NULL ORDER BY action_date"
                ),
                description="The recorded votes on H.R. 2882 (118th)",
            ),
            FunctionExample(
                sql=(
                    f"SELECT action_type, count(*) AS actions FROM {Q}.bill_actions(118, 'hr', 2882) "
                    "GROUP BY action_type ORDER BY actions DESC"
                ),
                description="Count a bill's actions by type",
            ),
        ]

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``actions``."""
        return list(body.get("actions") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One action."""
        vote = (item.get("recordedVotes") or [{}])[0]
        return {
            **_bill_ref(params),
            "action_date": parse_date(item.get("actionDate")),
            "action_time": to_text(item.get("actionTime")),
            "action_text": to_text(item.get("text")),
            "action_type": to_text(item.get("type")),
            "action_code": to_text(item.get("actionCode")),
            "source_system": to_text((item.get("sourceSystem") or {}).get("name")),
            "committee_names": [str(c.get("name")) for c in item.get("committees") or [] if c.get("name")],
            "vote_chamber": chamber_name(vote.get("chamber")),
            "vote_session": to_int(vote.get("sessionNumber")),
            "vote_roll_number": to_int(vote.get("rollNumber")),
        }


COSPONSOR_SCHEMA = pa.schema(
    [
        *BILL_ID_COLS,
        column("bioguide_id", pa.string(), "Cosponsor's bioguide ID; join members.", nullable=False),
        column("full_name", pa.string(), "Display name, e.g. 'Rep. Cole, Tom [R-OK-4]'."),
        column("first_name", pa.string(), "First name."),
        column("last_name", pa.string(), "Last name."),
        column("party", pa.string(), "Party letter."),
        column("state", pa.string(), "State code."),
        column("district", pa.int32(), "House district (NULL for senators)."),
        column("is_original_cosponsor", pa.bool_(), "True when the member cosponsored at introduction."),
        column("sponsorship_date", pa.date32(), "Date the member signed on."),
        column("sponsorship_withdrawn_date", pa.date32(), "Date the member withdrew, if they did."),
    ]
)


@init_single_worker
class BillCosponsors(_BillChild):
    """``bill_cosponsors(congress, bill_type, bill_number)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = COSPONSOR_SCHEMA
    SUFFIX: ClassVar[str] = "/cosponsors"

    class Meta:
        """Function metadata."""

        name = "bill_cosponsors"
        required_secrets = [SECRET_TYPE]
        description = "The cosponsors of one bill with sign-on dates and bioguide IDs"
        tags = _bill_child_tags(
            "Bill Cosponsors (Congress.gov)",
            "cosponsors, sponsorship, support, bipartisan, sign on",
            "One row per cosponsor (including withdrawn ones, which have sponsorship_withdrawn_date), with "
            "bioguide_id, party, state, district, is_original_cosponsor and sponsorship_date. Count by party for "
            "bipartisanship.",
            "## bill_cosponsors(congress, bill_type, bill_number)\n\nWho cosponsored a bill and when.",
            COSPONSOR_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT party, count(*) AS cosponsors FROM {Q}.bill_cosponsors(118, 'hr', 2882) "
                    "WHERE sponsorship_withdrawn_date IS NULL GROUP BY party ORDER BY party"
                ),
                description="Cosponsors of H.R. 2882 (118th) by party",
            ),
            FunctionExample(
                sql=(
                    f"SELECT bioguide_id, full_name, sponsorship_date FROM {Q}.bill_cosponsors(118, 'hr', 2882) "
                    "WHERE NOT is_original_cosponsor ORDER BY sponsorship_date"
                ),
                description="Members who signed on after introduction",
            ),
        ]

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``cosponsors``."""
        return list(body.get("cosponsors") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One cosponsor."""
        return {
            **_bill_ref(params),
            "bioguide_id": to_text(item.get("bioguideId")),
            "full_name": to_text(item.get("fullName")),
            "first_name": to_text(item.get("firstName")),
            "last_name": to_text(item.get("lastName")),
            "party": to_text(item.get("party")),
            "state": to_text(item.get("state")),
            "district": to_int(item.get("district")),
            "is_original_cosponsor": item.get("isOriginalCosponsor"),
            "sponsorship_date": parse_date(item.get("sponsorshipDate")),
            "sponsorship_withdrawn_date": parse_date(item.get("sponsorshipWithdrawnDate")),
        }


SUBJECT_SCHEMA = pa.schema(
    [
        *BILL_ID_COLS,
        column(
            "subject",
            pa.string(),
            "Legislative subject term (or the policy area), e.g. 'Appropriations'.",
            nullable=False,
        ),
        column("is_policy_area", pa.bool_(), "True for the bill's single CRS policy area row.", nullable=False),
        column("update_date", pa.timestamp("us", tz="UTC"), "When the term was assigned or updated."),
    ]
)


@init_single_worker
class BillSubjects(_BillChild):
    """``bill_subjects(congress, bill_type, bill_number)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SUBJECT_SCHEMA
    SUFFIX: ClassVar[str] = "/subjects"

    class Meta:
        """Function metadata."""

        name = "bill_subjects"
        required_secrets = [SECRET_TYPE]
        description = "The CRS policy area and legislative subject terms of one bill"
        tags = _bill_child_tags(
            "Bill Subjects (Congress.gov)",
            "subjects, topics, policy area, crs, index terms, classification",
            "One row per legislative subject term the Congressional Research Service assigned, plus one row with "
            "is_policy_area = true for the bill's single policy area.",
            "## bill_subjects(congress, bill_type, bill_number)\n\nCRS subject terms and policy area.",
            SUBJECT_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT subject FROM {Q}.bill_subjects(118, 'hr', 2882) WHERE NOT is_policy_area "
                    "ORDER BY subject LIMIT 20"
                ),
                description="The first 20 subject terms of H.R. 2882 (118th)",
            ),
            FunctionExample(
                sql=f"SELECT subject FROM {Q}.bill_subjects(118, 'hr', 2882) WHERE is_policy_area",
                description="A bill's policy area",
            ),
        ]

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``subjects.legislativeSubjects``, plus the policy area on the first page."""
        subjects = body.get("subjects") or {}
        out: list[Any] = [{**s, "_policy": False} for s in subjects.get("legislativeSubjects") or []]
        policy = subjects.get("policyArea")
        if offset == 0 and isinstance(policy, dict) and policy.get("name"):
            out.append({**policy, "_policy": True})
        return out

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One subject term."""
        return {
            **_bill_ref(params),
            "subject": to_text(item.get("name")),
            "is_policy_area": bool(item.get("_policy")),
            "update_date": parse_timestamp(item.get("updateDate")),
        }

    @classmethod
    def process(cls, params: ProcessParams[Any], state: PageCursor, out: OutputCollector) -> None:
        """Page as usual; the policy-area row does not advance the API offset."""
        if state.done:
            out.finish()
            return
        body = fetch_page(f"{state.url}&offset={state.offset}&limit={PAGE_LIMIT}", api_key(params))
        items = cls.items(body, state.offset)
        legislative = sum(1 for i in items if not i.get("_policy"))
        has_next = bool((body.get("pagination") or {}).get("next"))
        state.offset += legislative
        state.done = not (has_next and legislative)
        if items:
            rows = [cls.row(i, params) for i in items]
            out.emit(pa.RecordBatch.from_pylist(rows, schema=cls.FIXED_SCHEMA).select(params.output_schema.names))
        if state.done:
            out.finish()


SUMMARY_SCHEMA = pa.schema(
    [
        *BILL_ID_COLS,
        column("version_code", pa.string(), "CRS summary version code, e.g. '00' (as introduced)."),
        column("action_date", pa.date32(), "Date of the action the summary describes."),
        column("action_desc", pa.string(), "Stage summarized, e.g. 'Introduced in House', 'Public Law'."),
        column("update_date", pa.timestamp("us", tz="UTC"), "When the summary was published or updated."),
        column("summary_html", pa.string(), "The CRS summary text, as HTML."),
    ]
)


@init_single_worker
class BillSummaries(_BillChild):
    """``bill_summaries(congress, bill_type, bill_number)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SUMMARY_SCHEMA
    SUFFIX: ClassVar[str] = "/summaries"

    class Meta:
        """Function metadata."""

        name = "bill_summaries"
        required_secrets = [SECRET_TYPE]
        description = "The Congressional Research Service summaries of one bill, one per legislative stage"
        tags = _bill_child_tags(
            "Bill Summaries (Congress.gov)",
            "summary, crs summary, abstract, what does the bill do, plain language",
            "One row per CRS summary (written at introduction, reporting, passage and enactment), with "
            "version_code, action_date, action_desc and summary_html (HTML text). Take the latest action_date "
            "for the most current summary.",
            "## bill_summaries(congress, bill_type, bill_number)\n\nPlain-language CRS summaries of a bill.",
            SUMMARY_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT action_date, action_desc, length(summary_html) AS chars "
                    f"FROM {Q}.bill_summaries(118, 'hr', 2882) ORDER BY action_date"
                ),
                description="The summary versions of H.R. 2882 (118th)",
            ),
            FunctionExample(
                sql=(
                    f"SELECT summary_html FROM {Q}.bill_summaries(118, 'hr', 2882) "
                    "ORDER BY action_date DESC, version_code DESC LIMIT 1"
                ),
                description="The most recent summary of a bill",
            ),
        ]

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``summaries``."""
        return list(body.get("summaries") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One summary."""
        return {
            **_bill_ref(params),
            "version_code": to_text(item.get("versionCode")),
            "action_date": parse_date(item.get("actionDate")),
            "action_desc": to_text(item.get("actionDesc")),
            "update_date": parse_timestamp(item.get("updateDate")),
            "summary_html": to_text(item.get("text")),
        }


TEXT_SCHEMA = pa.schema(
    [
        *BILL_ID_COLS,
        column(
            "version_type", pa.string(), "Text version, e.g. 'Introduced in House', 'Enrolled Bill'.", nullable=False
        ),
        column("version_date", pa.timestamp("us", tz="UTC"), "Date of the version (NULL for some enrolled bills)."),
        column("html_url", pa.string(), "URL of the formatted text (HTML)."),
        column("pdf_url", pa.string(), "URL of the PDF."),
        column("xml_url", pa.string(), "URL of the USLM/Bill DTD XML."),
    ]
)


@init_single_worker
class BillTextVersions(_BillChild):
    """``bill_text_versions(congress, bill_type, bill_number)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TEXT_SCHEMA
    SUFFIX: ClassVar[str] = "/text"

    class Meta:
        """Function metadata."""

        name = "bill_text_versions"
        required_secrets = [SECRET_TYPE]
        description = "The published text versions of one bill with links to HTML, PDF and XML"
        tags = _bill_child_tags(
            "Bill Text Versions (Congress.gov)",
            "bill text, versions, pdf, xml, enrolled, engrossed, full text",
            "One row per published text version (introduced, reported, engrossed, enrolled, ...) with "
            "version_date and the URLs of its HTML, PDF and XML renderings (fetch them separately).",
            "## bill_text_versions(congress, bill_type, bill_number)\n\nLinks to every version of a bill's text.",
            TEXT_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT version_type, version_date, pdf_url FROM {Q}.bill_text_versions(118, 'hr', 2882) "
                    "ORDER BY version_date NULLS LAST"
                ),
                description="The text versions of H.R. 2882 (118th) in order",
            ),
            FunctionExample(
                sql=(
                    f"SELECT xml_url FROM {Q}.bill_text_versions(118, 'hr', 2882) WHERE version_type = 'Enrolled Bill'"
                ),
                description="The XML of the enrolled (final) text",
            ),
        ]

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``textVersions``."""
        return list(body.get("textVersions") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One text version."""
        formats = {str(f.get("type")): to_text(f.get("url")) for f in item.get("formats") or []}
        return {
            **_bill_ref(params),
            "version_type": to_text(item.get("type")),
            "version_date": parse_timestamp(item.get("date")),
            "html_url": formats.get("Formatted Text"),
            "pdf_url": formats.get("PDF"),
            "xml_url": formats.get("Formatted XML"),
        }


# ---------------------------------------------------------------- laws

LAW_SCHEMA = pa.schema(
    [
        column("law_number", pa.string(), "Law number '<congress>-<n>', e.g. '118-47'.", nullable=False),
        column("law_type", pa.string(), "'Public Law' or 'Private Law'.", nullable=False),
        *BILL_ID_COLS,
        column("bill_id", pa.string(), "Bill ID of the enacted bill, e.g. 'hr2882-118'.", nullable=False),
        column("title", pa.string(), "Title of the enacted bill."),
        column("origin_chamber", pa.string(), "'house' or 'senate'."),
        column("latest_action_date", pa.date32(), "Date of the latest action (usually the signing)."),
        column("latest_action_text", pa.string(), "Latest action, e.g. 'Became Public Law No: 118-47.'."),
        column("update_date", pa.timestamp("us", tz="UTC"), "When Congress.gov last updated the record."),
        column("api_url", pa.string(), "Congress.gov API URL of the bill."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class LawsArgs:
    """``laws(congress, law_type := NULL)``."""

    congress: Annotated[
        int, Arg(0, doc="Congress number, e.g. 118. Congress.gov has laws from the 93rd on.", ge=1, le=200)
    ]
    law_type: Annotated[
        str | None,
        Arg("law_type", default=None, doc="'public' or 'private'; NULL lists both.", choices=["public", "private"]),
    ]


@init_single_worker
class Laws(CongressGovSource[Any]):
    """``laws(congress, law_type := NULL)``."""

    FunctionArguments = LawsArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = LAW_SCHEMA

    class Meta:
        """Function metadata."""

        name = "laws"
        required_secrets = [SECRET_TYPE]
        description = "The public and private laws enacted by one Congress, with the bill each one came from"
        tags = function_tags(
            title="Laws (Congress.gov)",
            category="legislation",
            keywords=CG_KEYWORDS + "laws, public law, private law, enacted, passed bills, statutes",
            doc_llm=(
                "One row per law enacted by a Congress: laws(congress), optionally law_type := 'public' or "
                "'private'. law_number is '<congress>-<n>' (e.g. '118-47'); the row carries the enacted bill's "
                "congress, bill_type, bill_number, bill_id and title, so it joins bill(...) and the bill_* "
                "functions. Count rows for 'how many laws did the Nth Congress pass'. " + API_KEY_NOTE
            ),
            doc_md="## laws(congress, law_type := NULL)\n\nEnacted public and private laws. " + _md_source(),
            schema=LAW_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=f"SELECT law_type, count(*) AS laws FROM {Q}.laws(118) GROUP BY law_type ORDER BY law_type",
                description="How many public and private laws the 118th Congress enacted",
            ),
            FunctionExample(
                sql=(
                    f"SELECT law_number, title FROM {Q}.laws(118, law_type := 'public') "
                    "ORDER BY latest_action_date DESC LIMIT 10"
                ),
                description="The ten most recently signed public laws of the 118th Congress",
            ),
        ]

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """``/law/{congress}[/pub|priv]``."""
        a = params.args
        suffix = {"public": "/pub", "private": "/priv"}.get(a.law_type or "", "")
        return f"/law/{a.congress}{suffix}", {}

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """One item per law (a bill can carry more than one law number)."""
        return [{**b, "_law": law} for b in body.get("bills") or [] for law in b.get("laws") or []]

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One law."""
        law = item.get("_law") or {}
        base = _bill_list_row(item)
        return {
            "law_number": to_text(law.get("number")),
            "law_type": to_text(law.get("type")),
            **{k: base[k] for k in ("congress", "bill_type", "bill_number", "bill_id", "title", "origin_chamber")},
            "latest_action_date": base["latest_action_date"],
            "latest_action_text": base["latest_action_text"],
            "update_date": base["update_date"],
            "api_url": base["api_url"],
        }

    @classmethod
    def process(cls, params: ProcessParams[Any], state: PageCursor, out: OutputCollector) -> None:
        """Page by bills (the API's unit), emitting one row per law."""
        if state.done:
            out.finish()
            return
        body = fetch_page(f"{state.url}&offset={state.offset}&limit={PAGE_LIMIT}", api_key(params))
        bills = body.get("bills") or []
        rows = [cls.row(i, params) for i in cls.items(body, state.offset)]
        state.offset += len(bills)
        state.done = not ((body.get("pagination") or {}).get("next") and bills)
        if rows:
            out.emit(pa.RecordBatch.from_pylist(rows, schema=cls.FIXED_SCHEMA).select(params.output_schema.names))
        if state.done:
            out.finish()


# ---------------------------------------------------------------- amendments

AMENDMENT_SCHEMA = pa.schema(
    [
        CONGRESS_COL,
        column("amendment_type", pa.string(), "'hamdt' (House), 'samdt' (Senate) or 'suamdt' (Senate unprinted)."),
        column("amendment_number", pa.int32(), "Amendment number within its type and Congress."),
        column("description", pa.string(), "Description (House amendments)."),
        column("purpose", pa.string(), "Purpose (Senate amendments), e.g. 'In the nature of a substitute.'."),
        column("latest_action_date", pa.date32(), "Date of the latest action."),
        column("latest_action_text", pa.string(), "Latest action text."),
        column("update_date", pa.timestamp("us", tz="UTC"), "When Congress.gov last updated the record."),
        column("api_url", pa.string(), "Congress.gov API URL of the amendment."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class AmendmentsArgs:
    """``amendments(congress := NULL, amendment_type := NULL)``."""

    congress: Annotated[int | None, Arg("congress", default=None, doc="Only this Congress.", ge=1, le=200)]
    amendment_type: Annotated[
        str | None,
        Arg(
            "amendment_type",
            default=None,
            doc="'hamdt', 'samdt' or 'suamdt' (needs congress).",
            choices=AMENDMENT_TYPES,
        ),
    ]


@init_single_worker
class Amendments(CongressGovSource[Any]):
    """``amendments(congress := NULL, amendment_type := NULL)``."""

    FunctionArguments = AmendmentsArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = AMENDMENT_SCHEMA

    class Meta:
        """Function metadata."""

        name = "amendments"
        required_secrets = [SECRET_TYPE]
        description = "List House and Senate amendments from Congress.gov, with filters pushed into the API"
        filter_pushdown = True
        auto_apply_filters = True
        tags = function_tags(
            title="Amendments (Congress.gov)",
            category="legislation",
            keywords=CG_KEYWORDS + "amendments, hamdt, samdt, floor amendments, substitute",
            doc_llm=(
                "List amendments: amendments(congress := N, amendment_type := 'hamdt' | 'samdt' | 'suamdt'), or "
                "the same as WHERE congress = N AND amendment_type = '...'; update_date ranges are pushed as "
                "fromDateTime/toDateTime. Columns: congress, amendment_type, amendment_number, description "
                "(House), purpose (Senate), latest action and update_date. Always filter or LIMIT. " + API_KEY_NOTE
            ),
            doc_md="## amendments(congress := NULL, amendment_type := NULL)\n\nFloor amendments. " + _md_source(),
            schema=AMENDMENT_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT amendment_number, purpose, latest_action_text "
                    f"FROM {Q}.amendments(congress := 118, amendment_type := 'samdt') LIMIT 10"
                ),
                description="Ten recently updated Senate amendments of the 118th Congress",
            ),
            FunctionExample(
                sql=(
                    f"SELECT amendment_type, count(*) AS n FROM {Q}.amendments() WHERE congress = 119 "
                    "AND update_date >= TIMESTAMPTZ '2026-11-01 00:00:00+00' GROUP BY amendment_type"
                ),
                description="Amendments of the 119th Congress updated since November 1, 2026",
            ),
        ]

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """``/amendment[/congress[/type]]``."""
        congress, kind = _congress_and(params, "amendment_type")
        path = "/amendment"
        if congress is not None:
            path += f"/{congress}"
            if kind:
                path += f"/{str(kind).lower()}"
        elif params.args.amendment_type:
            raise CongressGovError("amendments(amendment_type := ...) also needs congress := N")
        return path, date_window(params)

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``amendments``."""
        return list(body.get("amendments") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One amendment."""
        date, text = _latest(item)
        return {
            "congress": to_int(item.get("congress")),
            "amendment_type": _lower(item.get("type")),
            "amendment_number": to_int(item.get("number")),
            "description": to_text(item.get("description")),
            "purpose": to_text(item.get("purpose")),
            "latest_action_date": date,
            "latest_action_text": text,
            "update_date": parse_timestamp(item.get("updateDate")),
            "api_url": to_text(item.get("url")),
        }


# ---------------------------------------------------------------- members (Congress.gov)

CG_MEMBER_SCHEMA = pa.schema(
    [
        column("bioguide_id", pa.string(), "Bioguide ID; join members and member_terms.", nullable=False),
        column("name", pa.string(), "Name as 'Last, First', e.g. 'Fong, Vince'."),
        column("party_name", pa.string(), "Party, e.g. 'Democratic', 'Republican', 'Independent'."),
        column("state_name", pa.string(), "Full state name, e.g. 'California'."),
        column("district", pa.int32(), "House district of the latest term (NULL for senators)."),
        column("chambers", pa.list_(pa.string()), "Chambers served in: 'house' and/or 'senate'."),
        column("first_year", pa.int32(), "First year of service."),
        column("last_year", pa.int32(), "Last year of service (NULL while serving)."),
        column("image_url", pa.string(), "Official portrait URL."),
        column("update_date", pa.timestamp("us", tz="UTC"), "When Congress.gov last updated the record."),
        column("api_url", pa.string(), "Congress.gov API URL of the member."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class CgMembersArgs:
    """``congress_gov_members(congress := NULL, state := NULL, district := NULL, current_only := false)``."""

    congress: Annotated[
        int | None, Arg("congress", default=None, doc="Members who served in this Congress.", ge=1, le=200)
    ]
    state: Annotated[
        str | None, Arg("state", default=None, doc="Two-letter state code, e.g. 'CA'.", pattern=r"^[A-Za-z]{2}$")
    ]
    district: Annotated[int | None, Arg("district", default=None, doc="House district (needs state).", ge=0)]
    current_only: Annotated[bool, Arg("current_only", default=False, doc="Only members serving now.")]


@init_single_worker
class CongressGovMembers(CongressGovSource[Any]):
    """``congress_gov_members(...)``."""

    FunctionArguments = CgMembersArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = CG_MEMBER_SCHEMA

    class Meta:
        """Function metadata."""

        name = "congress_gov_members"
        required_secrets = [SECRET_TYPE]
        description = "Members of Congress as listed by Congress.gov, by Congress, state and district, with portraits"
        tags = function_tags(
            title="Members (Congress.gov)",
            category="members",
            keywords=CG_KEYWORDS + "members, portraits, delegation, state delegation, district representative",
            doc_llm=(
                "Congress.gov's member list: congress_gov_members(congress := N, state := 'CA', district := 12, "
                "current_only := true); every argument is optional and pushed into the request path. Columns: "
                "bioguide_id, name, party_name, state_name (full name), district, chambers, first_year, "
                "last_year, image_url. The keyless members / member_terms tables carry the same people with "
                "dates and IDs; use this for Congress.gov's portraits and its own view of a delegation. " + API_KEY_NOTE
            ),
            doc_md=(
                "## congress_gov_members(congress :=, state :=, district :=, current_only := false)\n\n"
                "Congress.gov's member directory. " + _md_source()
            ),
            schema=CG_MEMBER_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT party_name, count(*) AS members FROM {Q}.congress_gov_members(congress := 118, "
                    "state := 'CA') GROUP BY party_name ORDER BY members DESC"
                ),
                description="California's delegation in the 118th Congress by party",
            ),
            FunctionExample(
                sql=(
                    f"SELECT bioguide_id, name, image_url FROM {Q}.congress_gov_members(state := 'VT', "
                    "current_only := true) ORDER BY name"
                ),
                description="Vermont's current members with portraits",
            ),
        ]

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """Pick the narrowest member path the arguments allow."""
        a = params.args
        state = a.state.upper() if a.state else None
        if a.district is not None and not state:
            raise CongressGovError("congress_gov_members(district := ...) also needs state := 'XX'")
        parts = ["/member"]
        if a.congress is not None:
            parts.append(f"/congress/{a.congress}")
        if state:
            parts.append(f"/{state}")
            if a.district is not None:
                parts.append(f"/{a.district}")
        query = {"currentMember": "true"} if a.current_only else {}
        return "".join(parts), query

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``members``."""
        return list(body.get("members") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One member."""
        terms = (item.get("terms") or {}).get("item") or []
        chambers = sorted({c for c in (chamber_name(t.get("chamber")) for t in terms) if c})
        starts = [to_int(t.get("startYear")) for t in terms if t.get("startYear")]
        ends = [to_int(t.get("endYear")) for t in terms]
        return {
            "bioguide_id": to_text(item.get("bioguideId")),
            "name": to_text(item.get("name")),
            "party_name": to_text(item.get("partyName")),
            "state_name": to_text(item.get("state")),
            "district": to_int(item.get("district")),
            "chambers": chambers,
            "first_year": min((s for s in starts if s is not None), default=None),
            "last_year": None if not ends or None in ends else max(e for e in ends if e is not None),
            "image_url": to_text((item.get("depiction") or {}).get("imageUrl")),
            "update_date": parse_timestamp(item.get("updateDate")),
            "api_url": to_text(item.get("url")),
        }


SPONSORED_SCHEMA = pa.schema(
    [
        column("bioguide_id", pa.string(), "The member whose legislation this is.", nullable=False),
        CONGRESS_COL,
        column("bill_type", pa.string(), "Bill type code (NULL for amendments)."),
        column("bill_number", pa.int32(), "Bill number (NULL for amendments)."),
        column("bill_id", pa.string(), "Bill ID, e.g. 's4417-117' (NULL for amendments)."),
        column("amendment_number", pa.int32(), "Amendment number, for sponsored amendments."),
        column("title", pa.string(), "Bill title."),
        column("introduced_date", pa.date32(), "Date introduced."),
        column("policy_area", pa.string(), "CRS policy area."),
        column("latest_action_date", pa.date32(), "Date of the latest action."),
        column("latest_action_text", pa.string(), "Latest action text."),
        column("api_url", pa.string(), "Congress.gov API URL of the bill or amendment."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class BioguideArgs:
    """``fn(bioguide_id)``."""

    bioguide_id: Annotated[
        str, Arg(0, doc="Bioguide ID, e.g. 'S000148' (find it in members).", pattern=r"^[A-Za-z][0-9]{6}$")
    ]


class _MemberLegislation(CongressGovSource[Any]):
    FunctionArguments = BioguideArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = SPONSORED_SCHEMA
    KIND: ClassVar[str] = "sponsored"

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """``/member/{id}/sponsored-legislation`` or ``cosponsored-legislation``."""
        return f"/member/{params.args.bioguide_id.upper()}/{cls.KIND}-legislation", {}

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``sponsoredLegislation`` / ``cosponsoredLegislation``."""
        return list(body.get(f"{cls.KIND}Legislation") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One bill or amendment."""
        bill_type = _lower(item.get("type"))
        date, text = _latest(item)
        return {
            "bioguide_id": params.args.bioguide_id.upper(),
            "congress": to_int(item.get("congress")),
            "bill_type": bill_type,
            "bill_number": to_int(item.get("number")),
            "bill_id": bill_id(bill_type, item.get("number"), item.get("congress")),
            "amendment_number": to_int(item.get("amendmentNumber")),
            "title": to_text(item.get("title")),
            "introduced_date": parse_date(item.get("introducedDate")),
            "policy_area": to_text((item.get("policyArea") or {}).get("name")),
            "latest_action_date": date,
            "latest_action_text": text,
            "api_url": to_text(item.get("url")),
        }


def _member_leg_tags(kind: str) -> dict[str, str]:
    return function_tags(
        title=f"{kind.title()} Legislation (Congress.gov)",
        category="legislation",
        keywords=CG_KEYWORDS + f"{kind} legislation, member bills, legislative record, {kind} bills",
        doc_llm=(
            f"Every bill, resolution and amendment a member {kind}, newest first: {kind}_legislation(bioguide_id), "
            "e.g. ('S000148'). Columns: congress, bill_type, bill_number, bill_id (NULL for amendments, which "
            "carry amendment_number), title, introduced_date, policy_area and the latest action. Count by "
            "policy_area for a member's legislative focus. " + API_KEY_NOTE
        ),
        doc_md=f"## {kind}_legislation(bioguide_id)\n\nA member's {kind} bills and amendments. " + _md_source(),
        schema=SPONSORED_SCHEMA,
    )


@init_single_worker
class SponsoredLegislation(_MemberLegislation):
    """``sponsored_legislation(bioguide_id)``."""

    KIND: ClassVar[str] = "sponsored"

    class Meta:
        """Function metadata."""

        name = "sponsored_legislation"
        required_secrets = [SECRET_TYPE]
        description = "Every bill, resolution and amendment one member sponsored, from Congress.gov"
        tags = _member_leg_tags("sponsored")
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT policy_area, count(*) AS bills FROM {Q}.sponsored_legislation('S000148') "
                    "WHERE bill_id IS NOT NULL GROUP BY policy_area ORDER BY bills DESC LIMIT 5"
                ),
                description="The five policy areas Senator Schumer has sponsored the most bills in",
            ),
            FunctionExample(
                sql=(
                    f"SELECT congress, count(*) AS bills FROM {Q}.sponsored_legislation('S000148') "
                    "GROUP BY congress ORDER BY congress"
                ),
                description="Sponsored legislation per Congress",
            ),
        ]


@init_single_worker
class CosponsoredLegislation(_MemberLegislation):
    """``cosponsored_legislation(bioguide_id)``."""

    KIND: ClassVar[str] = "cosponsored"

    class Meta:
        """Function metadata."""

        name = "cosponsored_legislation"
        required_secrets = [SECRET_TYPE]
        description = "Every bill, resolution and amendment one member cosponsored, from Congress.gov"
        tags = _member_leg_tags("cosponsored")
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT congress, count(*) AS bills FROM {Q}.cosponsored_legislation('S000148') "
                    "GROUP BY congress ORDER BY congress DESC LIMIT 5"
                ),
                description="How many bills Senator Schumer cosponsored in each of the last five Congresses",
            ),
            FunctionExample(
                sql=(
                    f"SELECT bill_id, title FROM {Q}.cosponsored_legislation('S000148') "
                    "WHERE policy_area = 'Health' ORDER BY introduced_date DESC LIMIT 10"
                ),
                description="Recent health bills a member cosponsored",
            ),
        ]


# ---------------------------------------------------------------- committees, nominations, treaties

COMMITTEE_SCHEMA = pa.schema(
    [
        column(
            "system_code",
            pa.string(),
            "Committee system code, e.g. 'hspw00' (subcommittees end in digits).",
            nullable=False,
        ),
        column("name", pa.string(), "Committee name, e.g. 'Transportation and Infrastructure Committee'."),
        column("chamber", pa.string(), "'house', 'senate' or 'joint'."),
        column("committee_type", pa.string(), "'Standing', 'Select', 'Special', 'Joint', 'Subcommittee', ..."),
        column("parent_system_code", pa.string(), "Parent committee's system code, for subcommittees."),
        column("parent_name", pa.string(), "Parent committee's name, for subcommittees."),
        column("subcommittee_count", pa.int32(), "Number of subcommittees."),
        column("update_date", pa.timestamp("us", tz="UTC"), "When Congress.gov last updated the record."),
        column("api_url", pa.string(), "Congress.gov API URL of the committee."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class CommitteesArgs:
    """``committees(congress := NULL, chamber := NULL)``."""

    congress: Annotated[int | None, Arg("congress", default=None, doc="Committees of this Congress.", ge=1, le=200)]
    chamber: Annotated[
        str | None,
        Arg(
            "chamber",
            default=None,
            doc="'house', 'senate' or 'joint'; NULL for all.",
            choices=["house", "senate", "joint"],
        ),
    ]


@init_single_worker
class Committees(CongressGovSource[Any]):
    """``committees(congress := NULL, chamber := NULL)``."""

    FunctionArguments = CommitteesArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = COMMITTEE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "committees"
        required_secrets = [SECRET_TYPE]
        description = "House, Senate and joint committees and subcommittees from Congress.gov"
        tags = function_tags(
            title="Committees (Congress.gov)",
            category="senate_business",
            keywords=CG_KEYWORDS + "committees, subcommittees, standing committee, committee codes, jurisdiction",
            doc_llm=(
                "Committees and subcommittees: committees(congress := N, chamber := 'house' | 'senate' | "
                "'joint'), both optional and pushed into the request. Columns: system_code (e.g. 'hspw00'; "
                "subcommittees have a parent_system_code), name, chamber, committee_type, parent_name, "
                "subcommittee_count. Filter parent_system_code IS NULL for full committees. " + API_KEY_NOTE
            ),
            doc_md="## committees(congress := NULL, chamber := NULL)\n\nCongressional committees. " + _md_source(),
            schema=COMMITTEE_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT system_code, name, subcommittee_count FROM {Q}.committees(congress := 118, "
                    "chamber := 'senate') WHERE parent_system_code IS NULL ORDER BY name"
                ),
                description="The Senate's full committees in the 118th Congress",
            ),
            FunctionExample(
                sql=(
                    f"SELECT committee_type, count(*) AS committees FROM {Q}.committees(chamber := 'house') "
                    "GROUP BY committee_type ORDER BY committees DESC"
                ),
                description="House committees by type",
            ),
        ]

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """``/committee[/congress][/chamber]``."""
        a = params.args
        path = "/committee"
        if a.congress is not None:
            path += f"/{a.congress}"
        if a.chamber:
            path += f"/{a.chamber}"
        return path, {}

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``committees``."""
        return list(body.get("committees") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One committee."""
        parent = item.get("parent") or {}
        chamber = _lower(item.get("chamber"))
        return {
            "system_code": to_text(item.get("systemCode")),
            "name": to_text(item.get("name")),
            "chamber": chamber if chamber == "joint" else chamber_name(chamber),
            "committee_type": to_text(item.get("committeeTypeCode")),
            "parent_system_code": to_text(parent.get("systemCode")),
            "parent_name": to_text(parent.get("name")),
            "subcommittee_count": len(item.get("subcommittees") or []),
            "update_date": parse_timestamp(item.get("updateDate")),
            "api_url": to_text(item.get("url")),
        }


NOMINATION_SCHEMA = pa.schema(
    [
        CONGRESS_COL,
        column("number", pa.int32(), "Nomination number, e.g. 1064 for PN1064."),
        column("part_number", pa.string(), "Part number when a nomination is split, e.g. '00'."),
        column("citation", pa.string(), "Citation, e.g. 'PN1064' or 'PN12-1' (matches senate_votes.issue)."),
        column("description", pa.string(), "Nominee and position, e.g. 'Lisette Nieves, of New York, to be ...'."),
        column("organization", pa.string(), "Agency or organization of the position."),
        column("received_date", pa.date32(), "Date the Senate received the nomination."),
        column("is_civilian", pa.bool_(), "True for civilian nominations."),
        column("is_military", pa.bool_(), "True for military promotions lists."),
        column("latest_action_date", pa.date32(), "Date of the latest action."),
        column("latest_action_text", pa.string(), "Latest action, e.g. 'Confirmed by the Senate by Voice Vote.'."),
        column("update_date", pa.timestamp("us", tz="UTC"), "When Congress.gov last updated the record."),
        column("api_url", pa.string(), "Congress.gov API URL of the nomination."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class OptionalCongressArgs:
    """``fn(congress := NULL)``."""

    congress: Annotated[int | None, Arg("congress", default=None, doc="Only this Congress.", ge=1, le=200)]


@init_single_worker
class Nominations(CongressGovSource[Any]):
    """``nominations(congress := NULL)``."""

    FunctionArguments = OptionalCongressArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = NOMINATION_SCHEMA

    class Meta:
        """Function metadata."""

        name = "nominations"
        required_secrets = [SECRET_TYPE]
        description = "Presidential nominations sent to the Senate, with their latest action, from Congress.gov"
        filter_pushdown = True
        auto_apply_filters = True
        tags = function_tags(
            title="Nominations (Congress.gov)",
            category="senate_business",
            keywords=CG_KEYWORDS + "nominations, confirmations, appointments, judges, cabinet, advice and consent",
            doc_llm=(
                "Presidential nominations: nominations(congress := N) or WHERE congress = N (pushed), plus "
                "update_date ranges pushed as fromDateTime/toDateTime. Columns: citation ('PN1064', matches "
                "senate_votes.issue), description (nominee and post), organization, received_date, is_civilian, "
                "is_military, latest_action_text (e.g. 'Confirmed by the Senate by Voice Vote.'). Search "
                "latest_action_text ILIKE '%confirmed%' for confirmations. " + API_KEY_NOTE
            ),
            doc_md="## nominations(congress := NULL)\n\nNominations and their status. " + _md_source(),
            schema=NOMINATION_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT organization, count(*) AS nominations FROM {Q}.nominations(congress := 118) "
                    "WHERE is_civilian GROUP BY organization ORDER BY nominations DESC LIMIT 10"
                ),
                description="Organizations with the most civilian nominations in the 118th Congress",
            ),
            FunctionExample(
                sql=(
                    f"SELECT citation, description FROM {Q}.nominations(congress := 119) "
                    "WHERE latest_action_text ILIKE '%confirmed%' ORDER BY latest_action_date DESC LIMIT 10"
                ),
                description="The ten most recent confirmations in the 119th Congress",
            ),
        ]

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """``/nomination[/congress]``."""
        congress = params.args.congress if params.args.congress is not None else to_int(pushed_eq(params, "congress"))
        return ("/nomination" + (f"/{congress}" if congress is not None else "")), date_window(params)

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``nominations``."""
        return list(body.get("nominations") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One nomination."""
        kind = item.get("nominationType") or {}
        date, text = _latest(item)
        return {
            "congress": to_int(item.get("congress")),
            "number": to_int(item.get("number")),
            "part_number": to_text(item.get("partNumber")),
            "citation": to_text(item.get("citation")),
            "description": to_text(item.get("description")),
            "organization": to_text(item.get("organization")),
            "received_date": parse_date(item.get("receivedDate")),
            "is_civilian": kind.get("isCivilian"),
            "is_military": kind.get("isMilitary"),
            "latest_action_date": date,
            "latest_action_text": text,
            "update_date": parse_timestamp(item.get("updateDate")),
            "api_url": to_text(item.get("url")),
        }


TREATY_SCHEMA = pa.schema(
    [
        column("congress_received", pa.int32(), "Congress in which the treaty was received."),
        column("congress_considered", pa.int32(), "Congress in which the Senate considered it."),
        column("number", pa.int32(), "Treaty document number within the Congress received."),
        column("suffix", pa.string(), "Part suffix, e.g. 'A', for treaties split into parts."),
        column("topic", pa.string(), "Topic, e.g. 'International Law and Organization'."),
        column("transmitted_date", pa.timestamp("us", tz="UTC"), "Date transmitted to the Senate."),
        column("parts_count", pa.int32(), "Number of parts."),
        column("update_date", pa.timestamp("us", tz="UTC"), "When Congress.gov last updated the record."),
        column("api_url", pa.string(), "Congress.gov API URL of the treaty."),
    ]
)


@init_single_worker
class Treaties(CongressGovSource[Any]):
    """``treaties(congress := NULL)``."""

    FunctionArguments = OptionalCongressArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = TREATY_SCHEMA

    class Meta:
        """Function metadata."""

        name = "treaties"
        required_secrets = [SECRET_TYPE]
        description = "Treaties transmitted to the Senate for advice and consent, from Congress.gov"
        tags = function_tags(
            title="Treaties (Congress.gov)",
            category="senate_business",
            keywords=CG_KEYWORDS + "treaties, treaty documents, ratification, advice and consent, foreign relations",
            doc_llm=(
                "Treaty documents sent to the Senate: treaties(congress := N) filters by the Congress the treaty "
                "was received in (pushed into the request). Columns: congress_received, congress_considered, "
                "number, suffix, topic, transmitted_date, parts_count. " + API_KEY_NOTE
            ),
            doc_md="## treaties(congress := NULL)\n\nTreaty documents. " + _md_source(),
            schema=TREATY_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT number, topic, transmitted_date FROM {Q}.treaties(congress := 117) "
                    "ORDER BY transmitted_date"
                ),
                description="Treaties received in the 117th Congress",
            ),
            FunctionExample(
                sql=(
                    f"SELECT topic, count(*) AS treaties FROM {Q}.treaties() GROUP BY topic "
                    "ORDER BY treaties DESC LIMIT 5"
                ),
                description="The most common treaty topics",
            ),
        ]

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """``/treaty[/congress]``."""
        congress = params.args.congress
        return ("/treaty" + (f"/{congress}" if congress is not None else "")), {}

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``treaties``."""
        return list(body.get("treaties") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One treaty."""
        parts = item.get("parts") or {}
        return {
            "congress_received": to_int(item.get("congressReceived")),
            "congress_considered": to_int(item.get("congressConsidered")),
            "number": to_int(item.get("number")),
            "suffix": to_text(item.get("suffix")),
            "topic": to_text(item.get("topic")),
            "transmitted_date": parse_timestamp(item.get("transmittedDate")),
            "parts_count": to_int(parts.get("count")) if isinstance(parts, dict) else None,
            "update_date": parse_timestamp(item.get("updateDate")),
            "api_url": to_text(item.get("url")),
        }


# ---------------------------------------------------------------- House votes (beta)

HOUSE_VOTE_SCHEMA = pa.schema(
    [
        column("congress", pa.int32(), "Congress number (118 onward).", nullable=False),
        column("session", pa.int32(), "Session (1 or 2).", nullable=False),
        column("roll_call_number", pa.int32(), "House roll-call number within the session.", nullable=False),
        column("identifier", pa.int64(), "Congress.gov vote identifier (congress, session, year, number)."),
        column("start_time", pa.timestamp("us", tz="UTC"), "When the vote opened."),
        column("vote_type", pa.string(), "'Yea and Nay', 'Recorded Vote', '2/3 Yea-And-Nay', 'Quorum', ..."),
        column("result", pa.string(), "'Passed', 'Failed' or 'Agreed to'."),
        column(
            "legislation_type", pa.string(), "Measure type: 'hr', 'hres', 'hjres', 'hconres' (NULL for amendments)."
        ),
        column("legislation_number", pa.int32(), "Measure number."),
        column("amendment_type", pa.string(), "'hamdt' when the vote was on an amendment."),
        column("amendment_number", pa.int32(), "Amendment number."),
        column("amendment_author", pa.string(), "Amendment author, e.g. 'Davidson of Ohio Amendment No. 51'."),
        column("legislation_url", pa.string(), "Congress.gov page of the measure."),
        column("source_data_url", pa.string(), "The Clerk's roll-call XML."),
        column("update_date", pa.timestamp("us", tz="UTC"), "When Congress.gov last updated the record."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class HouseVotesArgs:
    """``house_votes(congress, session := NULL)``."""

    congress: Annotated[int, Arg(0, doc="Congress number; the beta endpoint covers the 118th on.", ge=118, le=200)]
    session: Annotated[int | None, Arg("session", default=None, doc="1 or 2; NULL for both.", ge=1, le=3)]


@init_single_worker
class HouseVotes(CongressGovSource[Any]):
    """``house_votes(congress, session := NULL)``."""

    FunctionArguments = HouseVotesArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = HOUSE_VOTE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "house_votes"
        required_secrets = [SECRET_TYPE]
        description = "House roll-call votes on legislation from Congress.gov's beta vote API, 118th Congress on"
        tags = function_tags(
            title="House Votes (Congress.gov)",
            category="votes",
            keywords=CG_KEYWORDS + "house votes, roll call, clerk, yea and nay, recorded vote",
            doc_llm=(
                "House roll calls from Congress.gov's beta endpoint (118th Congress on, legislation-related "
                "votes only): house_votes(congress, session := 1 | 2). Columns: roll_call_number (restarts each "
                "session), start_time, vote_type, result, legislation_type/number or amendment_type/number/"
                "author, source_data_url. For member positions use house_vote_positions(congress, session, "
                "roll_call_number); for older or non-legislative votes use rollcalls(congress, chamber := "
                "'house'). " + API_KEY_NOTE
            ),
            doc_md="## house_votes(congress, session := NULL)\n\nHouse roll-call votes (beta). " + _md_source(),
            schema=HOUSE_VOTE_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT result, count(*) AS votes FROM {Q}.house_votes(118, session := 1) "
                    "GROUP BY result ORDER BY votes DESC"
                ),
                description="Outcomes of House roll calls in 2023",
            ),
            FunctionExample(
                sql=(
                    f"SELECT roll_call_number, start_time, legislation_type, legislation_number, result "
                    f"FROM {Q}.house_votes(119) ORDER BY start_time DESC LIMIT 10"
                ),
                description="The ten most recent House votes of the 119th Congress",
            ),
        ]

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """``/house-vote/{congress}[/{session}]``."""
        a = params.args
        return f"/house-vote/{a.congress}" + (f"/{a.session}" if a.session else ""), {}

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``houseRollCallVotes``."""
        return list(body.get("houseRollCallVotes") or [])

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One vote."""
        return {
            "congress": to_int(item.get("congress")),
            "session": to_int(item.get("sessionNumber")),
            "roll_call_number": to_int(item.get("rollCallNumber")),
            "identifier": to_int(item.get("identifier")),
            "start_time": parse_timestamp(item.get("startDate")),
            "vote_type": to_text(item.get("voteType")),
            "result": to_text(item.get("result")),
            "legislation_type": _lower(item.get("legislationType")),
            "legislation_number": to_int(item.get("legislationNumber")),
            "amendment_type": _lower(item.get("amendmentType")),
            "amendment_number": to_int(item.get("amendmentNumber")),
            "amendment_author": to_text(item.get("amendmentAuthor")),
            "legislation_url": to_text(item.get("legislationUrl")),
            "source_data_url": to_text(item.get("sourceDataURL")),
            "update_date": parse_timestamp(item.get("updateDate")),
        }


HOUSE_POSITION_SCHEMA = pa.schema(
    [
        column("congress", pa.int32(), "Congress number.", nullable=False),
        column("session", pa.int32(), "Session.", nullable=False),
        column("roll_call_number", pa.int32(), "House roll-call number within the session.", nullable=False),
        column("vote_question", pa.string(), "The question, e.g. 'On Passage'."),
        column("result", pa.string(), "Outcome of the vote."),
        column("bioguide_id", pa.string(), "Member's bioguide ID; join members."),
        column("first_name", pa.string(), "First name."),
        column("last_name", pa.string(), "Last name."),
        column("party", pa.string(), "Party letter: 'D', 'R' or 'I'."),
        column("state", pa.string(), "State code."),
        column("vote_cast", pa.string(), "'Aye'/'Yea', 'No'/'Nay', 'Present' or 'Not Voting'."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class HouseVoteRefArgs:
    """``house_vote_positions(congress, session, roll_call_number)``."""

    congress: Annotated[int, Arg(0, doc="Congress number (118 on).", ge=118, le=200)]
    session: Annotated[int, Arg(1, doc="Session: 1 or 2.", ge=1, le=3)]
    roll_call_number: Annotated[int, Arg(2, doc="Roll-call number within the session, from house_votes.", ge=1)]


def _member_votes_block(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        return {}
    for key in ("houseRollCallVoteMemberVotes", "houseRollCallMemberVotes", "houseRollCallVote"):
        if isinstance(body.get(key), dict):
            return dict(body[key])
    for value in body.values():
        if isinstance(value, dict) and "results" in value:
            return dict(value)
    return {}


@init_single_worker
class HouseVotePositions(CongressGovSource[Any]):
    """``house_vote_positions(congress, session, roll_call_number)``."""

    FunctionArguments = HouseVoteRefArgs
    FIXED_SCHEMA: ClassVar[pa.Schema] = HOUSE_POSITION_SCHEMA

    class Meta:
        """Function metadata."""

        name = "house_vote_positions"
        required_secrets = [SECRET_TYPE]
        description = "How each representative voted on one House roll call, from Congress.gov's beta vote API"
        tags = function_tags(
            title="House Vote Positions (Congress.gov)",
            category="votes",
            keywords=CG_KEYWORDS + "house member votes, positions, aye, no, how did representatives vote",
            doc_llm=(
                "One row per representative for one House roll call (118th Congress on): "
                "house_vote_positions(congress, session, roll_call_number). vote_cast is the Clerk's wording "
                "('Aye', 'No', 'Yea', 'Nay', 'Present', 'Not Voting'). bioguide_id joins members. " + API_KEY_NOTE
            ),
            doc_md=(
                "## house_vote_positions(congress, session, roll_call_number)\n\nEach member's position on one "
                "House vote. " + _md_source()
            ),
            schema=HOUSE_POSITION_SCHEMA,
        )
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT party, vote_cast, count(*) AS members FROM {Q}.house_vote_positions(118, 1, 296) "
                    "GROUP BY party, vote_cast ORDER BY party, vote_cast"
                ),
                description="The party split on House roll call 296 of 2023",
            ),
            FunctionExample(
                sql=(
                    f"SELECT bioguide_id, last_name, state FROM {Q}.house_vote_positions(118, 1, 296) "
                    "WHERE vote_cast = 'Not Voting' ORDER BY last_name"
                ),
                description="Representatives who did not vote",
            ),
        ]

    @classmethod
    def endpoint(cls, params: ProcessParams[Any]) -> tuple[str, dict[str, str]]:
        """``/house-vote/{congress}/{session}/{number}/members``."""
        a = params.args
        return f"/house-vote/{a.congress}/{a.session}/{a.roll_call_number}/members", {}

    @classmethod
    def items(cls, body: Any, offset: int) -> list[Any]:
        """``results`` of the member-votes block, each with the vote-level fields attached."""
        block = _member_votes_block(body)
        shared = {"_question": block.get("voteQuestion"), "_result": block.get("result")}
        return [{**r, **shared} for r in block.get("results") or []]

    @classmethod
    def row(cls, item: Any, params: ProcessParams[Any]) -> dict[str, Any]:
        """One member's position."""
        a = params.args
        return {
            "congress": a.congress,
            "session": a.session,
            "roll_call_number": a.roll_call_number,
            "vote_question": to_text(item.get("_question")),
            "result": to_text(item.get("_result")),
            "bioguide_id": to_text(item.get("bioguideID") or item.get("bioguideId")),
            "first_name": to_text(item.get("firstName")),
            "last_name": to_text(item.get("lastName")),
            "party": to_text(item.get("voteParty")),
            "state": to_text(item.get("voteState")),
            "vote_cast": to_text(item.get("voteCast")),
        }


CONGRESS_GOV_FUNCTIONS: list[type[Any]] = [
    Bills,
    Bill,
    BillActions,
    BillCosponsors,
    BillSubjects,
    BillSummaries,
    BillTextVersions,
    Laws,
    Amendments,
    CongressGovMembers,
    SponsoredLegislation,
    CosponsoredLegislation,
    Committees,
    Nominations,
    Treaties,
    HouseVotes,
    HouseVotePositions,
]
