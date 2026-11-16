"""Unit tests for parsing, pushdown translation and the download cache.

End-to-end SQL tests live in test/sql and run through DuckDB.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
from vgi.arguments import Arguments
from vgi.client import Client
from vgi.table_filter_pushdown import AndFilter, ComparisonOp, ConstantFilter, PushdownFilters

from us_congress_worker import congress_gov as cg
from us_congress_worker.common import chamber_name, parse_timestamp, to_int
from us_congress_worker.fetch import Settings, cache_entries, cache_path, cached_file, read_meta
from us_congress_worker.legislators import parse_legislators
from us_congress_worker.senate import parse_menu, parse_vote
from us_congress_worker.voteview import build_ideology, build_member_votes, build_rollcalls, cast_labels

FIXTURES = Path(__file__).resolve().parent.parent / "test" / "fixtures"


def _table(client: Client, name: str, *positional: Any, **named: Any) -> pa.Table:
    arguments = Arguments(
        positional=tuple(pa.scalar(v) for v in positional),
        named={k: pa.scalar(v) for k, v in named.items()},
    )
    batches = list(client.table_function(function_name=name, schema_path=["main"], arguments=arguments))
    return pa.Table.from_batches(batches)


def test_keyed_function_without_secret_fails_clearly(client: Client) -> None:
    with pytest.raises(Exception, match="TYPE us_congress"):
        _table(client, "bills", congress=118)


def test_argument_constraints_enforced(client: Client) -> None:
    with pytest.raises(Exception, match=r"(?i)choices|one of|must be"):
        _table(client, "rollcalls", 118, chamber="both")


# ---------------------------------------------------------------- legislators


def _legislator_paths() -> list[Path]:
    return [FIXTURES / "legislators" / n for n in ("legislators-current.json", "legislators-historical.json")]


def test_parse_legislators_members_and_terms() -> None:
    leg = parse_legislators(_legislator_paths(), today=dt.date(2026, 11, 15))
    members = {r["bioguide_id"]: r for r in leg.members.to_pylist()}
    assert members["M000043"]["full_name"] == "James Madison"
    assert members["M000043"]["is_current"] is False
    assert members["S000033"]["latest_chamber"] == "senate"
    assert members["S000033"]["icpsr_id"] == 29147
    assert leg.lis_to_bioguide["S313"] == "S000033"
    assert leg.icpsr_to_bioguide[29147] == "S000033"
    terms = [r for r in leg.terms.to_pylist() if r["bioguide_id"] == "S000033"]
    assert [t["term_number"] for t in terms] == list(range(1, 13))
    assert terms[-1]["is_current"] is True and terms[0]["is_current"] is False
    assert all(t["district"] is None for t in terms if t["chamber"] == "senate")
    assert leg.terms.schema.field("start_date").type == pa.date32()


# ---------------------------------------------------------------- Voteview


def _settings(base: str, tmp_path: Path) -> Settings:
    return Settings(
        legislators_url=f"{base}/legislators",
        voteview_url=f"{base}/voteview",
        senate_url=f"{base}/senate",
        congress_gov_url=f"{base}/congress/v3",
        cache_dir=tmp_path,
    )


def test_cast_labels() -> None:
    codes = pa.array([1, 2, 6, 9, 0, 42, None], pa.int64())
    assert cast_labels(codes, detailed=False).to_pylist() == [
        "Yea",
        "Yea",
        "Nay",
        "Not Voting",
        "Not a member",
        None,
        None,
    ]
    assert cast_labels(codes, detailed=True).to_pylist()[1] == "Paired Yea"


def test_voteview_builders(mock_api: str, tmp_path: Path) -> None:
    s = _settings(mock_api, tmp_path)
    rc = build_rollcalls(s, 119, "senate")
    assert rc.column("chamber").to_pylist() == ["senate"] * 3
    assert rc.column("date").type == pa.date32()
    votes = build_member_votes(s, 119, None)
    assert set(votes.column("chamber").to_pylist()) == {"house", "senate"}
    assert None not in votes.column("bioguide_id").to_pylist()
    ideology = build_ideology(s, 119, None).to_pylist()
    assert sum(1 for r in ideology if r["chamber"] == "president") == 1
    assert {r["party_name"] for r in ideology if r["party_code"] == 328} == {"Independent"}


# ---------------------------------------------------------------- senate.gov


def test_parse_menu_dates_and_order() -> None:
    rows = parse_menu((FIXTURES / "senate/roll_call_lists/vote_menu_119_1.xml").read_bytes())
    assert [r["vote_number"] for r in rows] == [1, 2, 3]
    assert rows[0]["vote_date"] == dt.date(2025, 1, 9)
    assert rows[0]["question"] == "On Cloture on the Motion to Proceed"


def test_parse_menu_rolls_into_next_year() -> None:
    xml = (
        b"<vote_summary><congress>118</congress><session>1</session><congress_year>2023</congress_year><votes>"
        b"<vote><vote_number>00002</vote_number><vote_date>02-Jan</vote_date></vote>"
        b"<vote><vote_number>00001</vote_number><vote_date>21-Dec</vote_date></vote>"
        b"</votes></vote_summary>"
    )
    rows = parse_menu(xml)
    assert [r["vote_date"] for r in rows] == [dt.date(2023, 12, 21), dt.date(2024, 1, 2)]


def test_parse_vote_maps_lis_to_bioguide() -> None:
    rows = parse_vote(
        (FIXTURES / "senate/roll_call_votes/vote1191/vote_119_1_00001.xml").read_bytes(), {"S428": "A000382"}
    )
    first = rows[0]
    assert first["lis_member_id"] == "S428" and first["bioguide_id"] == "A000382"
    assert first["vote_timestamp"] == dt.datetime(2025, 1, 9, 19, 54, tzinfo=dt.UTC)
    assert rows[1]["bioguide_id"] is None


# ---------------------------------------------------------------- Congress.gov pushdown


def _params(filters: list[Any], **args: Any) -> Any:
    return SimpleNamespace(
        args=SimpleNamespace(**args),
        current_pushdown_filters=PushdownFilters(filters=tuple(filters)) if filters else None,
        attach_options={"congress_gov_url": "http://mock/congress/v3"},
    )


def _cf(name: str, op: ComparisonOp, value: Any) -> ConstantFilter:
    return ConstantFilter(column_name=name, column_index=0, op=op, value=pa.scalar(value))


def test_bills_endpoint_from_pushed_filters() -> None:
    p = _params(
        [
            AndFilter(
                column_name="",
                column_index=0,
                children=(_cf("congress", ComparisonOp.EQ, 118), _cf("bill_type", ComparisonOp.EQ, "HR")),
            ),
            _cf("update_date", ComparisonOp.GE, dt.datetime(2025, 3, 4, 12, 30, tzinfo=dt.UTC)),
            _cf("update_date", ComparisonOp.LT, dt.datetime(2025, 4, 1, tzinfo=dt.UTC)),
        ],
        congress=None,
        bill_type=None,
    )
    path, query = cg.Bills.endpoint(p)
    assert path == "/bill/118/hr"
    assert query["fromDateTime"] == "2025-03-04T00:00:00Z"
    assert query["toDateTime"] == "2025-04-01T23:59:59Z"


def test_bills_arguments_win_and_validate() -> None:
    assert cg.Bills.endpoint(_params([], congress=117, bill_type="s"))[0] == "/bill/117/s"
    assert cg.Bills.endpoint(_params([], congress=None, bill_type=None))[0] == "/bill"
    with pytest.raises(cg.CongressGovError, match="needs congress"):
        cg.Bills.endpoint(_params([], congress=None, bill_type="hr"))


def test_member_paths() -> None:
    def path(**kw: Any) -> str:
        base = {"congress": None, "state": None, "district": None, "current_only": False}
        return cg.CongressGovMembers.endpoint(_params([], **(base | kw)))[0]

    assert path() == "/member"
    assert path(congress=118, state="ca") == "/member/congress/118/CA"
    assert path(state="VT", district=0) == "/member/VT/0"
    with pytest.raises(cg.CongressGovError):
        path(district=3)


def test_missing_secret_names_the_type() -> None:
    params = SimpleNamespace(secrets=SimpleNamespace(of_type=lambda _t: []))
    with pytest.raises(cg.CongressGovError, match=r"CREATE SECRET \(TYPE us_congress"):
        cg.api_key(params)  # type: ignore[arg-type]


def test_row_mappers_on_recorded_responses() -> None:
    body = json.loads((FIXTURES / "congress/bill/118/hr.json").read_text())
    row = cg.Bills.row(body["bills"][0], _params([]))
    assert row["bill_id"] == "hr27-118" and row["origin_chamber"] == "house"
    assert row["update_date"] == dt.datetime(2025, 5, 27, tzinfo=dt.UTC)
    laws = json.loads((FIXTURES / "congress/law/118.json").read_text())
    items = cg.Laws.items(laws, 0)
    assert [cg.Laws.row(i, _params([]))["law_number"] for i in items] == ["118-90", "118-2", "118-4"]
    votes = json.loads((FIXTURES / "congress/house-vote/118/1/296/members.json").read_text())
    positions = cg.HouseVotePositions.items(votes, 0)
    p = _params([], congress=118, session=1, roll_call_number=296)
    assert cg.HouseVotePositions.row(positions[0], p)["bioguide_id"] == "P000197"


def test_converters() -> None:
    assert parse_timestamp("2023-07-13T14:05:00-04:00") == dt.datetime(2023, 7, 13, 18, 5, tzinfo=dt.UTC)
    assert parse_timestamp("2025-05-27") == dt.datetime(2025, 5, 27, tzinfo=dt.UTC)
    assert parse_timestamp("") is None and parse_timestamp("nope") is None
    assert to_int("1946.0") == 1946 and to_int("") is None
    assert [chamber_name(x) for x in ("House", "S", "House of Representatives", "President", "")] == [
        "house",
        "senate",
        "house",
        "president",
        None,
    ]


# ---------------------------------------------------------------- download cache


def test_cache_download_revalidate_and_stale_fallback(mock_api: str, tmp_path: Path) -> None:
    s = Settings(cache_dir=tmp_path, cache_ttl_hours=0)
    url = f"{mock_api}/voteview/parties/HSall_parties.csv"
    path = cached_file(s, url)
    assert path == cache_path(s, url) and path.read_bytes().startswith(b"congress,chamber")
    first = read_meta(path)
    assert first["etag"] and len(first["sha256"]) == 64
    # TTL 0: the next call revalidates (the mock answers 304 to the stored ETag).
    path2 = cached_file(s, url)
    assert path2 == path and read_meta(path)["fetched_at"] >= first["fetched_at"]
    assert "revalidated_at" in read_meta(path)
    # An unreachable upstream falls back to the stale copy.
    dead = Settings(cache_dir=tmp_path, cache_ttl_hours=0)
    moved = cache_path(dead, "http://127.0.0.1:9/voteview/parties/HSall_parties.csv")
    moved.parent.mkdir(parents=True, exist_ok=True)
    moved.write_bytes(path.read_bytes())
    moved.with_name(moved.name + ".meta.json").write_text(json.dumps({"fetched_at": 0}))
    assert cached_file(dead, "http://127.0.0.1:9/voteview/parties/HSall_parties.csv") == moved
    assert {e["path"] for e in cache_entries(s)} >= {str(path)}
