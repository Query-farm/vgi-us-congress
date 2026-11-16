<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

<p align="center"><em>A Query.Farm VGI worker for DuckDB.</em></p>

# Query US Congress Members, Roll-Call Votes, Bills and Laws in DuckDB

> **vgi-us-congress** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-us-congress/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-us-congress/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Every member of Congress since 1789, every House and Senate roll call, each member's vote,
DW-NOMINATE ideology scores, and the bills, laws, amendments, committees, nominations and
treaties on Congress.gov, all as SQL and all joined on **`bioguide_id`**.

```sql
-- Which senators broke with their party most often in the 118th Congress?
WITH v AS (
  SELECT v.bioguide_id, v.rollnumber, v.vote, i.party_name
  FROM us_congress.main.member_votes(118, chamber := 'senate') v
  JOIN us_congress.main.member_ideology(118, chamber := 'senate') i USING (bioguide_id)
  WHERE v.vote IN ('Yea', 'Nay')
), party AS (
  SELECT rollnumber, party_name, mode(vote) AS party_vote FROM v GROUP BY ALL
)
SELECT m.full_name, v.party_name, round(avg((v.vote <> p.party_vote)::INT) * 100, 1) AS pct_against_party
FROM v JOIN party p USING (rollnumber, party_name)
JOIN us_congress.main.members m USING (bioguide_id)
GROUP BY ALL ORDER BY pct_against_party DESC LIMIT 10;
```

## What's in it

| Relation | Kind | Source | Key |
|---|---|---|---|
| `members` | table | congress-legislators | none |
| `member_terms` | table | congress-legislators | none |
| `member_ideology(congress, chamber :=)` / `member_ideology(chamber :=)` | table function | Voteview | none |
| `rollcalls(congress, chamber :=)` / `rollcalls(chamber :=)` | table function | Voteview | none |
| `member_votes(congress, chamber :=)` | table function | Voteview | none |
| `senate_votes(congress, session)` | table function | senate.gov XML | none |
| `senate_vote_positions(congress, session, vote_number)` | table function | senate.gov XML | none |
| `vote_cast_label(cast_code)` | scalar | built in | none |
| `cache_files` | table function | local cache | none |
| `bills(congress :=, bill_type :=)` | table function | Congress.gov | API key |
| `bill(congress, bill_type, bill_number)` | table function | Congress.gov | API key |
| `bill_actions` / `bill_cosponsors` / `bill_subjects` / `bill_summaries` / `bill_text_versions` `(congress, bill_type, bill_number)` | table functions | Congress.gov | API key |
| `laws(congress, law_type :=)` | table function | Congress.gov | API key |
| `amendments(congress :=, amendment_type :=)` | table function | Congress.gov | API key |
| `congress_gov_members(congress :=, state :=, district :=, current_only :=)` | table function | Congress.gov | API key |
| `sponsored_legislation(bioguide_id)` / `cosponsored_legislation(bioguide_id)` | table functions | Congress.gov | API key |
| `committees(congress :=, chamber :=)` | table function | Congress.gov | API key |
| `nominations(congress :=)` | table function | Congress.gov | API key |
| `treaties(congress :=)` | table function | Congress.gov | API key |
| `house_votes(congress, session :=)` | table function | Congress.gov (beta, 118th on) | API key |
| `house_vote_positions(congress, session, roll_call_number)` | table function | Congress.gov (beta) | API key |

Conventions: Congress numbers are integers (118 = 2023–2025). `chamber` is `'house'` or
`'senate'`. Bill types are lower-case Congress.gov codes (`hr`, `s`, `hjres`, `sjres`,
`hconres`, `sconres`, `hres`, `sres`); `bill_id` is `hr2882-118`. Dates are `DATE`, instants
are `TIMESTAMPTZ`, identifiers stay `VARCHAR`.

## Quick start

**Prerequisites:** Python 3.13+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/Query-farm/vgi-us-congress && cd vgi-us-congress
make build
```

Attach the worker from **DuckDB 2.0**. `LOCATION` is the command DuckDB runs, so give it an
absolute path:

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
ATTACH 'us_congress' (TYPE vgi, LOCATION '/abs/path/to/vgi-us-congress/.venv/bin/vgi-us-congress-worker');

SELECT latest_party, count(*) FROM us_congress.main.members
WHERE is_current AND latest_chamber = 'senate' GROUP BY ALL;
```

Or attach it from **Haybarn 2.0** (`uvx haybarn-cli`): `INSTALL vgi FROM community; LOAD vgi;`,
then the same `ATTACH`.

Once a release is tagged you can run it without cloning:
`LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-us-congress@v0.1.0 vgi-us-congress-worker'`.

## API key

The member, Voteview and Senate relations need no key. The Congress.gov relations need a free
[api.data.gov key](https://api.congress.gov/sign-up/) in a DuckDB secret:

```sql
CREATE SECRET (TYPE us_congress, API_KEY 'your-api.data.gov-key');
SELECT bill_id, title, latest_action_text FROM us_congress.main.bills(congress := 119, bill_type := 'hr') LIMIT 10;
```

Without the secret those scans fail with a message naming `TYPE us_congress`; `DESCRIBE` and
`EXPLAIN` still work. The key is sent in the `X-Api-Key` header, never in a URL, a log line or a
scan cursor. api.data.gov allows 5,000 requests per hour per key; a list function makes one
request per 250 rows.

## Pushdown

* **Congress.gov lists.** `bills`, `amendments` and `nominations` turn `WHERE congress = N`,
  `WHERE bill_type = 'hr'` / `amendment_type = '...'` into the request path, and ranges on
  `update_date` into `fromDateTime` / `toDateTime` (widened to whole days; DuckDB re-applies every
  predicate). The same filters are also named arguments, as are `law_type`, `chamber`, `state`,
  `district`, `current_only` and `session` on the other list functions. Without a Congress filter
  `bills()` pages through every bill since 1973, so filter or `LIMIT`.
* **Voteview.** The `congress` and `chamber` arguments choose which files are downloaded:
  `member_votes(118, chamber := 'senate')` reads `S118_votes.csv` (a few MB), never the 700 MB
  `HSall_votes.csv`. `rollcalls()` and `member_ideology()` without a Congress read the
  whole-history files (30 MB and 6 MB).
* **senate.gov.** One request per session menu or per vote.

## Caching

Bulk files (congress-legislators JSON, Voteview CSV, Senate XML) are downloaded on first use,
never at import or `ATTACH`, into a per-user cache (`~/Library/Caches/vgi-us-congress` on macOS,
`$XDG_CACHE_HOME/vgi-us-congress` on Linux, `%LOCALAPPDATA%\vgi-us-congress\Cache` on Windows).
Each download is written atomically, checked against `Content-Length`, and recorded with its
ETag, Last-Modified and SHA-256. After the TTL a file is revalidated with a conditional GET; if
the upstream is unreachable the stale copy is used. `cache_files` lists what is cached.

| ATTACH option | Default | Purpose |
|---|---|---|
| `cache_dir` | per-user cache dir | where downloads go |
| `cache_ttl_hours` | 24 | hours before a cached file is revalidated (Senate vote files: 30 days) |
| `congress_gov_url` | `https://api.congress.gov/v3` | Congress.gov API root |
| `legislators_url` | `https://unitedstates.github.io/congress-legislators` | congress-legislators root |
| `voteview_url` | `https://voteview.com/static/data/out` | Voteview data root |
| `senate_url` | `https://www.senate.gov/legislative/LIS` | Senate roll-call XML root |

The URL options exist so tests (and mirrors) can point the worker elsewhere; the test suite
points them at `test/mock_api/server.py`.

## Examples

```sql
-- Who represented a state on a given day?
SELECT t.bioguide_id, m.full_name, t.party FROM us_congress.main.member_terms t
JOIN us_congress.main.members m USING (bioguide_id)
WHERE t.chamber = 'senate' AND t.state = 'CA' AND DATE '2000-01-01' BETWEEN t.start_date AND t.end_date;

-- Polarization: the gap between the parties' Senate medians, by Congress.
SELECT congress, round(median(nominate_dim1) FILTER (WHERE party_code = 200)
                     - median(nominate_dim1) FILTER (WHERE party_code = 100), 3) AS gap
FROM us_congress.main.member_ideology(chamber := 'senate') WHERE congress >= 90 GROUP BY congress ORDER BY congress;

-- Today's Senate votes and how each senator voted on the latest one.
SELECT * FROM us_congress.main.senate_votes(119, 2) ORDER BY vote_number DESC LIMIT 5;
SELECT party, vote_cast, count(*) FROM us_congress.main.senate_vote_positions(119, 2, 1) GROUP BY ALL;

-- A bill's sponsor and its recorded votes (needs the secret).
SELECT title, sponsor_bioguide_id, became_law FROM us_congress.main.bill(118, 'hr', 2882);
SELECT action_date, action_text FROM us_congress.main.bill_actions(118, 'hr', 2882) WHERE vote_roll_number IS NOT NULL;
```

## Data sources and terms

The code is BSD Zero Clause licensed; the data it reads is not part of that licence and each
upstream's terms apply. Nothing is bundled in the package: every dataset is fetched at runtime.
See [DATA-SOURCES.md](DATA-SOURCES.md) for URLs, licences and the small test fixtures.

* **[unitedstates/congress-legislators](https://github.com/unitedstates/congress-legislators)**:
  CC0 1.0 public-domain dedication.
* **[Voteview](https://voteview.com/data)**: free to use with citation. Please cite: Lewis,
  Jeffrey B., Keith Poole, Howard Rosenthal, Adam Boche, Aaron Rudkin, and Luke Sonnet (2026).
  *Voteview: Congressional Roll-Call Votes Database*. https://voteview.com/
* **[Senate roll-call votes](https://www.senate.gov/legislative/votes_new.htm)** and the
  **[Congress.gov API](https://github.com/LibraryOfCongress/api.congress.gov)**: works of the US
  government, public domain. Congress.gov requires an api.data.gov key and its rate limits.

House votes from Congress.gov are a **beta** endpoint covering legislation-related votes from the
118th Congress on; for older or procedural House votes use Voteview's `rollcalls`/`member_votes`.

## Development

```bash
make check        # ruff, mypy, pydoclint, pytest, SQL tests over stdio/launch/http, vgi-lint
make test-unit    # pytest only
make test-sql     # SQL tests through Haybarn (TRANSPORT=stdio|launch|http)
make lint-explain # metadata quality report
make live-check   # opt-in: a few keyless queries against the real upstreams
```

Every test runs offline: `test/mock_api/server.py` serves trimmed real responses from
`test/fixtures` for all four upstreams, rate-limits each URL once to exercise retries, and pages
Congress.gov lists two items at a time to exercise the cursor.

## Support

Open an issue at https://github.com/Query-farm/vgi-us-congress/issues.

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the BSD Zero Clause License
(see [LICENSE](LICENSE)).

Copyright 2026 Query Farm LLC - https://query.farm
