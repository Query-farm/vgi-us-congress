# Data sources

The worker bundles **no datasets** in its package. Every relation reads its upstream at
runtime: bulk files are downloaded on first use into a per-user cache (see README,
"Caching"); the Congress.gov API is read live. The upstream terms below apply to that data;
they are separate from the worker's BSD Zero Clause code licence.

| Upstream | URL | Licence / terms | Used by | Fetched |
|---|---|---|---|---|
| unitedstates/congress-legislators | https://unitedstates.github.io/congress-legislators/ (`legislators-current.json`, `legislators-historical.json`, ~15 MB) | CC0 1.0 public-domain dedication (https://github.com/unitedstates/congress-legislators) | `members`, `member_terms`, lis/icpsr → bioguide cross-walks | runtime, cached, revalidated after TTL |
| Voteview | https://voteview.com/static/data/out/{rollcalls,votes,members,parties}/ (`H118_votes.csv`, `HSall_rollcalls.csv`, ...) | No licence stated; the data page asks users to cite: Lewis, Jeffrey B., Keith Poole, Howard Rosenthal, Adam Boche, Aaron Rudkin, and Luke Sonnet (2026). *Voteview: Congressional Roll-Call Votes Database*. https://voteview.com/ | `rollcalls`, `member_votes`, `member_ideology` | runtime only (never bundled), cached |
| US Senate roll-call votes | https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{congress}_{session}.xml and `roll_call_votes/vote{c}{s}/vote_{c}_{s}_{nnnnn}.xml` | US government work, public domain | `senate_votes`, `senate_vote_positions` | runtime, cached (vote files 30 days) |
| Congress.gov API v3 (Library of Congress) | https://api.congress.gov/v3 (docs https://github.com/LibraryOfCongress/api.congress.gov) | US government work, public domain; access needs an api.data.gov key and is rate-limited (5,000 requests/hour/key) | `bills`, `bill`, `bill_*`, `laws`, `amendments`, `congress_gov_members`, `*_legislation`, `committees`, `nominations`, `treaties`, `house_votes`, `house_vote_positions` | live, not cached |

Voteview is only ever downloaded by the user's own worker process, so its terms (citation)
travel with the use; the worker never redistributes Voteview files.

## Test fixtures (`test/fixtures`, not shipped in the wheel)

Small excerpts used by the offline test suite. Retrieved 2026-11-15 with a generic
`vgi-us-congress/0.1 (+https://query.farm)` User-Agent, then trimmed. Total 89 KB.

* **congress-legislators** (CC0): 14 people (11 current, 3 historical: Robert Byrd, Patrick
  Leahy, James Madison) copied verbatim from the two JSON files.
* **Voteview** (cited above): a handful of rows from the S119, H118 and H119 rollcall/vote/member
  files and the 118th–119th Congress rows of the parties file; `HSall_*` fixtures are the union of
  the per-chamber fixtures. These are short factual excerpts kept only to test parsing.
* **senate.gov** (public domain): the 119th Congress session-1 vote menu trimmed to votes 1–3,
  and vote 1 trimmed to six senators.
* **Congress.gov** (public domain): recorded with the published `DEMO_KEY` (10 requests:
  bill list, H.R. 2882 detail/actions/cosponsors/subjects/summaries/text, law list, House vote
  list, members by state), with `pagination`/`request` blocks removed (the mock server rebuilds
  pagination). One action on H.R. 2882 with a `recordedVotes` entry was added by hand. The
  `bill.json` (unfiltered listing) sentinel, `amendment/118/samdt`, `committee/118/senate`,
  `nomination/119`, `treaty/117`, `house-vote/118/1/296/members` and
  `member/S000148/*-legislation` fixtures are **hand-written** in the documented response shape
  (the demo key's hourly quota was spent); their values are illustrative, not records.

| File | Bytes | SHA-256 (first 16) |
|---|---|---|
| `test/fixtures/congress/amendment/118/samdt.json` | 1127 | dc6828cced38a759 |
| `test/fixtures/congress/bill.json` | 482 | fa4c6025a0dd874f |
| `test/fixtures/congress/bill/118.json` | 1467 | 035473727952e5db |
| `test/fixtures/congress/bill/118/hr.json` | 1467 | 035473727952e5db |
| `test/fixtures/congress/bill/118/hr/2882.json` | 2510 | 04ba518ecace2a8c |
| `test/fixtures/congress/bill/118/hr/2882/actions.json` | 1611 | 4d77c3a04a88af0d |
| `test/fixtures/congress/bill/118/hr/2882/cosponsors.json` | 1018 | a3b100c10370374e |
| `test/fixtures/congress/bill/118/hr/2882/subjects.json` | 513 | c9bee581b64e0852 |
| `test/fixtures/congress/bill/118/hr/2882/summaries.json` | 1698 | 14bba2e7fbe30ff4 |
| `test/fixtures/congress/bill/118/hr/2882/text.json` | 3194 | f8327a478e15db82 |
| `test/fixtures/congress/committee/118/senate.json` | 1142 | 71d156810da895aa |
| `test/fixtures/congress/house-vote/118/1.json` | 1137 | 2f5c3aaed0a92fc1 |
| `test/fixtures/congress/house-vote/118/1/296/members.json` | 1185 | 940bf5cbeba66ef6 |
| `test/fixtures/congress/law/118.json` | 1561 | 52f7e785240e7e4c |
| `test/fixtures/congress/law/118/pub.json` | 1561 | 52f7e785240e7e4c |
| `test/fixtures/congress/member/congress/118/CA.json` | 1067 | 642a3b279cb5aedf |
| `test/fixtures/congress/member/S000148/cosponsored-legislation.json` | 791 | 53d1b331e4ce2347 |
| `test/fixtures/congress/member/S000148/sponsored-legislation.json` | 1061 | 556272c7a6e549d7 |
| `test/fixtures/congress/nomination/119.json` | 1708 | a2afd89567b03389 |
| `test/fixtures/congress/treaty/117.json` | 689 | 1222248a53123a7b |
| `test/fixtures/legislators/legislators-current.json` | 35864 | fda50351acdad240 |
| `test/fixtures/legislators/legislators-historical.json` | 5946 | 3ce67b7766b50085 |
| `test/fixtures/senate/roll_call_lists/vote_menu_119_1.xml` | 1706 | 3b3fe8f09136f430 |
| `test/fixtures/senate/roll_call_votes/vote1191/vote_119_1_00001.xml` | 3497 | cddd1e675b8180c1 |
| `test/fixtures/voteview/members/H118_members.csv` | 1143 | 212b2261298fb275 |
| `test/fixtures/voteview/members/H119_members.csv` | 1369 | 64a04f203245a15e |
| `test/fixtures/voteview/members/HSall_members.csv` | 3060 | ed944f7e55a4c610 |
| `test/fixtures/voteview/members/S119_members.csv` | 1244 | e138b42df84621a4 |
| `test/fixtures/voteview/parties/HSall_parties.csv` | 1078 | 14d171fa0c6665e1 |
| `test/fixtures/voteview/rollcalls/H118_rollcalls.csv` | 462 | d86c75382285a478 |
| `test/fixtures/voteview/rollcalls/H119_rollcalls.csv` | 358 | 1adb1cecfbe7219d |
| `test/fixtures/voteview/rollcalls/HSall_rollcalls.csv` | 1364 | c34ff48c215c4784 |
| `test/fixtures/voteview/rollcalls/S119_rollcalls.csv` | 1036 | fe231ea1428b9cb0 |
| `test/fixtures/voteview/votes/H118_votes.csv` | 204 | bcdae0c9d2d21af9 |
| `test/fixtures/voteview/votes/H119_votes.csv` | 177 | 6de75b7e90df3d2d |
| `test/fixtures/voteview/votes/S119_votes.csv` | 417 | 8643098fbaa14912 |
