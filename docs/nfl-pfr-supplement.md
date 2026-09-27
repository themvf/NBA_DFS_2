# PFR game supplements

## Working 2026 collection path

The public nflverse PFR weekly release is accessible even though direct PFR
boxscores currently return a Cloudflare browser challenge. To import advanced
passing, rushing, receiving and defense for every completed game:

```powershell
python -m ingest.nfl_pfr_nflverse --season 2026 --write-db
```

`--week` and repeatable `--game` optionally narrow the selection. It writes the
same revisioned supplement tables and puts JSON and a coverage report in
`data/pfr/nflverse-output/`. Every game's identities and percentage units are
validated before database writes start. Starters and snaps remain missing;
the overall snapshot is consequently `partial` even when all four advanced
sections are present. This feed supplies a subset of boxscore fields.

Provenance distinguishes `source_provider=nflverse_pfr` from HTML collection,
including each input file's URL and SHA-256. `stats` retains nflverse field
names and converts `_pct` fractions to percentage points; `raw` keeps the
original fraction. The `stats_schema` marker makes this explicit. HTML and
nflverse field names are different; consumers must use that source's schema.
`source_sha256` for this adapter hashes the selected game rows; original CSV
hashes are in `source_files`. Capture time is download/import time, never a
backdated claim that historical data was already available.

Tests: `python -m pytest tests/test_nfl_pfr_supplement.py tests/test_nfl_pfr_nflverse.py`.

`python -m ingest.nfl_pfr_supplement` collects advanced passing, rushing,
receiving, defense, starters and snap counts from completed PFR boxscores.
The exact nflverse schedule `pfr` identifier maps each page to the same
`game_id` used by `nfl_pbp_archetypes`. No date/team URL guessing is used.
Regular season and postseason games are supported when the schedule supplies
completed scores and a PFR identifier.

## Run

Single game, JSON only (default):

```powershell
python -m ingest.nfl_pfr_supplement --season 2025 --game 2025_01_DAL_PHI
```

Persist a completed week, or omit `--week` for the completed season:

```powershell
python -m ingest.nfl_pfr_supplement --season 2026 --week 2 --write-db
```

Use `--limit 1` for a small pilot. `--game` is repeatable and matches exact IDs.
Default proxy configuration reuses `WEBSHARE_PROXY_USERNAME` and
`WEBSHARE_PROXY_PASSWORD` from the existing environment. `PFR_PROXY_URL`
overrides that with an authenticated HTTP(S) proxy URL. A provider management
API key alone is not a proxy URL. Credentials never belong in CLI arguments,
artifacts or source control. `--direct` explicitly selects a direct connection.

The client makes requests serially, at least six seconds apart. It uses the
existing Webshare rotating endpoint. HTTP 401/403/407/429 or a recognized
challenge stops the batch without retrying through another IP. Other HTTP
failures are reported per game. Cached validated pages
avoid repeat requests; `--refresh` fetches revisions. A failed page cannot
replace an existing validated snapshot. Concurrent collector runs should be
avoided: the rate limit is per process.

Offline import accepts saved **HTML**, including the canonical URL and table
markup. Plain pasted text is not sufficient for reliable player IDs:

```powershell
python -m ingest.nfl_pfr_supplement --season 2025 --game 2025_01_DAL_PHI --html-dir data/pfr/saved --write-db
```

Name files `<pfr_id>.htm`, for example `202509040phi.htm`. `--schedule-csv`
accepts a local copy of nflverse `games.csv` for fully offline processing.

## Outputs and integration

- `data/pfr/cache/<pfr_id>.json`: validated HTML and original capture timestamp.
- `data/pfr/output/<game_id>.json`: parsed game supplement.
- `data/pfr/output/run-report.json`: complete/partial/failed/not-attempted
  coverage. A partial page is explicitly identified, not treated as zero.
- `--write-db` creates only the supplemental schema and saves revisioned
  `nfl_pfr_game_snapshots`. Cached replay is idempotent. Fresh captures are
  retained even when their content is unchanged, preserving observations.
- `nfl_pfr_game_latest` exposes one snapshot per game;
  `nfl_pfr_player_game_latest` exposes section/player rows for SQL consumers.
- `db.nfl_pfr_schema.read_game_supplement(db, game_id, as_of=...)` returns a
  single companion object for a PBP/game report. It resolves PFR player IDs
  through the existing `pfr` identity crosswalk when available. Conflicts and
  unmatched players retain their source identity and no GSIS ID. Do not join
  the player view directly to every play: that would multiply play counts.

Example query alongside the existing PBP:

```sql
SELECT p.game_id, count(*) AS pbp_rows,
       s.status AS pfr_status, s.payload->'coverage' AS pfr_coverage
FROM nfl_pbp_archetypes p
LEFT JOIN nfl_pfr_game_latest s ON s.game_id = p.game_id
WHERE p.season = 2026
GROUP BY p.game_id, s.status, s.payload;
```

The parser retains original `data-stat` names, source display strings, header
definitions, numeric values, URL, content hash and parser version. Percentages
are percentage points (18.5 means 18.5%), and blank cells remain null. A
player can appear in several sections; those are separate observations.
Defender pressure credits must not be summed into unique team pressure plays.

These are **game-level charting observations**. They never fill missing PBP
pressure/blitz fields, produce pressured EPA, or change pick'em probabilities.
The existing PBP and pick'em UI remain unchanged; consumers opt into the
companion object. Stat-specific predictive features require separate validation.

`captured_at` is observation/import time, not the historical publication time.
Offline files are first observed when imported. An `as_of` read filters captures
and prevents a newly collected historical boxscore from appearing available
before collection. Player identity resolution uses the current crosswalk;
it is not a historical identity snapshot.

## Verification and current limitation

Run `python -m pytest tests/test_nfl_pfr_supplement.py`.
Tests cover commented tables, null versus zero, percentages, missing sections,
identity mismatches, malformed pages, completed-game selection, cached replay,
proxy encoding and stopping blocked batches. Fixtures are synthetic HTML, not
evidence of a successful live PFR collection.

Database storage was also verified in an isolated PostgreSQL schema inside a
rolled-back transaction: schema creation, idempotent replay, player view,
revision retention and as-of reads passed. No synthetic game data was retained.

The September 27, 2026 pilot through the configured Webshare proxy returned
HTTP 403. No live PFR game data was loaded. Live markup compatibility and a
successful end-to-end proxy capture remain unverified until access works or
a genuine saved HTML boxscore is supplied. No scheduled scraping job is enabled.

Follow-up diagnosis confirmed the configured proxy fetched an independent
HTTPS control page successfully (HTTP 200). PFR returned HTTP 403 with
`server: cloudflare`, `cf-mitigated: challenge`, title `Just a moment...`, and
`Enable JavaScript and cookies to continue`. This is a PFR-side browser
challenge, not evidence of missing proxy credentials. The collector now
reports that specific cause, separately from HTTP 407 proxy authentication.
