# NFL matchup reports and morning refresh

For the proposed connection from these reports to numeric forecasts and
contest decisions, see the [matchup data and projection improvement
specification](./nfl-matchup-data-projection-spec.md). The behavior described
below remains descriptive until the corresponding changes are implemented
and qualified.

Generate one matchup or all games in the next seven days:

```powershell
python -m research.nfl_matchup_report --game 2026_03_MIN_TB
python -m research.nfl_matchup_report --upcoming
```

Timestamped Markdown and JSON reports go to `artifacts/nfl-matchups/`.
Each report combines the latest eligible two-sided market capture, up to four
prior completed games of PBP, PFR charting, and recent availability observations.
The prior-game list and capture timestamps are explicit. Games with missing
PBP or PFR are flagged, not given zero performance. Market quotes use the
existing freshness rule: two hours within 24 hours of kickoff, otherwise 24
hours. Availability observations older than 24 hours trigger a warning.

The reports remain descriptive. No market probabilities are changed.
PBP is the current revision, so historical as-of reports are not valid
point-in-time backtests. PFR captures and odds/injury observations respect
the cutoff; no target-game plays are included. Opposing-QB PFR pressure
observations describe a team's defense without summing defender pressure credits.
Rates stay per game because rounded percentages do not supply exact denominators.
The JSON retains all receiving and defensive fields for deeper analysis.

## Pick'em page

The pick'em Evidence & decision panel reads the same database snapshots directly
and displays Pressure, protection & contact for each team, with up to four prior
completed games. Expand a prior game to see own and opposing quarterbacks,
rushing contact, receiving and defensive details. Captures after the target
kickoff are excluded; absent or incompatible snapshots are marked incomplete.
The database reader fetches each distinct snapshot once. Source percentages
remain percentage points and nulls display as missing, not zero. This evidence
is included in the existing saved-card evidence snapshot and does not adjust
market probabilities. The page is dynamic, so future ingestion refreshes appear
on the next page load without generating local Markdown first.

## Morning operations

Requested cadence: Friday, Monday and Tuesday at 7 a.m. America/New_York.
Use the Codex task automation attached to this conversation; this is a local
automation, not a deployed GitHub Actions workflow. The computer and app must
be running and the workspace available.

On each run derive the NFL season as the previous calendar year in January
through March, otherwise the current calendar year. Refresh the season schedule,
current-season PBP (without a stale local cache), PFR statistics through nflverse,
market feeds and availability; then generate upcoming matchup reports.
Use these existing entry points, substituting the derived year:

```powershell
python -m ingest.nfl_season_schedule --season 2026
python -m ingest.nfl_pbp_archetypes --season 2026
python -m ingest.nfl_pfr_nflverse --season 2026 --write-db
python -m ingest.refresh_nfl_vegas
python -m ingest.nfl_availability_operations --season 2026 --mode capture --force
python -m research.nfl_matchup_report --upcoming
```

Record each exit status. Continue independent refreshes after an individual
failure; do not label the aggregate run successful when any source failed.
The PFR collector uses fresh downloads unless explicitly given `--input-dir`.
Exit 2 signals completed games awaiting publication or incomplete advanced
sections, while still importing available validated games. Malformed identity
data fails before any game writes. Expected missing starters/snaps alone do
not cause exit 2. Inspect the JSON coverage report, not only the command status.

Verify each completed game's PBP/PFR coverage and inspect report freshness
warnings. New Sunday/Monday/Thursday games may not have charting published by
7 a.m.; the run can collect only what the source has released. Report those
specific gaps. Do not repeatedly notify about unchanged expected missing
starters/snaps or the known direct-PFR Cloudflare challenge.

Notify for actionable changes in matchup evidence, new gaps, failures or
required user action. Quiet successful refreshes still retain timestamped
reports. Official game-day inactives and lineup decisions remain a separate
verification close to kickoff; a morning refresh cannot guarantee them.
