# Fantasy Football V2 Historical Context

**Status:** populated shadow data; not active in displayed projections or ranks
**Ledger target:** V2-003
**Transform:** `ff-v2-context-v1`

This artifact supplies the effective-dated roster and team-week history needed
before the roster-aware challenger can be fitted. It is shared infrastructure
for redraft and Best Ball. It does not forecast opportunity and does not change
the live `ff-independent-v1.14` champion.

## Reproducible run

The canonical 2020-2025 run is
[`../artifacts/ff_v2_historical_context_2020_2025.json`](../artifacts/ff_v2_historical_context_2020_2025.json).

| Field | Value |
|---|---|
| Run ID | `9077ad91-e258-5e47-beb8-f41b68c6651b` |
| Artifact digest | `64fee3305b76dbc3d9bda9f6cf9d722c05bb9077a535bbb884ab90297cf39f3c` |
| Team-week contexts | 3,424 |
| Game-team opportunity facts | 3,230 |
| Fantasy-relevant roster weeks | 85,688 |
| Effective team changes | 2,378 |
| Unique players | 2,214 |

The run covers all 32 teams in every regular-season week from 2020 through
2025. The 3,424 contexts include 194 bye team-weeks. The remaining 3,230 rows
are the two team sides of 1,615 scheduled games. The coverage audit reports no
missing or extra game-team facts, no missing teams, no unresolved multi-team
roster conflicts, and no unknown rusher or receiver positions.

## Source and identity contract

Raw nflverse schedule, weekly roster, weekly player-stat, play-by-play, and
participation files are cached by SHA-256 under `data/ff_v2_sources`. Every
partition is registered through the shared immutable source-snapshot contract
with its exact URL, response hash, fetch time, source publication time, row
count, and snapshot ID.

Historical membership comes only from the requested season's weekly roster and
weekly-stat partitions. A player appearing on multiple teams in one week is
resolved only when that week's stats identify the effective team; unresolved
conflicts are skipped and counted. Transactions are inferred from changes in
the ordered weekly roster history and are effective at the first week of the
new team. Current rosters are never consulted.

The persisted tables are:

- `ff_v2_context_runs`: run metadata, inputs, coverage, and artifact digest.
- `ff_v2_team_week_context`: opponent, venue, game/bye state, quarterback, and
  source lineage for each team-week.
- `ff_v2_roster_weeks`: effective weekly player-team membership for fantasy
  positions `QB`, `RB`, `FB`, `WR`, `TE`, and `K`.
- `ff_v2_transactions`: effective team changes inferred from roster weeks.
- `ff_v2_team_week_facts`: reconciled observed team opportunity and environment
  facts for each game-team.

All rows are keyed by the deterministic run ID, and game facts also carry a
row-level digest and their contributing snapshot IDs.

## Count semantics

- `plays`: pass-attempt flags, including sacks, plus non-kneel rush-attempt
  flags; `no_play` rows are excluded.
- `pass_attempts`: official pass-attempt flags minus sacks. Spikes remain;
  throwaways can be official attempts without an allocatable receiver.
- `allocatable_targets`: pass attempts with a named target.
- `rush_attempts`: rush-attempt flags excluding quarterback kneels; scrambles
  remain.
- `rb_carries` and `rb_targets`: opportunities assigned to `RB` or `FB` using
  same-season weekly stats first and weekly rosters second.
- `goal_line_carries`: non-kneel rushes at or inside the opponent five-yard
  line.
- `end_zone_targets`: named targets whose air yards reach the goal line from
  the snap location.
- `red_zone_trips`: unique offensive drives with a valid scrimmage snap at or
  inside the opponent 20-yard line.
- `neutral_pass_rate`: pass share while the score is within seven points.
- `seconds_per_play`: elapsed offensive seconds divided by counted plays.

These definitions are persisted in each fact's derivation payload. They are
inputs to future rolling-origin modeling, not claims about an optimal forecast.

## Build and replay

```powershell
python -m ingest.ff_v2_historical_context `
  --start-season 2020 `
  --end-season 2025 `
  --artifact artifacts/ff_v2_historical_context_2020_2025.json

python -m ingest.ff_v2_historical_context `
  --verify artifacts/ff_v2_historical_context_2020_2025.json
```

Verification reloads the hash-pinned raw files, rebuilds every row, checks the
run ID and artifact digest, and compares the rebuilt counts with the persisted
database run. The canonical replay returned `status: verified` with all four
persisted row counts matching.

## Known boundary

The source set provides quarterback identity but not an attributable historical
play-caller ID, so `play_caller_id` is explicitly null in this foundation run.
This missing field is reported in coverage rather than inferred.

The current nflverse release bytes for earlier seasons were published after the
frozen historical cutoffs, so they remain ineligible for rolling-origin fitting.
`ingest/ff_v2_archived_team_context.py` separately recovers exact pre-cutoff Git
blobs and an archived release asset for 2020-2021. It verifies SHA-256 for every
file, Git object identity where applicable, and complete offensive-player
position coverage before persisting append-only training facts. The two bundles
replay to 512 facts for the 2021 cutoff and 1,056 facts for the 2022 cutoff,
cover all 32 teams, and contain zero unknown rusher or receiver positions.

That archive intentionally declares weekly stats, participation, schedule,
transactions, quarterback, and play-caller context missing. Team Opportunity
therefore uses it only as Tier C evidence; missing inputs are not replaced with
zero or reconstructed from future data.
