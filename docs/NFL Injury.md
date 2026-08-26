# NFL Injury Intelligence

## Purpose

Build a deterministic, source-attributable NFL injury system for the redraft
and Best Ball products. This system is independent of the AI draft advisors.
It must identify new injuries, status and timeline changes, and cleared
injuries without silently treating a missing or failed provider response as a
clearance.

The current `ff_players.injury_status` column remains a convenient current-state
cache. It is not the historical source of truth.

## Source policy

- nflverse remains the canonical current-season player universe.
- Sleeper is the primary no-auth current-status and depth-chart enrichment
  source. Only Sleeper players matched into the nflverse universe are eligible.
- FantasyPros is an optional richer injury source. Preserve its status,
  body-part/type, comment, provider update time, IR weeks, practice fields,
  availability probabilities, and return estimate when present.
- FantasyPros unavailability must not block the independent board.
- News is optional context. A news item cannot automatically override a
  structured injury observation.
- Every displayed fact must retain source and provider/capture timestamps.

## Storage contract

### `ff_player_injury_observations`

Append-only provider observations, deduplicated by source snapshot, player, and
source. Store:

```text
player_id, season, source, source_snapshot_id
source_status, normalized_status
body_part, injury_type, description, practice_status
injury_started_at, provider_updated_at
expected_return_min, expected_return_max
weeks_out_min, weeks_out_max
availability_probability
raw_payload, response_hash, observed_at
```

`raw_payload` is provenance, not a UI contract. UI and model code consume only
normalized columns.

### `ff_player_injuries`

One canonical row per injury episode:

```text
player_id, season, status, body_part, injury_type
first_seen_at, last_confirmed_at, cleared_at
expected_return_min, expected_return_max
weeks_out_min, weeks_out_max
estimate_basis, confidence, primary_source
source_conflict, active
```

At most one episode may be active per player.

### `ff_injury_events`

Append-only meaningful transitions:

```text
NEW_INJURY, STATUS_CHANGED, TIMELINE_CHANGED
PRACTICE_UPGRADE, PRACTICE_DOWNGRADE
CLEARED, RETURNED, SOURCE_CONFLICT
```

Each event stores the previous state, new state, source observation, and event
time. Identical provider payloads must not create duplicate events.

## Normalization and reconciliation

Canonical status values are:

```text
HEALTHY, QUESTIONABLE, DOUBTFUL, OUT, IR, PUP, NFI, SUSPENDED, UNKNOWN
```

- A non-healthy observation opens or updates the active episode.
- A successful full Sleeper player observation with a blank/healthy status may
  clear the episode. A failed Sleeper refresh performs no writes.
- An explicit FantasyPros healthy/active status may clear an episode.
- Mere absence from the FantasyPros injury list never clears an episode.
- Source disagreements are preserved and surfaced; they are never erased by
  overwriting raw observations.
- Ambiguous identity matches are quarantined and reported, never guessed.

## Timeline policy

Timeline fields must state their basis:

```text
provider                 Provider supplied a date/range or IR-week estimate
return-date-derived      Range derived from an explicit provider return date
status-heuristic         Coarse fallback based only on designation
unknown                  No defensible timeline
```

A heuristic is not a medical forecast. The live V1 baseline continues to show
17 active-game production. Richer availability estimates remain shadow data
until chronological calibration justifies changing projections.

Best Ball eventually needs week-specific availability distributions because an
early absence and a Weeks 15-17 absence have different roster consequences.

## Product behavior

Player rows should ultimately show compact, sourced badges such as:

```text
Q · Hamstring · 1-2 wk
OUT · Knee · Return unknown
CLEARED 6h ago
```

An injury detail drawer should show current status, body part/type, practice
progression, first seen, latest update, expected-return range, estimate basis,
source agreement, freshness, and the transition timeline.

Global filters:

- New injuries
- Recently cleared
- Timeline worsened
- Practice upgrades/downgrades
- Source conflicts
- Drafted players only

Recently cleared context should remain visible for 24-48 hours.

## Refresh cadence

- Run a lightweight injury refresh every two hours from July through January,
  subject to provider entitlement and rate limits.
- Keep the full draft-board rebuild on its existing cadence.
- A meaningful transition should refresh injury indicators without requiring a
  full historical-stat rebuild.
- Mark injury data stale after twice the expected refresh interval.

## Implementation phases

1. **Storage and ingestion** — create the observation, episode, and event
   tables; persist full matched Sleeper and FantasyPros injury observations.
2. **Reconciliation** — normalize statuses, open/update/clear episodes, emit
   idempotent events, and surface conflicts and match coverage.
3. **Read model and UI** — expose current details and recent events in redraft
   and Best Ball, add filters and a detail drawer.
4. **Lightweight workflow** — separate injury refresh from the expensive board
   rebuild and add freshness/coverage monitoring.
5. **Shadow availability model** — store P10/P50/P90 games or weeks missed and
   validate chronologically.
6. **Promotion gate** — activate projection or Best Ball simulation effects
   only after calibration and coverage tests pass.

## Current implementation status

Implemented in the first slice:

- All three injury-history tables and supporting indexes exist in the Python,
  Drizzle, and runtime ensure-schema definitions.
- Sleeper observations are persisted for every matched offensive player during
  the full independent refresh.
- Sleeper opens, updates, and clears canonical episodes and emits idempotent
  `NEW_INJURY`, `STATUS_CHANGED`, `TIMELINE_CHANGED`, and `CLEARED` events.
- FantasyPros injury rows are identity-matched and persisted with richer fields
  in shadow mode.
- FantasyPros cannot yet mutate canonical current state. This is deliberate
  until the cross-source disagreement and freshness policy is implemented.
- Live projection scoring is unchanged.

Still pending: cross-source conflict reconciliation, practice transition
events, a lightweight injury-only workflow, queries/UI, news ingestion,
timeline calibration, and the projection promotion gate.

## Acceptance criteria

- A new non-healthy state creates exactly one `NEW_INJURY` event.
- Identical reprocessing is idempotent.
- A failed or partial provider refresh cannot clear an injury.
- A valid clearance closes the active episode, updates the player cache, and
  creates exactly one `CLEARED` event.
- Every displayed detail has source and freshness provenance.
- Source conflicts remain visible.
- Ambiguous player matches are never guessed.
- FantasyPros injury failure cannot block the independent board.
- No new timeline field changes the live projection until its promotion gate
  passes.
