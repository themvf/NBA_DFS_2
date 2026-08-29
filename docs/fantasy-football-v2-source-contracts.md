# Fantasy Football V2 Source Contracts

**Status:** Active foundation contract for the shadow V2 challenger
**Contract owner:** `ingest/ff_source_contracts.py`
**Provenance table:** `ff_source_snapshots`

These contracts govern football-performance inputs for roster-aware V2. Market
rankings and ADP may be stored in the same provenance table, but they must have
`model_eligible = false` and cannot become performance features.

## Immutable snapshot contract

Each consumed response is identified by `(source, dataset, response_hash)`.
Re-fetching identical bytes returns the original snapshot ID and does not
rewrite its fetch time, request parameters, cutoff, or eligibility. A corrected
upstream response has a new SHA-256 hash and therefore creates a new snapshot.

Every V2-eligible snapshot records:

- source, dataset, contract key, season, and week where applicable;
- requested parameters and SHA-256 response hash;
- UTC fetch time, source-published time when supplied, conservative
  `available_at`, and the simulated `as_of_at` cutoff;
- row, matched, and unmatched counts;
- structured missingness, status, and error summary;
- fallback tier, confidence multiplier, model eligibility, and eligibility
  reason.

`source_published_at` is preferred for historical eligibility. When it is not
available, the system conservatively uses `fetched_at`. An input whose
availability timestamp is newer than `as_of_at` is rejected; it is never
silently included in a historical example.

## Required football inputs

| Contract | Source and license | Required fields | Cadence and history | Explicit fallback |
|---|---|---|---|---|
| Weekly rosters | nflverse-data, CC BY 4.0 with attribution | `season`, `week`, `team`, `gsis_id`, `position`, `status` | Daily in season; week-level history from 2002 | Effective-dated prior roster plus Sleeper enrichment; tier B |
| Weekly stats | nflverse-data, CC BY 4.0 with attribution | identity, season/week/type/team/position, attempts, carries, targets, receptions, and pass/rush/receive TDs | Snapshot after games and corrections; history from 1999 | Eligible play-by-play aggregation at tier B; prior and league rates at tier C |
| Play-by-play | nflverse-data, CC BY 4.0 with attribution | game/team/play identity, play type, pass/rush/sack/kneel/scramble flags, completion, touchdown, field position, score state | Incremental/nightly; history from 1999 | Weekly facts without red-zone/game-script detail at tier B; league priors at tier C |
| Participation | FTN Data via nflverse for 2023+ and NFL NextGenStats via nflverse for earlier years; CC BY-SA 4.0 attribution rules apply | game/play/team identity and players on play | 2016+; recent seasons can arrive only after the postseason | Snap, depth, and weekly-usage evidence at tier B with wider role uncertainty |
| Schedule | nflverse-data, CC BY 4.0 with attribution | game/season/type/week/date/time, teams, location, stadium | Daily and on league revisions; history from 1999 | Last eligible revision; missing opponent or venue context forces tier C |
| Transactions | nflverse trades/weekly-roster changes under CC BY 4.0; Sleeper enrichment remains subject to Sleeper API terms and is not treated as having a redistribution license | player, effective time, type, prior team, new team | Daily preseason/in-season; observed and effective times remain distinct | Infer only from adjacent eligible weekly rosters; tier B with transaction detail flagged missing |

Primary references:

- [nflverse-data repository and CC BY 4.0 license](https://github.com/nflverse/nflverse-data)
- [nflverse weekly-roster loader and historical coverage](https://github.com/nflverse/nflreadr/blob/main/R/load_rosters_weekly.R)
- [nflverse participation loader and attribution requirements](https://github.com/nflverse/nflreadr/blob/main/R/load_participation.R)
- [Sleeper API documentation](https://docs.sleeper.com/)

## Fallback and confidence rules

- **Tier A — 1.00 maximum confidence:** all required sources are eligible at
  the decision cutoff.
- **Tier B — 0.80 maximum confidence:** contextual evidence such as
  participation or transaction detail is missing, but the core historical
  opportunity inputs remain available.
- **Tier C — 0.60 maximum confidence:** a core roster, weekly-stat,
  play-by-play, or schedule input is missing, requiring prior or league-level
  estimates.

Missing sources are persisted as names/flags. Downstream code must consume the
fallback decision and uncertainty multiplier; it must not manufacture a zero
target, carry, touchdown, route, or snap value.

## Simulated as-of examples

- A response published on 2025-08-20 is eligible for a 2025-08-25 preseason
  cutoff.
- A response published on 2025-09-02 is rejected for that same cutoff.
- If the source supplies no publication time and the response was fetched on
  2026-08-28, it is not eligible for a 2025 backtest merely because its rows
  describe the 2025 season. A verified earlier availability timestamp or an
  older stored snapshot is required.

## Non-goals for this contract

This foundation does not ingest historical team-week facts, fit Team
Opportunity, or activate V2 projections. Those remain V2-003 and later ledger
targets.
