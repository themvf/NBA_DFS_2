# NBA Top Scorers evidence and forward comparison

## Acceptance criteria

- The season reference is the arithmetic mean of all stored regular-season appearances for that season, grouped by player across trades. Exclude playoffs, missing points and non-appearances (minutes <= 0); show missing coverage rather than falling back to the rolling EWMA table.
- Treat the last historical team as a prior-season association. It is not current roster or game-day availability evidence. A line–season difference is descriptive, not an edge.
- The verified Herb Jones / Herbert Jones alias can resolve a unique player inside the matched game; collisions remain ambiguous.
- Grade version 2 uses the last entire event/market capture before the earlier known ESPN or market scheduled start. It must reselect an earlier snapshot, not retain a removed player's old quote. Scheduled time does not certify actual tip time.
- Preserve version 1 grades. The page shows a single version, uses it for both pooled and nightly statistics, and explains the legacy boundary while version 2 is absent.
- Research selections freeze candidate coverage, raw per-book odds, source times, lock/as-of times, reader version and deterministic tie rules before scoring. Results are written separately and never replace selections.
- A reconstruction created after lock must say `retrospective_reconstruction`; it cannot become forward evidence. Missing players, DNP rules, contest legality, availability, duplication and payout remain unresolved unless separately established.

## Historical PPG

`getNbaSeasonStats` reads `nba_player_game_logs`, not `nba_player_stats`. The latter holds a ten-game EWMA (alpha 0.25), and can include playoff games. That source caused false season comparisons in the original page. Season PPG is aggregated across teams; the last regular-season log supplies only a historical team association. Position is omitted because the game logs do not provide it.

`web/scripts/verify-nba-top-scorers.ts` reads all regular-season records and independently checks every returned reference's count and arithmetic mean. It also checks that pooled and nightly grades use the same version. Run against a configured read-only-capable warehouse:

```powershell
cd web
node --env-file=.env.local -r ./scripts/server-only-stub.cjs --import tsx ./scripts/verify-nba-top-scorers.ts
```

## Versioned grading and rollout

`nba-points-grade-v1` is retained in the database. Version 2 changes identity matching and the capture boundary, not the ladder mathematics. The page prefers a populated v2 cohort and otherwise shows v1 with its limitation. Versions are never pooled. A populated v2 cohort can initially cover fewer dates; backfill the whole intended evaluation period before comparing pooled totals.

No schema change is needed. Both dry-run and apply modes now avoid schema initialization; missing required tables produce an explicit error. For the four-night review period:

```powershell
# Read-only calculation; does not write grades.
python -m model.nba_points_calibration --date 2026-10-23 --days-back 4

# Release/backfill step once the revised code is integrated.
# Writes a separate v2 cohort; leaves all v1 rows intact.
python -m model.nba_points_calibration --date 2026-10-23 --days-back 4 --apply
```

The October 24 verification ran the calculation without `--apply`. Production grades were not changed. Expected dry-run statuses: October 20: 42 graded; October 21: 152 graded and one unresolved player; October 22: 29 graded; October 23: 168 graded. Herb/Herbert accounts for the additional resolved row. The earlier Chicago–Milwaukee capture retains that game's coverage while avoiding the disputed 4:04 PM snapshot. The 25+ shortfall remains.

## Freeze and grade research baselines

`research.nba_points_forward` makes no database writes and requests no new sportsbook data. It freezes the observed all-day market pool, not a verified contest pool. It explicitly marks missing eligibility, availability and completeness. No automated schedule or new paid capture is introduced.

Choose an explicit timezone-aware lock no later than the pool's earliest market start. A file created before lock uses only captures strictly before creation; `as_of` records that earlier time. A file created after lock uses captures strictly before lock and is labeled retrospective. An earlier known ESPN scheduled start further restricts selection. Main and alternate capture times are recorded separately.

```powershell
# Example historical reconstruction, deliberately not forward evidence.
python -m research.nba_points_forward freeze --date 2026-10-23 --lock-at 2026-10-23T20:00:00Z --output artifacts/nba-points/2026-10-23-snapshot.json
python -m research.nba_points_forward grade --snapshot artifacts/nba-points/2026-10-23-snapshot.json --output artifacts/nba-points/2026-10-23-results.json
```

The baseline rules are highest line, highest ceiling and highest probability of 30+. A rule requires three candidates with its metric. Order by the chosen metric, then line, over probability, canonical name and event id. Record the number tied at the third-player metric. The latter two rules are unvalidated benchmarks, not claims to optimize combined score or prize share.

Snapshots include a content digest and are opened in exclusive-create mode; rerunning cannot overwrite one. Grading checks the digest and writes a separate exclusive-create result. DNP, ambiguous, unmatched, missing box scores or disallowed capture timing leave the trio total unresolved. No contest DNP scoring rule is assumed. Finish position and payout remain null.

Before calling any future run a contest evaluation, establish the actual eligible games/players, roster constraints, entry lock and late-swap rules, DNP scoring, and tie/payout rules. The research CLI deliberately does not accept a contest identifier or claim those facts were verified. A specified game pool and rules need their own validation before legal trios or field performance can be assessed.

## Verification performed October 24

- 32 targeted Python tests passed: capture ingestion, ladder calculations, box scores, revised grading and frozen-baseline lifecycle.
- Both existing TypeScript NBA suites passed, including the verified alias and ambiguity cases.
- Full TypeScript check and the production webpack build passed.
- Live warehouse verification checked all 582 season references against independently accumulated raw regular-season records.
- Four-date v2 calculation completed without writing. Original v1 records remain the live page's cohort.
- The built local page rendered the corrected references: Edwards 28.8/+0.7 and Brunson 26.0/−0.5. The October 23 headline changed from Schröder's misleading +9.6 to Rui Hachimura's descriptive +7.0 gap.
- A historical October 23 freeze/grade exercise completed through the CLI and correctly marked itself retrospective. It is an execution check, not a new forward trial.

Current roster/availability resolution, a contest-specific legal pool, calibrated upper-tail probabilities and an optimized trio model remain open. No probability haircut or strategy was fitted to the first four nights.
