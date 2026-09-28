# Opponent processing repairs — September 28, 2026

## Normal behavior

Pick'em displays Baseline and Baseline + Defense Opponent Adjusted. Normal
selection uses the adjusted probability when verified opponent inputs exist.
Fantasy eligibility does not determine which players are retained as evidence.
Model qualification remains a separate prospective accuracy decision.

## Repairs

- Append complete weekly participant evidence to
  `nfl_weekly_participant_evidence`, independently of the fantasy player universe.
  Punter passing and fullback rushing rows survive ingestion. Anonymous team
  records without offensive exposure are excluded; unresolved offensive records
  still fail validation. Identical evidence refreshes preserve first observation.
- Use passing attempts, sacks, or retained passing plays to define pressure
  participants. Quarterback carries alone do not imply passing exposure. Verified
  sack-only and scramble-only appearances remain included.
- Normalize FB to RB for rushing-contact evidence while retaining the original
  source position. Missing participant rows and unequal carry counts are distinct
  failures. Actual unequal counts include both values and the player ID.
- Aggregate only prior games that pass each family's checks. A bad or missing
  game does not invalidate other verified games. Minimum evidence remains two
  pressure games or twenty RB/FB carries per relevant side. Coverage reports retain
  exclusions and missing game IDs.
- Prefer the combined Pick'em model. If only one complete family is available,
  use its independently fitted pressure/contact model. Both use the fixed ridge
  20, no-intercept development procedure and 2023–2025 outcomes only; no improved
  accuracy is claimed. Every registered arm, including exact fallbacks, is frozen
  separately for evaluation. The preferred usable arm is marked explicitly.
- Recalculate the opponent residual against the latest fresh pregame moneyline
  baseline using the retained opponent inputs. A changed quote no longer causes
  an automatic exact-timestamp rejection. The saved card stores the actual
  recalculated input and both numbers for replay. The baseline reader consistently
  supplies the baseline, including after future model qualification.
- Refresh stored forecasts after existing odds-capture workflows when the quote
  changes. This adds no paid odds requests. Existing DFS results ingestion also
  retains the full participant evidence.
- Display saved pregame comparisons after kickoff. Historical display does not
  authorize a new recommendation or revise a previously frozen card. Display
  specific fallback reasons in current comparisons and newly saved evidence.

## Verified real examples

All pressure and contact checks pass for both teams in these seven audited games:

- `2026_01_CLE_JAX`: Nick Mullens had three kneels and no passing exposure.
- `2026_02_CAR_ATL`: Kenny Pickett had one kneel and no passing exposure.
- `2026_02_MIA_SF`: Mac Jones had two kneels and no passing exposure.
- `2026_01_ATL_PIT`: Cameron Johnston's punter passing row is now retained.
- `2026_02_CIN_HOU`: Kai Kroeger's punter passing row is now retained.
- `2026_01_MIA_LV` and `2026_02_LV_LAC`: Connor Heyward is FB in the weekly
  source and RB in the retained PFR identity. Both sources report one carry.

Receipt: `artifacts/nfl-matchup-implementation/2026-09-28/participant-repair-verification.json`.

The current Week 3 saved DFS salary uploads contain no remaining unstarted-game
players; this repair cannot create a Monday salary slate. New eligible uploads
use the repaired ingestion and feature calculations. Existing optimizer tests
verify adjusted distributions affect selection and export.

## Remaining conditions

Missing source statistics are never filled with invented zeros. Invalid identities,
unreconciled exposure totals, insufficient verified history, missing/future/stale
moneylines, invalid probabilities and kickoff locks remain explicit conditions.
The repair removes unnecessary suppression caused by inconsistent processing;
it does not manufacture unavailable evidence.

## Checks

Python regressions cover real participant cases, RB/FB consistency, true versus
missing carry mismatches, valid-history aggregation, independently fitted family
selection, preservation of all evaluation arms, and changed-quote refreshes.
TypeScript checks cover current-quote recalculation, stale-input recovery,
saved-input replay, historical display, optimizer selection and export, and real
Pick'em card freezing/reopening. Earlier forecasts and registrations remain
immutable; new implementation pins apply forward only.
