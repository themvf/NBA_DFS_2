# NBA role reliability: what is built (2026-10-31)

Implements Phase 2 of [`nba-points-opportunity-reliability-spec.md`](nba-points-opportunity-reliability-spec.md).
None of it changes a forecast, a probability, an eligibility field or a registered test.

## A. Workload conflicts (live, `nba-workload-conflict-v1`)

`research/nba_points_workload_conflicts.py` reviews every candidate in the day's
frozen ghost v2 snapshot. It runs in `nba_points_ghost.yml` right after the
freeze and appends one record per snapshot to `nba_role_reliability_records`
(append-only, trigger-enforced; on `/health`, 72 h).

- Window: latest 3-5 current-season appearances in the frozen history, zero-shot
  and low-minute games kept; a bad FGA or minutes value makes the window
  `incomplete_window`, never back-filled with an older game.
- `review_required` when recent FGA R minus forecast F is at least 3.0, R > 0,
  and F/R is at most 0.85. `recent_workload_rising` (R at least 2.0 above the
  previous 10-20 appearances) is an explanatory tag only.
- Every candidate gets a status: review_required, no_conflict,
  insufficient_history, missing_forecast, incomplete_window, ambiguous_identity.
- Team continuity comes from the window's own team fields; the decision-time
  team is always `unverified` (no roster source).
- Replays (retrospective, not counted): Oct 28 flagged 9 of 132 candidates,
  Oct 30 flagged 4 of 132. Barnes on Oct 30: model 13.3 shots, recent 18.5
  across 4 games, rising.
- `grade()` gives the descriptive outcome (actual FGA above the forecast, ties
  separate, DNP unresolved). It is not scheduled yet.

## B. Availability-source recovery (built, local collector)

`research/nba_points_injury_evidence.py` now discovers the official PDF with at
most 12 requests: the season index, four recent quarter-hour URLs, then up to
six earlier hourly URLs on the same Eastern day, newest first, never a future
time. Timeouts and connection errors are recorded by category. A run that
finds nothing writes a failure receipt under `<archive>/failures/` (every
attempt, observation time, next action) and exits 2 with no availability file,
which the v3 orchestrator already checks before passing `--availability`.

`research/nba_points_injury_news.py` records typed claims from nba.com articles
by hand (raw article kept, passage must appear in it): `reported_status` for one
date, `reported_unavailable_interval` with a start and a MINIMUM duration (never
an end), `reported_expected_return` (not confirmed availability). `review`
lists the claims that apply on a date; disagreements show as
`conflicting_claims`, and a claim observed after the date is excluded.

## C. Role challenger (formula only, NOT activated)

`research/nba_points_role_challenger.py` implements the 50/50 blend and every
skip (base arithmetic mismatch, invalid inputs, zero-minute rows with attempts,
team identity). It produces no probabilities. It cannot run prospectively:

1. its registered base is the original v3 forecast, produced only by the local
   v3 job (the model file is not in the repository);
2. eligibility needs decision-time team identity from a timestamped official
   roster observation, and none is collected, so every player skips
   `team_identity_unverified`;
3. the protocol at `artifacts/nba-role-reliability/protocol-draft.json` is a
   draft, not a registration.
