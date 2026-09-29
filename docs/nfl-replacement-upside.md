# Replacement upside: baseline and "if he gets the job", side by side

Version `nfl-replacement-upside-v1`. **Display only.** The optimizer, the
ownership prior and the projection column keep the baseline. Both numbers are
shown until the mixture is tuned and graded on 2026 slates.

Code: `web/src/lib/nfl-dfs/replacement-upside.ts` (math),
`web/src/db/nfl-dfs-usage-window.ts` (usage history),
`attachReplacementUpside` in `web/src/app/dfs/nfl/actions.ts` (wiring),
`web/src/app/dfs/nfl/replacement-upside-display.tsx` (UI).
Fit: `model/nfl_replacement_upside_fit.py` →
`artifacts/nfl_replacement_upside_v1_fit.json`.
Tests: `npm run test:nfl-replacement-upside` (includes a check that the app's
chances equal the fit artifact).

## Why a second range, not a higher average

When a starter sits, the player behind him has two possible games: he takes
the starter's role and plays like the starter, or he doesn't and plays like
himself. Averaging the two (the withheld redistribution, roughly +2 points)
describes a game that rarely happens, and it cannot win a GPP.

First game a starter misses, top remaining player at the position, 2020–25
(seasons earlier studies used, so this describes rather than tests):

| | Got the job (≥75% of the starter's volume) | Got it: median / P90 DK | Didn't: median / P90 DK | Starter's own P90 |
|---|---:|---|---|---:|
| RB | 65% | 15.3 / 29.0 | 5.1 / 13.2 | 21.9 |
| TE | 39% | 8.1 / 18.2 | 2.1 / 9.0 | 16.6 |
| WR | 58% | 16.2 / 29.2 | 5.8 / 14.1 | 27.0 |

His history-based 90th percentile was beaten in 46% (RB) and 42% (TE) of
these games, against 15–18% for backups in full-strength games.

## The model

For each flagged player:

    with chance (1 − π): his own projected range (unchanged baseline)
    with chance π:       the ruled-out starter's projected range

The mixture's mean, P10, median, P90 and boom rate are shown next to the
baseline. The starter's range is the one the pipeline projected before ruling
him out (`pre_availability`, same run; feature v2), else his stored
projection, because the workspace row has already been zeroed.

**Trigger.** A player the slate marks OUT who was a starter over his team's
last 8 games (recency-weighted, half-life 4, ≥ 2 games with a stat row):
RB ≥ 10 carries, TE ≥ 4 targets, WR ≥ 7 targets a game, the same bars as
the blind studies. He must have a stat row in his team's most recent
completed game (first game of the absence only: once he has missed a game the
backup's recent games already carry the new role). Usage comes from the
nflverse weekly feed (`ff_player_week_stats.source_row`); a game with no stat
row counts as not active, the activity proxy validated for the pre-2019
studies. A row for a different team (a traded player's old club) does not
count.

**Recipients.** Active players in the same room (RB/FB, TE, or WR) with
recent volume, ranked by it.

**Chances by role** (fitted on 2020–25 by 90th-percentile pinball loss,
rechecked on 2014–18; a role keeps π > 0 only if it improves both):

| Room | Role | π | Fit pinball (0 → π) | Recheck pinball (0 → π) |
|---|---|---:|---|---|
| RB | lead | 0.5 | 3.98 → 2.21 | 3.87 → 2.36 |
| RB | other | 0.2 | 1.82 → 1.46 | 2.07 → 1.48 |
| TE | lead | 0.4 | 2.42 → 1.44 | 2.09 → 1.41 |
| TE | other | 0.2 | 1.16 → 0.79 | 1.40 → 0.92 |
| WR | lead | **0** | 2.63 → 2.21 | 2.18 → **2.27** |
| WR | other | 0.2 | 2.04 → 1.56 | 2.34 → 1.55 |
| WR | TE | **0** | 1.11 → 1.11 | 1.54 → **1.58** |

The top remaining receiver gets no adjustment: his own recent games already
show that ceiling, and the extra upside did not hold up on 2014–18 (the same
pattern as the WR blind test, where the extra big games landed on the other
receivers). He is marked "no change" with that reason rather than left
unexplained. A TE gets nothing when a receiver sits.

Example (fixture): a backup back at 6.2 projected / 12.4 P90 behind a
starter at 16.5 / 27.0 shows **if job 11.3 / 24.4**, boom 1% → 8%. The
ceiling moves far more than the average, which is the point.

## Limits, stated

- **Not validated.** π was chosen on seasons already examined. The 2014–18
  recheck is a replication, not a blind test. The only unseen data left is
  2026 forward.
- The chances are marginal per player and are not forced to sum to one
  within a room.
- "Got the job" is known only after the game; π is a probability, not a
  prediction of who.
- History-based ceilings run low for everyone (full-strength backups beat
  theirs 15–18% of the time), so part of the gain is correcting that.
- The stat-row activity proxy treats a player who appeared without a stat
  as inactive, which can raise his per-game baseline slightly.
- A starter the pipeline ruled out is read from its pre-availability range
  (v2, 2026-09-29). On runs built before that, whose zeroed range was not
  kept, he is still skipped with a stated reason, never mixed in.

## How to promote it

The gate is pre-registered in
[nfl-replacement-upside-grading.md](nfl-replacement-upside-grading.md)
(`nfl-replacement-upside-grade-v1`, registered 2026-09-28, before week 4). It
grades the frozen pregame pool captures on DraftKings results, 2026 week 4
onward. The "if job" P90 must beat the baseline and a generic ceiling
widening, and its boom rate must not be worse. The grade is blinded until 60
absence events, then there is one look. It runs itself every Tuesday and
Wednesday after DraftKings results are ingested, and its progress shows on
`/dfs/nfl/results`. Until it passes, the optimizer reads
only the baseline. Any change to π, the thresholds or the trigger is a new
version.
